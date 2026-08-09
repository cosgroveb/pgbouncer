import base64
import hashlib
import hmac
import socket
import ssl
import struct
from dataclasses import dataclass
from typing import Optional

AUTH_OK = 0
AUTH_SASL = 10
AUTH_SASL_CONTINUE = 11
AUTH_SASL_FINAL = 12
SSL_REQUEST = 80877103
PROTOCOL_VERSION_3 = 196608


@dataclass(frozen=True)
class AuthenticationOutcome:
    status: str
    category: Optional[str] = None


class ScramFailure(Exception):
    def __init__(self, message, category="transport"):
        super().__init__(message)
        self.category = category


class ScramClient:
    def __init__(
        self,
        host,
        port,
        user="bouncer",
        password="zzzz",
        database="p0",
        use_tls=False,
        direct_tls=False,
        ssl_context=None,
        unix_socket_dir=None,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.use_tls = use_tls or direct_tls
        self.direct_tls = direct_tls
        self.ssl_context = ssl_context
        self.unix_socket_dir = unix_socket_dir
        self.sock = None
        self.peer_certificate = None
        self.mechanisms = None
        self.client_first_bare = None
        self.server_first = None
        self.server_nonce = None
        self.salt = None
        self.iterations = None
        self.last_outcome = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        if self.sock is not None:
            self.sock.close()
            self.sock = None

    def connect(self):
        self.peer_certificate = None
        try:
            if self.unix_socket_dir is None:
                self.sock = socket.create_connection((self.host, self.port), timeout=5)
            else:
                self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self.sock.settimeout(5)
                self.sock.connect(f"{self.unix_socket_dir}/.s.PGSQL.{self.port}")
            self.sock.settimeout(5)
            if self.use_tls:
                context = self.ssl_context or ssl.create_default_context()
                if self.direct_tls:
                    context.set_alpn_protocols(["postgresql"])
                else:
                    self.sock.sendall(struct.pack("!II", 8, SSL_REQUEST))
                    if self._recv_exact(self.sock, 1) != b"S":
                        raise ScramFailure("server rejected SSLRequest")
                self.sock = context.wrap_socket(self.sock, server_hostname=self.host)
                self.peer_certificate = self.sock.getpeercert(binary_form=True)
            self._send_startup()
            auth_data = self._read_until_auth(AUTH_SASL)
            self.mechanisms = self._parse_mechanisms(auth_data)
            return self.mechanisms
        except BaseException:
            self.close()
            raise

    @staticmethod
    def _recv_exact(sock, length):
        result = bytearray()
        while len(result) < length:
            try:
                data = sock.recv(length - len(result))
            except OSError as exc:
                raise ScramFailure("could not read from server") from exc
            if not data:
                raise ScramFailure("server closed the connection")
            result.extend(data)
        return bytes(result)

    def _read_message(self):
        header = self._recv_exact(self.sock, 5)
        message_type = header[:1]
        length = struct.unpack("!I", header[1:])[0]
        if length < 4:
            raise ScramFailure("server sent an invalid message length", "protocol")
        return message_type, self._recv_exact(self.sock, length - 4)

    def _read_until_auth(self, expected_code):
        while True:
            message_type, payload = self._read_message()
            if message_type == b"E":
                category = self._classify_error_response(payload)
                raise ScramFailure("server rejected authentication", category)
            if message_type != b"R":
                continue
            if len(payload) < 4:
                raise ScramFailure(
                    "server sent a truncated authentication request", "protocol"
                )
            code = struct.unpack("!I", payload[:4])[0]
            if code != expected_code:
                raise ScramFailure(
                    f"expected authentication code {expected_code}, received {code}",
                    "protocol",
                )
            return payload[4:]

    @staticmethod
    def _classify_error_response(payload):
        fields = {}
        for field in payload.split(b"\0"):
            if not field:
                continue
            if len(field) < 2:
                return "protocol"
            fields[field[:1]] = field[1:].decode(errors="replace")
        sqlstate = fields.get(b"C")
        if sqlstate in {"28000", "28P01"}:
            return "authentication"
        return "protocol"

    def _send_message(self, message_type, payload):
        self.sock.sendall(message_type + struct.pack("!I", len(payload) + 4) + payload)

    def _send_startup(self):
        params = (
            b"user\0"
            + self.user.encode()
            + b"\0database\0"
            + self.database.encode()
            + b"\0\0"
        )
        payload = struct.pack("!I", PROTOCOL_VERSION_3) + params
        self.sock.sendall(struct.pack("!I", len(payload) + 4) + payload)

    @staticmethod
    def _parse_mechanisms(data):
        if not data.endswith(b"\0\0"):
            raise ScramFailure("invalid SASL mechanism list", "protocol")
        return data[:-2].split(b"\0")

    def begin(
        self,
        mechanism,
        gs2_header,
        client_first_bare=None,
        declared_length=None,
        initial_suffix=b"",
        mechanism_terminator=b"\0",
    ):
        if client_first_bare is None:
            client_first_bare = b"n=" + self.user.encode() + b",r=clientnonce"
        self.client_first_bare = client_first_bare
        initial = gs2_header + client_first_bare
        if declared_length is None:
            declared_length = len(initial)
        payload = (
            mechanism
            + mechanism_terminator
            + struct.pack("!I", declared_length & 0xFFFFFFFF)
            + initial
            + initial_suffix
        )
        self._send_message(b"p", payload)
        self.server_first = self._read_until_auth(AUTH_SASL_CONTINUE)
        attributes = self._parse_attributes(self.server_first)
        try:
            self.server_nonce = attributes[b"r"]
            self.salt = base64.b64decode(attributes[b"s"], validate=True)
            self.iterations = int(attributes[b"i"])
        except (KeyError, ValueError) as exc:
            raise ScramFailure(
                "invalid SCRAM server-first-message", "protocol"
            ) from exc
        return self.server_first

    @staticmethod
    def _parse_attributes(message):
        result = {}
        for attribute in message.split(b","):
            if len(attribute) < 2 or attribute[1:2] != b"=":
                raise ScramFailure("invalid SCRAM attribute", "protocol")
            result[attribute[:1]] = attribute[2:]
        return result

    def tls_server_end_point(self):
        if self.peer_certificate is None:
            raise ScramFailure("TLS server certificate is unavailable")
        # Integration certificates are signed with SHA-256. Algorithm edge
        # cases are covered by tls_server_end_point_test.c.
        return hashlib.sha256(self.peer_certificate).digest()

    def finish(
        self,
        gs2_header,
        channel_binding=None,
        extensions=b"",
        nonce=None,
        proof=None,
        trailing=b"",
    ):
        if channel_binding is None:
            channel_binding = base64.b64encode(
                gs2_header
                + (self.tls_server_end_point() if gs2_header.startswith(b"p=") else b"")
            )
        if nonce is None:
            nonce = self.server_nonce
        client_final_without_proof = (
            b"c=" + channel_binding + b",r=" + nonce + extensions
        )
        auth_message = (
            self.client_first_bare
            + b","
            + self.server_first
            + b","
            + client_final_without_proof
        )
        if proof is None:
            salted_password = hashlib.pbkdf2_hmac(
                "sha256",
                self.password.encode(),
                self.salt,
                self.iterations,
            )
            client_key = hmac.new(
                salted_password, b"Client Key", hashlib.sha256
            ).digest()
            stored_key = hashlib.sha256(client_key).digest()
            client_signature = hmac.new(
                stored_key, auth_message, hashlib.sha256
            ).digest()
            proof = bytes(
                client_byte ^ signature_byte
                for client_byte, signature_byte in zip(client_key, client_signature)
            )
            proof = base64.b64encode(proof)
        return self.send_final(client_final_without_proof + b",p=" + proof + trailing)

    def send_final(self, payload):
        self._send_message(b"p", payload)
        self.last_outcome = self._read_authentication_outcome()
        return self.last_outcome.status

    def _read_authentication_outcome(self):
        saw_final = False
        while True:
            try:
                message_type, payload = self._read_message()
            except OSError:
                return AuthenticationOutcome("rejected", "transport")
            except ScramFailure as exc:
                return AuthenticationOutcome("rejected", exc.category)
            if message_type == b"E":
                category = self._classify_error_response(payload)
                return AuthenticationOutcome("rejected", category)
            if message_type == b"Z":
                if saw_final:
                    return AuthenticationOutcome("accepted")
                return AuthenticationOutcome("rejected", "protocol")
            if message_type != b"R":
                continue
            if len(payload) < 4:
                return AuthenticationOutcome("rejected", "protocol")
            code = struct.unpack("!I", payload[:4])[0]
            if code == AUTH_SASL_FINAL:
                saw_final = True
            elif code != AUTH_OK:
                return AuthenticationOutcome("rejected", "protocol")

    def authenticate(self, mechanism, gs2_header, **finish_kwargs):
        self.begin(mechanism, gs2_header)
        return self.finish(gs2_header, **finish_kwargs)
