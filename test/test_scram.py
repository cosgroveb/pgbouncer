import base64
import hashlib
import shutil
import ssl
import struct
import subprocess
from dataclasses import dataclass
from typing import Optional

import pytest

from .scram_client import ScramClient, ScramFailure
from .utils import (
    DIRECT_TLS_SUPPORT,
    PG_SUPPORTS_SCRAM,
    TLS_SUPPORT,
    USE_UNIX_SOCKETS,
)

pytestmark = pytest.mark.skipif("not PG_SUPPORTS_SCRAM")

ORDINARY = b"SCRAM-SHA-256"
PLUS = b"SCRAM-SHA-256-PLUS"
GS2_N = b"n,,"
GS2_Y = b"y,,"
GS2_PLUS = b"p=tls-server-end-point,,"


@dataclass(frozen=True)
class ExchangeOutcome:
    status: str
    stage: str
    category: Optional[str]


class ReadSocket:
    def __init__(self, data):
        self.data = data

    def recv(self, length):
        result = self.data[:length]
        self.data = self.data[length:]
        return result


def configure_scram(bouncer):
    bouncer.admin("set auth_type = 'scram-sha-256'")


def test_invalid_server_message_length_is_protocol_failure():
    client = ScramClient("localhost", 0)
    client.sock = ReadSocket(b"R" + struct.pack("!I", 3))

    with pytest.raises(ScramFailure, match="invalid message length") as exc_info:
        client._read_message()

    assert exc_info.value.category == "protocol"


def test_truncated_authentication_payload_is_protocol_failure():
    payload = b"\0\0\0"
    client = ScramClient("localhost", 0)
    client.sock = ReadSocket(b"R" + struct.pack("!I", len(payload) + 4) + payload)

    outcome = client._read_authentication_outcome()

    assert outcome.status == "rejected"
    assert outcome.category == "protocol"


def configure_tls_scram(bouncer, cert_dir):
    if not TLS_SUPPORT:
        pytest.skip("TLS support is unavailable")
    root = cert_dir / "TestCA1" / "ca.crt"
    key = cert_dir / "TestCA1" / "sites" / "01-localhost.key"
    cert = cert_dir / "TestCA1" / "sites" / "01-localhost.crt"
    bouncer.write_ini(f"client_tls_key_file = {key}")
    bouncer.write_ini(f"client_tls_cert_file = {cert}")
    bouncer.write_ini(f"client_tls_ca_file = {root}")
    bouncer.write_ini("client_tls_sslmode = require")
    bouncer.write_ini("auth_type = scram-sha-256")
    bouncer.admin("reload")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


def test_non_tls_advertises_only_ordinary(bouncer):
    configure_scram(bouncer)
    with ScramClient("127.0.0.1", bouncer.port) as client:
        assert client.mechanisms == [ORDINARY]


def test_non_scram_auth_does_not_advertise_sasl(bouncer):
    bouncer.admin("set auth_type = 'plain'")
    client = ScramClient("127.0.0.1", bouncer.port)
    with pytest.raises(ScramFailure):
        client.connect()
    assert client.sock is None


@pytest.mark.skipif("not USE_UNIX_SOCKETS", reason="Unix sockets are unavailable")
def test_unix_socket_advertises_only_ordinary(bouncer):
    configure_scram(bouncer)
    with ScramClient(
        "localhost",
        bouncer.port,
        unix_socket_dir=bouncer.admin_host,
    ) as client:
        assert client.mechanisms == [ORDINARY]


def test_tls_advertises_plus_then_ordinary(bouncer, cert_dir):
    context = configure_tls_scram(bouncer, cert_dir)
    with ScramClient(
        "localhost", bouncer.port, use_tls=True, ssl_context=context
    ) as client:
        assert client.mechanisms == [PLUS, ORDINARY]


@pytest.mark.parametrize(
    "tls_version", [ssl.TLSVersion.TLSv1_2, ssl.TLSVersion.TLSv1_3]
)
def test_plus_with_tls_protocol_versions(bouncer, cert_dir, tls_version):
    context = configure_tls_scram(bouncer, cert_dir)
    context.minimum_version = tls_version
    context.maximum_version = tls_version
    with ScramClient(
        "localhost", bouncer.port, use_tls=True, ssl_context=context
    ) as client:
        assert client.authenticate(PLUS, GS2_PLUS) == "accepted"


@pytest.mark.skipif(
    "not DIRECT_TLS_SUPPORT", reason="Direct TLS is introduced in PG 17"
)
def test_direct_tls_advertises_plus_then_ordinary(bouncer, cert_dir):
    context = configure_tls_scram(bouncer, cert_dir)
    with ScramClient(
        "localhost",
        bouncer.port,
        direct_tls=True,
        ssl_context=context,
    ) as client:
        assert client.mechanisms == [PLUS, ORDINARY]
        assert client.authenticate(PLUS, GS2_PLUS) == "accepted"


@pytest.mark.parametrize(
    ("user", "password", "database"),
    [
        ("bouncer", "zzzz", "p0"),
        ("scramuser1", "foo", "p62"),
        ("scramuser3", "baz", "p61"),
    ],
)
def test_plus_credential_sources(bouncer, cert_dir, user, password, database):
    context = configure_tls_scram(bouncer, cert_dir)
    with ScramClient(
        "localhost",
        bouncer.port,
        user=user,
        password=password,
        database=database,
        use_tls=True,
        ssl_context=context,
    ) as client:
        assert client.authenticate(PLUS, GS2_PLUS) == "accepted"


def test_plus_auth_query_credentials(bouncer, cert_dir):
    context = configure_tls_scram(bouncer, cert_dir)
    bouncer.write_ini("auth_user = pswcheck")
    bouncer.write_ini(
        "auth_query = SELECT usename, passwd FROM pg_shadow where usename = $1"
    )
    bouncer.admin("reload")

    with ScramClient(
        "localhost",
        bouncer.port,
        user="someuser",
        password="anypasswd",
        database="authdb",
        use_tls=True,
        ssl_context=context,
    ) as client:
        assert client.authenticate(PLUS, GS2_PLUS) == "accepted"


def test_libpq_requires_plus_with_stored_verifier_passthrough(bouncer, cert_dir):
    configure_tls_scram(bouncer, cert_dir)
    bouncer.psql_test(
        host="localhost",
        user="scramuser1",
        password="foo",
        dbname="p62",
        sslmode="verify-full",
        sslrootcert=cert_dir / "TestCA1" / "ca.crt",
        channel_binding="require",
    )


def test_libpq_requires_plus_with_auth_query(bouncer, cert_dir):
    configure_tls_scram(bouncer, cert_dir)
    bouncer.write_ini("auth_user = pswcheck")
    bouncer.write_ini(
        "auth_query = SELECT usename, passwd FROM pg_shadow where usename = $1"
    )
    bouncer.admin("reload")

    bouncer.psql_test(
        host="localhost",
        user="someuser",
        password="anypasswd",
        dbname="authdb",
        sslmode="verify-full",
        sslrootcert=cert_dir / "TestCA1" / "ca.crt",
        channel_binding="require",
    )


def test_libpq_requires_plus_rejects_nonexistent_user(bouncer, cert_dir):
    configure_tls_scram(bouncer, cert_dir)
    with pytest.raises(subprocess.CalledProcessError):
        bouncer.psql_test(
            host="localhost",
            user="nosuchuser",
            password="whatever",
            dbname="p0",
            sslmode="verify-full",
            sslrootcert=cert_dir / "TestCA1" / "ca.crt",
            channel_binding="require",
        )


def test_plus_mock_user_rejects_authentication(bouncer, cert_dir):
    context = configure_tls_scram(bouncer, cert_dir)
    with ScramClient(
        "localhost",
        bouncer.port,
        user="nosuchuser",
        password="whatever",
        use_tls=True,
        ssl_context=context,
    ) as client:
        assert client.authenticate(PLUS, GS2_PLUS) == "rejected"


@pytest.mark.parametrize(
    ("mechanism", "gs2_header", "expected"),
    [
        (ORDINARY, GS2_N, "accepted"),
        (ORDINARY, GS2_Y, "rejected"),
        (ORDINARY, GS2_PLUS, "rejected"),
        (PLUS, GS2_PLUS, "accepted"),
        (PLUS, GS2_N, "rejected"),
        (PLUS, GS2_Y, "rejected"),
        (PLUS, b"p=tls-unique,,", "rejected"),
    ],
)
def test_tls_mechanism_and_gs2_matrix(
    bouncer, cert_dir, mechanism, gs2_header, expected
):
    context = configure_tls_scram(bouncer, cert_dir)
    with ScramClient(
        "localhost", bouncer.port, use_tls=True, ssl_context=context
    ) as client:
        try:
            outcome = client.authenticate(mechanism, gs2_header)
        except ScramFailure:
            outcome = "rejected"
        assert outcome == expected


@pytest.mark.parametrize(
    ("mechanism", "gs2_header", "expected"),
    [
        (ORDINARY, GS2_N, "accepted"),
        (ORDINARY, GS2_Y, "accepted"),
        (PLUS, GS2_PLUS, "rejected"),
    ],
)
def test_non_tls_mechanism_and_gs2_matrix(bouncer, mechanism, gs2_header, expected):
    configure_scram(bouncer)
    with ScramClient("127.0.0.1", bouncer.port) as client:
        try:
            outcome = client.authenticate(mechanism, gs2_header)
        except ScramFailure:
            outcome = "rejected"
        assert outcome == expected


@pytest.mark.parametrize("channel_binding", [b"biws", b"eSws"])
def test_ordinary_rejects_mismatched_channel_binding(bouncer, channel_binding):
    configure_scram(bouncer)
    gs2_header = GS2_Y if channel_binding == b"biws" else GS2_N
    with ScramClient("127.0.0.1", bouncer.port) as client:
        client.begin(ORDINARY, gs2_header)
        assert client.finish(gs2_header, channel_binding=channel_binding) == "rejected"


@pytest.mark.parametrize(
    "channel_binding",
    [
        b"biws",
        b"eSws",
        b"",
        b"not-base64",
    ],
)
def test_plus_rejects_incorrect_channel_binding(bouncer, cert_dir, channel_binding):
    context = configure_tls_scram(bouncer, cert_dir)
    with ScramClient(
        "localhost", bouncer.port, use_tls=True, ssl_context=context
    ) as client:
        client.begin(PLUS, GS2_PLUS)
        assert client.finish(GS2_PLUS, channel_binding=channel_binding) == "rejected"


@pytest.mark.parametrize("change", ["first", "middle", "last", "short", "long"])
def test_plus_rejects_modified_certificate_hash(bouncer, cert_dir, change):
    context = configure_tls_scram(bouncer, cert_dir)
    with ScramClient(
        "localhost", bouncer.port, use_tls=True, ssl_context=context
    ) as client:
        client.begin(PLUS, GS2_PLUS)
        digest = bytearray(client.tls_server_end_point())
        if change == "first":
            digest[0] ^= 1
        elif change == "middle":
            digest[len(digest) // 2] ^= 1
        elif change == "last":
            digest[-1] ^= 1
        elif change == "short":
            digest.pop()
        else:
            digest.append(0)
        channel_binding = base64.b64encode(GS2_PLUS + digest)
        assert client.finish(GS2_PLUS, channel_binding=channel_binding) == "rejected"


def test_plus_rejects_binding_with_wrong_digest_algorithm(bouncer, cert_dir):
    context = configure_tls_scram(bouncer, cert_dir)
    with ScramClient(
        "localhost", bouncer.port, use_tls=True, ssl_context=context
    ) as client:
        client.begin(PLUS, GS2_PLUS)
        digest = hashlib.sha384(client.peer_certificate).digest()
        channel_binding = base64.b64encode(GS2_PLUS + digest)
        assert client.finish(GS2_PLUS, channel_binding=channel_binding) == "rejected"


@pytest.mark.parametrize(
    "change",
    [
        "n-header",
        "y-header",
        "other-type",
        "missing-padding",
        "extra-padding",
        "embedded-nul",
    ],
)
def test_plus_rejects_noncanonical_channel_binding(bouncer, cert_dir, change):
    context = configure_tls_scram(bouncer, cert_dir)
    with ScramClient(
        "localhost", bouncer.port, use_tls=True, ssl_context=context
    ) as client:
        client.begin(PLUS, GS2_PLUS)
        digest = client.tls_server_end_point()
        if change == "n-header":
            channel_binding = base64.b64encode(GS2_N + digest)
        elif change == "y-header":
            channel_binding = base64.b64encode(GS2_Y + digest)
        elif change == "other-type":
            channel_binding = base64.b64encode(b"p=tls-unique,," + digest)
        else:
            channel_binding = base64.b64encode(GS2_PLUS + digest)
            if change == "missing-padding":
                channel_binding = channel_binding.rstrip(b"=")
            elif change == "extra-padding":
                channel_binding += b"="
            else:
                channel_binding += b"\0trailing"
        assert client.finish(GS2_PLUS, channel_binding=channel_binding) == "rejected"


def test_certificate_reload_preserves_connection_identity(bouncer, cert_dir):
    if not TLS_SUPPORT:
        pytest.skip("TLS support is unavailable")
    root = cert_dir / "TestCA1" / "ca.crt"
    active_key = bouncer.config_dir / "client.key"
    active_cert = bouncer.config_dir / "client.crt"
    shutil.copyfile(cert_dir / "TestCA1" / "sites" / "01-localhost.key", active_key)
    shutil.copyfile(cert_dir / "TestCA1" / "sites" / "01-localhost.crt", active_cert)
    bouncer.write_ini(f"client_tls_key_file = {active_key}")
    bouncer.write_ini(f"client_tls_cert_file = {active_cert}")
    bouncer.write_ini(f"client_tls_ca_file = {root}")
    bouncer.write_ini("client_tls_sslmode = require")
    bouncer.write_ini("auth_type = scram-sha-256")
    bouncer.admin("reload")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    with ScramClient(
        "localhost", bouncer.port, use_tls=True, ssl_context=context
    ) as old_client:
        old_client.begin(PLUS, GS2_PLUS)
        old_certificate_hash = old_client.tls_server_end_point()

        shutil.copyfile(cert_dir / "TestCA2" / "sites" / "01-localhost.key", active_key)
        shutil.copyfile(
            cert_dir / "TestCA2" / "sites" / "01-localhost.crt", active_cert
        )
        bouncer.admin("reload")

        assert old_client.finish(GS2_PLUS) == "accepted"

        with ScramClient(
            "localhost", bouncer.port, use_tls=True, ssl_context=context
        ) as new_client:
            assert new_client.tls_server_end_point() != old_certificate_hash
            assert new_client.authenticate(PLUS, GS2_PLUS) == "accepted"

        with ScramClient(
            "localhost", bouncer.port, use_tls=True, ssl_context=context
        ) as mismatched_client:
            mismatched_client.begin(PLUS, GS2_PLUS)
            channel_binding = base64.b64encode(GS2_PLUS + old_certificate_hash)
            assert (
                mismatched_client.finish(GS2_PLUS, channel_binding=channel_binding)
                == "rejected"
            )


@pytest.mark.parametrize(
    "extensions",
    [
        b",x=one",
        b",x=",
        b",x=" + b"x" * 4096,
        b",x=contains-p=marker",
        b",x=one,y=two",
    ],
)
def test_plus_accepts_extensions_in_exact_transcript(bouncer, cert_dir, extensions):
    context = configure_tls_scram(bouncer, cert_dir)
    with ScramClient(
        "localhost", bouncer.port, use_tls=True, ssl_context=context
    ) as client:
        client.begin(PLUS, GS2_PLUS)
        assert client.finish(GS2_PLUS, extensions=extensions) == "accepted"


def test_plus_rejects_data_after_proof(bouncer, cert_dir):
    context = configure_tls_scram(bouncer, cert_dir)
    with ScramClient(
        "localhost", bouncer.port, use_tls=True, ssl_context=context
    ) as client:
        client.begin(PLUS, GS2_PLUS)
        assert client.finish(GS2_PLUS, trailing=b",x=after") == "rejected"


@pytest.mark.parametrize("proof_length", [0, 1, 31, 33])
def test_plus_rejects_incorrect_proof_lengths(bouncer, cert_dir, proof_length):
    context = configure_tls_scram(bouncer, cert_dir)
    with ScramClient(
        "localhost", bouncer.port, use_tls=True, ssl_context=context
    ) as client:
        client.begin(PLUS, GS2_PLUS)
        proof = base64.b64encode(b"x" * proof_length)
        assert client.finish(GS2_PLUS, proof=proof) == "rejected"


@pytest.mark.parametrize("proof", [b"not-base64", b"====", b"AA=A"])
def test_plus_rejects_malformed_proof_base64(bouncer, cert_dir, proof):
    context = configure_tls_scram(bouncer, cert_dir)
    with ScramClient(
        "localhost", bouncer.port, use_tls=True, ssl_context=context
    ) as client:
        client.begin(PLUS, GS2_PLUS)
        assert client.finish(GS2_PLUS, proof=proof) == "rejected"


@pytest.mark.parametrize(
    ("packet_length", "accepted"), [(255, True), (256, True), (257, False)]
)
def test_plus_final_message_at_packet_limit(bouncer, cert_dir, packet_length, accepted):
    context = configure_tls_scram(bouncer, cert_dir)
    bouncer.admin("set max_packet_size = 256")
    try:
        with ScramClient(
            "localhost", bouncer.port, use_tls=True, ssl_context=context
        ) as client:
            client.begin(PLUS, GS2_PLUS)
            binding = base64.b64encode(GS2_PLUS + client.tls_server_end_point())
            packet_overhead = len(
                b"p"
                + b"\0\0\0\0"
                + b"c="
                + binding
                + b",r="
                + client.server_nonce
                + b",x="
                + b",p="
                + base64.b64encode(bytes(32))
            )
            extensions = b",x=" + b"x" * (packet_length - packet_overhead)
            outcome = client.finish(GS2_PLUS, extensions=extensions)
            assert (outcome == "accepted") is accepted
    finally:
        bouncer.admin("set max_packet_size = 2147483647")


@pytest.mark.parametrize(
    "case",
    [
        "missing-c",
        "empty-c",
        "misplaced-c",
        "repeated-c",
        "missing-r",
        "empty-r",
        "short-r",
        "long-r",
        "repeated-r",
        "missing-proof",
        "trailing-comma",
        "embedded-nul",
    ],
)
def test_plus_final_message_boundaries(bouncer, cert_dir, case):
    context = configure_tls_scram(bouncer, cert_dir)
    with ScramClient(
        "localhost", bouncer.port, use_tls=True, ssl_context=context
    ) as client:
        client.begin(PLUS, GS2_PLUS)
        binding = base64.b64encode(GS2_PLUS + client.tls_server_end_point())
        nonce = client.server_nonce
        proof = base64.b64encode(b"x" * 32)
        messages = {
            "missing-c": b"r=" + nonce + b",p=" + proof,
            "empty-c": b"c=,r=" + nonce + b",p=" + proof,
            "misplaced-c": b"r=" + nonce + b",c=" + binding + b",p=" + proof,
            "repeated-c": b"c="
            + binding
            + b",c="
            + binding
            + b",r="
            + nonce
            + b",p="
            + proof,
            "missing-r": b"c=" + binding + b",p=" + proof,
            "empty-r": b"c=" + binding + b",r=,p=" + proof,
            "short-r": b"c=" + binding + b",r=" + nonce[:-1] + b",p=" + proof,
            "long-r": b"c=" + binding + b",r=" + nonce + b"x,p=" + proof,
            "repeated-r": b"c="
            + binding
            + b",r="
            + nonce
            + b",r="
            + nonce
            + b",p="
            + proof,
            "missing-proof": b"c=" + binding + b",r=" + nonce,
            "trailing-comma": b"c=" + binding + b",r=" + nonce + b",p=" + proof + b",",
            "embedded-nul": b"c=" + binding + b",r=" + nonce + b"\0,p=" + proof,
        }
        assert client.send_final(messages[case]) == "rejected"


@pytest.mark.parametrize(
    (
        "mechanism",
        "gs2_header",
        "declared_length",
        "initial_suffix",
        "mechanism_terminator",
    ),
    [
        (b"unknown", GS2_N, None, b"", b"\0"),
        (b"scram-sha-256", GS2_N, None, b"", b"\0"),
        (ORDINARY + b" ", GS2_N, None, b"", b"\0"),
        (ORDINARY + b"\0extra", GS2_N, None, b"", b"\0"),
        (ORDINARY, GS2_N, -1, b"", b"\0"),
        (ORDINARY, GS2_N, 4096, b"", b"\0"),
        (ORDINARY, GS2_N, None, b"trailing", b"\0"),
        (ORDINARY, GS2_N + b"\0trailing", None, b"", b"\0"),
        (ORDINARY, GS2_N, None, b"", b""),
    ],
)
def test_initial_response_boundaries(
    bouncer,
    mechanism,
    gs2_header,
    declared_length,
    initial_suffix,
    mechanism_terminator,
):
    configure_scram(bouncer)
    with ScramClient("127.0.0.1", bouncer.port) as client:
        with pytest.raises(ScramFailure):
            client.begin(
                mechanism,
                gs2_header,
                declared_length=declared_length,
                initial_suffix=initial_suffix,
                mechanism_terminator=mechanism_terminator,
            )


@pytest.mark.parametrize("length_offset", [-1, 1])
def test_initial_response_length_near_exact_size(bouncer, length_offset):
    configure_scram(bouncer)
    initial_length = len(GS2_N + b"n=bouncer,r=clientnonce")
    with ScramClient("127.0.0.1", bouncer.port) as client:
        with pytest.raises(ScramFailure):
            client.begin(
                ORDINARY,
                GS2_N,
                declared_length=initial_length + length_offset,
            )


@pytest.mark.parametrize(
    ("packet_length", "accepted"), [(99, True), (100, True), (101, False)]
)
def test_initial_response_at_packet_limit(bouncer, packet_length, accepted):
    configure_scram(bouncer)
    bouncer.admin("set max_packet_size = 100")
    mechanism_overhead = len(ORDINARY) + 1 + 4
    protocol_overhead = 5
    initial_length = packet_length - mechanism_overhead - protocol_overhead
    bare_prefix = b"n=bouncer,r="
    client_first_bare = bare_prefix + b"x" * (
        initial_length - len(GS2_N) - len(bare_prefix)
    )
    try:
        with ScramClient("127.0.0.1", bouncer.port) as client:
            if accepted:
                client.begin(
                    ORDINARY,
                    GS2_N,
                    client_first_bare=client_first_bare,
                )
            else:
                with pytest.raises(ScramFailure):
                    client.begin(
                        ORDINARY,
                        GS2_N,
                        client_first_bare=client_first_bare,
                    )
    finally:
        bouncer.admin("set max_packet_size = 2147483647")


@pytest.mark.parametrize(
    "gs2_header",
    [
        b"",
        b"p",
        b"p=",
        b"p=tls-server-end-point",
        b"p=tls-server-end-point,",
        b"p=TLS-SERVER-END-POINT,,",
        b"p=tls-server-end-point,authz,",
        b"p=tls-server-end-point,,\0",
    ],
)
def test_plus_gs2_boundaries(bouncer, cert_dir, gs2_header):
    context = configure_tls_scram(bouncer, cert_dir)
    with ScramClient(
        "localhost", bouncer.port, use_tls=True, ssl_context=context
    ) as client:
        with pytest.raises(ScramFailure):
            client.begin(PLUS, gs2_header)


@pytest.mark.parametrize(
    ("gs2_header", "client_first_bare"),
    [
        (b"", b""),
        (b"p", b""),
        (b"p=,,", b"n=bouncer,r=clientnonce"),
        (b"p=x,,", b"n=bouncer,r=clientnonce"),
        (b"p=" + b"x" * 512 + b",,", b"n=bouncer,r=clientnonce"),
        (b"p=\x80,,", b"n=bouncer,r=clientnonce"),
        (GS2_PLUS, b"r=clientnonce"),
        (GS2_PLUS, b"n=bouncer"),
        (GS2_PLUS, b"n=bouncer,r=not\x01printable"),
        (GS2_PLUS, b"m=required,n=bouncer,r=clientnonce"),
    ],
)
def test_plus_client_first_boundaries(bouncer, cert_dir, gs2_header, client_first_bare):
    context = configure_tls_scram(bouncer, cert_dir)
    with ScramClient(
        "localhost", bouncer.port, use_tls=True, ssl_context=context
    ) as client:
        with pytest.raises(ScramFailure):
            client.begin(
                PLUS,
                gs2_header,
                client_first_bare=client_first_bare,
            )


def test_plus_accepts_empty_client_nonce_like_postgres(bouncer, cert_dir):
    context = configure_tls_scram(bouncer, cert_dir)
    with ScramClient(
        "localhost", bouncer.port, use_tls=True, ssl_context=context
    ) as client:
        client.begin(
            PLUS,
            GS2_PLUS,
            client_first_bare=b"n=bouncer,r=",
        )
        assert client.finish(GS2_PLUS) == "accepted"


def test_channel_binding_differential_against_postgres(bouncer, pg, cert_dir):
    context = configure_tls_scram(bouncer, cert_dir)
    pg.sql(
        "set password_encryption = 'scram-sha-256'; alter user bouncer password 'zzzz'"
    )
    pg.ssl_access("p0", "scram-sha-256", user="bouncer")
    pg.configure("ssl=on")
    pg.reload()

    cases = [
        {"name": "ordinary-n", "mechanism": ORDINARY, "gs2_header": GS2_N},
        {"name": "ordinary-y", "mechanism": ORDINARY, "gs2_header": GS2_Y},
        {
            "name": "ordinary-plus-header",
            "mechanism": ORDINARY,
            "gs2_header": GS2_PLUS,
        },
        {"name": "plus-canonical", "mechanism": PLUS, "gs2_header": GS2_PLUS},
        {"name": "plus-n", "mechanism": PLUS, "gs2_header": GS2_N},
        {"name": "plus-y", "mechanism": PLUS, "gs2_header": GS2_Y},
        {
            "name": "plus-other-binding-type",
            "mechanism": PLUS,
            "gs2_header": b"p=tls-unique,,",
        },
        {
            "name": "missing-client-first-username",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "client_first_bare": b"r=clientnonce",
        },
        {
            "name": "empty-client-nonce",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "client_first_bare": b"n=bouncer,r=",
        },
        {
            "name": "unknown-mechanism",
            "mechanism": b"SCRAM-SHA-256-UNKNOWN",
            "gs2_header": GS2_N,
            "begin_stage": "mechanism",
        },
        {
            "name": "missing-mechanism-terminator",
            "mechanism": ORDINARY,
            "gs2_header": GS2_N,
            "begin_kwargs": {"mechanism_terminator": b""},
            "begin_stage": "mechanism",
        },
        {
            "name": "short-initial-response",
            "mechanism": ORDINARY,
            "gs2_header": GS2_N,
            "begin_kwargs": {"declared_length": 1},
        },
        {
            "name": "long-initial-response",
            "mechanism": ORDINARY,
            "gs2_header": GS2_N,
            "begin_kwargs": {"declared_length": 4096},
        },
        {
            "name": "trailing-initial-response-data",
            "mechanism": ORDINARY,
            "gs2_header": GS2_N,
            "begin_kwargs": {"initial_suffix": b"trailing"},
        },
        {
            "name": "client-first-embedded-nul",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "client_first_bare": b"n=bouncer,r=clientnonce\0trailing",
        },
        {
            "name": "channel-binding-missing-padding",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "finish_case": "missing-padding",
        },
        {
            "name": "channel-binding-extra-padding",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "finish_case": "extra-padding",
        },
        {
            "name": "channel-binding-malformed-base64",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "finish_case": "malformed-channel-binding",
        },
        {
            "name": "channel-binding-embedded-nul",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "finish_case": "channel-binding-embedded-nul",
        },
        {
            "name": "wrong-certificate-binding",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "finish_case": "wrong-certificate",
        },
        {
            "name": "missing-final-nonce",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "finish_case": "missing-nonce",
        },
        {
            "name": "empty-final-nonce",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "finish_case": "empty-nonce",
        },
        {
            "name": "short-final-nonce",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "finish_case": "short-nonce",
        },
        {
            "name": "long-final-nonce",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "finish_case": "long-nonce",
        },
        {
            "name": "repeated-final-nonce",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "finish_case": "repeated-nonce",
        },
        {
            "name": "missing-proof",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "finish_case": "missing-proof",
        },
        *[
            {
                "name": f"proof-length-{proof_length}",
                "mechanism": PLUS,
                "gs2_header": GS2_PLUS,
                "finish_case": "proof-length",
                "proof_length": proof_length,
            }
            for proof_length in (0, 1, 31, 33)
        ],
        {
            "name": "repeated-proof",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "finish_case": "repeated-proof",
        },
        {
            "name": "optional-extensions",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "finish_case": "extensions",
        },
        {
            "name": "trailing-final-data",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "finish_case": "trailing",
        },
        {
            "name": "final-embedded-nul",
            "mechanism": PLUS,
            "gs2_header": GS2_PLUS,
            "finish_case": "embedded-nul",
        },
    ]

    def exchange(port, case):
        with ScramClient(
            "localhost", port, use_tls=True, ssl_context=context
        ) as client:
            mechanisms = client.mechanisms
            stage = case.get("begin_stage", "client-first")
            try:
                client.begin(
                    case["mechanism"],
                    case["gs2_header"],
                    client_first_bare=case.get("client_first_bare"),
                    **case.get("begin_kwargs", {}),
                )
                stage = "client-final"
                channel_binding = base64.b64encode(
                    case["gs2_header"] + client.tls_server_end_point()
                )
                finish_case = case.get("finish_case")
                if finish_case == "missing-padding":
                    outcome = client.finish(
                        case["gs2_header"],
                        channel_binding=channel_binding.rstrip(b"="),
                    )
                elif finish_case == "extra-padding":
                    outcome = client.finish(
                        case["gs2_header"], channel_binding=channel_binding + b"="
                    )
                elif finish_case == "malformed-channel-binding":
                    outcome = client.finish(
                        case["gs2_header"], channel_binding=b"not-base64"
                    )
                elif finish_case == "channel-binding-embedded-nul":
                    outcome = client.finish(
                        case["gs2_header"],
                        channel_binding=channel_binding + b"\0trailing",
                    )
                elif finish_case == "wrong-certificate":
                    digest = bytearray(client.tls_server_end_point())
                    digest[0] ^= 1
                    outcome = client.finish(
                        case["gs2_header"],
                        channel_binding=base64.b64encode(case["gs2_header"] + digest),
                    )
                elif finish_case == "missing-nonce":
                    proof = base64.b64encode(b"x" * 32)
                    outcome = client.send_final(
                        b"c=" + channel_binding + b",p=" + proof
                    )
                elif finish_case == "empty-nonce":
                    outcome = client.finish(case["gs2_header"], nonce=b"")
                elif finish_case == "short-nonce":
                    outcome = client.finish(
                        case["gs2_header"], nonce=client.server_nonce[:-1]
                    )
                elif finish_case == "long-nonce":
                    outcome = client.finish(
                        case["gs2_header"], nonce=client.server_nonce + b"x"
                    )
                elif finish_case == "repeated-nonce":
                    proof = base64.b64encode(b"x" * 32)
                    outcome = client.send_final(
                        b"c="
                        + channel_binding
                        + b",r="
                        + client.server_nonce
                        + b",r="
                        + client.server_nonce
                        + b",p="
                        + proof
                    )
                elif finish_case == "missing-proof":
                    outcome = client.send_final(
                        b"c=" + channel_binding + b",r=" + client.server_nonce
                    )
                elif finish_case == "proof-length":
                    proof = base64.b64encode(b"x" * case["proof_length"])
                    outcome = client.finish(case["gs2_header"], proof=proof)
                elif finish_case == "repeated-proof":
                    proof = base64.b64encode(b"x" * 32)
                    outcome = client.finish(
                        case["gs2_header"], proof=proof, trailing=b",p=" + proof
                    )
                elif finish_case == "extensions":
                    outcome = client.finish(
                        case["gs2_header"], extensions=b",x=one,y=two"
                    )
                elif finish_case == "trailing":
                    outcome = client.finish(case["gs2_header"], trailing=b",x=after")
                elif finish_case == "embedded-nul":
                    outcome = client.finish(case["gs2_header"], trailing=b"\0trailing")
                else:
                    outcome = client.finish(case["gs2_header"])
            except ScramFailure as exc:
                result = ExchangeOutcome(
                    "rejected",
                    stage,
                    exc.category,
                )
            else:
                result = ExchangeOutcome(
                    outcome,
                    "client-final",
                    client.last_outcome.category,
                )
            return mechanisms, result

    # PostgreSQL accepts a SASL initial-response length of -1, while PgBouncer
    # intentionally requires an initial response, so that case is excluded.
    expected_category_differences = {
        # PgBouncer's generic disconnect API reports its default 08P01.
        "ordinary-y": ("authentication", "protocol"),
        "channel-binding-missing-padding": ("authentication", "protocol"),
        "channel-binding-extra-padding": ("authentication", "protocol"),
        "channel-binding-malformed-base64": ("authentication", "protocol"),
        "wrong-certificate-binding": ("authentication", "protocol"),
        "repeated-final-nonce": ("authentication", "protocol"),
    }
    for case in cases:
        postgres_mechanisms, postgres_result = exchange(pg.port, case)
        pgbouncer_mechanisms, pgbouncer_result = exchange(bouncer.port, case)
        assert pgbouncer_mechanisms == postgres_mechanisms, case["name"]
        assert pgbouncer_result.status == postgres_result.status, case["name"]
        assert pgbouncer_result.stage == postgres_result.stage, case["name"]
        # PgBouncer closes some malformed exchanges without an ErrorResponse.
        # Compare the server-reported class whenever neither side collapsed the
        # failure to a transport close.
        categories = (postgres_result.category, pgbouncer_result.category)
        if "transport" not in categories:
            expected_difference = expected_category_differences.get(case["name"])
            if expected_difference is not None:
                assert categories == expected_difference, case["name"]
            else:
                assert categories[1] == categories[0], case["name"]


@pytest.mark.parametrize(
    "certificate_name", ["sha256", "sha384", "sha512", "sha1", "md5", "rsa-pss"]
)
def test_certificate_hash_matches_postgres(bouncer, pg, cert_dir, certificate_name):
    if not TLS_SUPPORT:
        pytest.skip("TLS support is unavailable")

    channel_binding_certificate_dir = cert_dir / "channel-binding-oracle"
    certificate = channel_binding_certificate_dir / f"{certificate_name}.crt"
    key = channel_binding_certificate_dir / f"{certificate_name}.key"
    expected_hash_file = channel_binding_certificate_dir / f"{certificate_name}.hex"
    if not certificate.exists() or not key.exists() or not expected_hash_file.exists():
        pytest.skip(f"{certificate_name} certificate generation is unavailable")

    expected_hash = bytes.fromhex(expected_hash_file.read_text())
    channel_binding = base64.b64encode(GS2_PLUS + expected_hash)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    default_certificate = cert_dir / "TestCA1" / "sites" / "01-localhost.crt"
    default_key = cert_dir / "TestCA1" / "sites" / "01-localhost.key"
    try:
        pg.sql(
            "set password_encryption = 'scram-sha-256'; "
            "alter user bouncer password 'zzzz'"
        )
        pg.ssl_access("p0", "scram-sha-256", user="bouncer")
        pg.configure("ssl=on")
        pg.configure(f"ssl_cert_file='{certificate}'")
        pg.configure(f"ssl_key_file='{key}'")
        pg.reload()

        bouncer.write_ini(f"client_tls_key_file = {key}")
        bouncer.write_ini(f"client_tls_cert_file = {certificate}")
        bouncer.write_ini("client_tls_sslmode = require")
        bouncer.write_ini("auth_type = scram-sha-256")
        bouncer.admin("reload")

        for port in (pg.port, bouncer.port):
            with ScramClient(
                "localhost", port, use_tls=True, ssl_context=context
            ) as client:
                client.begin(PLUS, GS2_PLUS)
                assert (
                    client.finish(GS2_PLUS, channel_binding=channel_binding)
                    == "accepted"
                ), certificate_name
    finally:
        pg.configure(f"ssl_cert_file='{default_certificate}'")
        pg.configure(f"ssl_key_file='{default_key}'")
        pg.reload()
