"""Controlled PostgreSQL backend for sbuf integration tests."""

import socket
import struct
import threading


def _message(kind, payload=b""):
    return kind + struct.pack("!I", len(payload) + 4) + payload


def _row_description(name=b"value"):
    field = name + b"\0" + struct.pack("!IhIhih", 0, 0, 25, -1, -1, 0)
    return _message(b"T", struct.pack("!h", 1) + field)


def _data_row(value):
    return _message(b"D", struct.pack("!hI", 1, len(value)) + value)


def query_end(rows):
    command_complete = _message(b"C", f"SELECT {rows}".encode() + b"\0")
    return command_complete + _message(b"Z", b"I")


def query_result(values):
    return (
        _row_description() + b"".join(map(_data_row, values)) + query_end(len(values))
    )


_STARTUP_RESPONSE = b"".join(
    [
        _message(b"R", struct.pack("!I", 0)),
        _message(b"S", b"client_encoding\0UTF8\0"),
        _message(b"Z", b"I"),
    ]
)


def _recv_exact(sock, size):
    chunks = []
    while size:
        chunk = sock.recv(size)
        if not chunk:
            raise EOFError("peer closed")
        chunks.append(chunk)
        size -= len(chunk)
    return b"".join(chunks)


def _recv_startup(sock):
    size = struct.unpack("!I", _recv_exact(sock, 4))[0]
    return _recv_exact(sock, size - 4)


def _recv_message(sock):
    kind = _recv_exact(sock, 1)
    size = struct.unpack("!I", _recv_exact(sock, 4))[0]
    return kind, _recv_exact(sock, size - 4)


class FakePostgres:
    def __init__(self):
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.listener.settimeout(5)
        self.port = self.listener.getsockname()[1]
        self.connection = None
        self.error = None
        self.ready = threading.Event()
        self.thread = threading.Thread(target=self._accept, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.listener.close()
        if self.connection is not None:
            self.connection.close()
        self.thread.join(timeout=5)
        if exc_type is None:
            assert not self.thread.is_alive()
            if self.error:
                raise self.error

    def query(self):
        kind, query = _recv_message(self._connection())
        assert kind == b"Q"
        return query

    def send(self, data):
        self._connection().sendall(data)

    def _connection(self):
        assert self.ready.wait(5), "PgBouncer did not connect to fake PostgreSQL"
        if self.error:
            raise self.error
        assert self.connection is not None
        return self.connection

    def _accept(self):
        conn = None
        try:
            conn, _ = self.listener.accept()
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            _recv_startup(conn)
            conn.sendall(_STARTUP_RESPONSE)
            self.connection = conn
        except BaseException as error:
            if conn is not None:
                conn.close()
            self.error = error
        finally:
            self.ready.set()


def bouncer_config(bouncer, backend_port, socket_buffer=4096, pool_mode="session"):
    return f"""
[databases]
fake = host=127.0.0.1 port={backend_port} dbname=fake user=postgres pool_mode={pool_mode}

[pgbouncer]
listen_addr = {bouncer.host}
listen_port = {bouncer.port}
unix_socket_dir = {bouncer.admin_host}
auth_type = trust
auth_file = {bouncer.auth_path}
admin_users = pgbouncer
logfile = {bouncer.log_path}
pidfile =
pool_mode = {pool_mode}
sbuf_loopcnt = 1
tcp_socket_buffer = {socket_buffer}
server_tls_sslmode = disable
"""


def response_segments():
    # Each sbuf-sized read ends with an empty 11-byte DataRow. The normal send
    # consumes the preceding row, leaving this packet for the loop-limit flush.
    description = _row_description(b"a" * 41)
    empty_row = _data_row(b"")
    assert len(description) == 67
    assert len(empty_row) == 11

    segments = []
    expected = []
    for segment_number in range(16):
        prefix = description if segment_number == 0 else b""
        filler_length = 4096 - len(prefix) - 2 * len(empty_row)
        filler_value = bytes([65 + segment_number % 26]) * filler_length
        segment = prefix + _data_row(filler_value) + empty_row
        assert len(segment) == 4096
        segments.append(segment)
        expected.extend([filler_value, b""])
    return segments, expected
