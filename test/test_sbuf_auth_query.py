"""Regression test for repeated auth-query client resumption."""

import asyncio
import socket
import struct
import threading

import pytest


def _message(kind, payload=b""):
    return kind + struct.pack("!I", len(payload) + 4) + payload


def _row_description(*names):
    fields = b"".join(
        name + b"\0" + struct.pack("!IhIhih", 0, 0, 25, -1, -1, 0) for name in names
    )
    return _message(b"T", struct.pack("!h", len(names)) + fields)


def _data_row(*values):
    columns = b"".join(struct.pack("!I", len(value)) + value for value in values)
    return _message(b"D", struct.pack("!h", len(values)) + columns)


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


class _AuthQueryBackend:
    def __init__(self):
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.listener.settimeout(5)
        self.port = self.listener.getsockname()[1]
        self.connection = None
        self.error = None
        self.thread = threading.Thread(target=self._serve, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.listener.close()
        if self.connection is not None:
            self.connection.close()
        self.thread.join(timeout=5)
        assert not self.thread.is_alive()
        if exc_type is None:
            self.check()

    def check(self):
        if self.error:
            raise self.error

    def _serve(self):
        conn = None
        try:
            conn, _ = self.listener.accept()
            conn.settimeout(5)
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.connection = conn

            _recv_startup(conn)
            conn.sendall(
                _message(b"R", struct.pack("!I", 0))
                + _message(b"S", b"client_encoding\0UTF8\0")
                + _message(b"Z", b"I")
            )

            while True:
                kind, _ = _recv_message(conn)
                if kind == b"S":
                    break

            auth_response = (
                _message(b"1")
                + _message(b"2")
                + _row_description(b"usename", b"passwd")
                + _data_row(b"dynamic_user", b"secret")
                + _message(b"C", b"SELECT 1\0")
                # The first ReadyForQuery resumes the paused client and leaves
                # it waiting for a password. The second resumes its live SBuf.
                + _message(b"Z", b"I")
                + _message(b"Z", b"I")
            )
            conn.sendall(auth_response)
        except (AssertionError, EOFError, OSError, struct.error) as error:
            self.error = error


def _login(host, port):
    parameters = b"user\0dynamic_user\0database\0fake\0\0"
    startup = struct.pack("!I", 0x30000) + parameters

    with socket.create_connection((host, port), timeout=5) as client:
        client.settimeout(5)
        client.sendall(struct.pack("!I", len(startup) + 4) + startup)

        authenticated = False
        while True:
            kind, payload = _recv_message(client)
            if kind == b"R":
                auth_type = struct.unpack("!I", payload[:4])[0]
                if auth_type == 3:
                    client.sendall(_message(b"p", b"secret\0"))
                else:
                    assert auth_type == 0
                    authenticated = True
            elif kind == b"E":
                raise AssertionError(f"login failed: {payload!r}")
            elif kind == b"Z":
                assert authenticated
                return


async def _kill_bouncer(bouncer):
    if bouncer.process is not None:
        bouncer.process.kill()
    if bouncer.aprocess is not None:
        bouncer.aprocess.kill()
    await bouncer.wait_for_exit()


async def test_duplicate_auth_query_ready_does_not_livelock(bouncer):
    with _AuthQueryBackend() as backend:
        config = f"""
[databases]
fake = host=127.0.0.1 port={backend.port} dbname=fake user=pswcheck password=x

[pgbouncer]
listen_addr = {bouncer.host}
listen_port = {bouncer.port}
unix_socket_dir = {bouncer.admin_host}
auth_type = plain
auth_file = {bouncer.auth_path}
auth_query = SELECT usename, passwd FROM pg_shadow WHERE usename = $1
auth_user = pswcheck
admin_users = pgbouncer
logfile = {bouncer.log_path}
pidfile =
client_tls_sslmode = disable
server_tls_sslmode = disable
"""
        bouncer.ini_path.write_text(config)
        bouncer.admin("RELOAD")

        try:
            await asyncio.to_thread(_login, bouncer.host, bouncer.port)
        except TimeoutError:
            backend.check()
            await _kill_bouncer(bouncer)
            pytest.fail("PgBouncer stopped processing after duplicate ReadyForQuery")
