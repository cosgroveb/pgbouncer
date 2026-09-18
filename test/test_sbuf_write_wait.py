"""Regression test for preserving destination write waits at the sbuf loop limit."""

import select
import shutil
import socket
import time

import pytest
from psycopg import pq

from .sbuf_fake_backend import (
    FakePostgres,
    bouncer_config,
    query_end,
    query_result,
    response_segments,
)
from .utils import LINUX, Bouncer

pytestmark = pytest.mark.skipif(
    not LINUX or shutil.which("strace") is None,
    reason="requires Linux epoll and strace",
)


@pytest.fixture
async def traced_bouncer(pg, tmp_path):
    trace_path = tmp_path / "pgbouncer.epoll.trace"
    bouncer = Bouncer(pg, tmp_path / "bouncer")
    pgbouncer_command = bouncer.base_command()
    bouncer.base_command = lambda: [
        "strace",
        "-D",
        "-yy",
        "-e",
        "trace=epoll_ctl",
        "-o",
        str(trace_path),
        *pgbouncer_command,
    ]

    await bouncer.start()
    yield bouncer, trace_path
    await bouncer.cleanup()


def _client_epoll_trace(bouncer, trace_path, client_port):
    endpoint = f"127.0.0.1:{bouncer.port}->127.0.0.1:{client_port}"
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        if trace_path.exists():
            lines = [
                line for line in trace_path.read_text().splitlines() if endpoint in line
            ]
            if any("EPOLLIN|EPOLLOUT" in line for line in lines):
                return lines
        time.sleep(0.001)
    raise AssertionError(f"no client EPOLLOUT registration for {endpoint}")


def _get_result(conn):
    deadline = time.monotonic() + 10
    while conn.pgconn.is_busy():
        assert time.monotonic() < deadline
        conn.pgconn.consume_input()
        select.select([conn.pgconn.socket], [], [], 0.01)
    return conn.pgconn.get_result()


def test_loop_limit_preserves_blocked_destination_watcher(traced_bouncer):
    segments, expected = response_segments()
    bouncer, trace_path = traced_bouncer

    with (
        FakePostgres() as backend,
        bouncer.run_with_config(
            bouncer_config(
                bouncer,
                backend.port,
                socket_buffer=65536,
                pool_mode="transaction",
            )
        ),
    ):
        # Create the server socket with a large buffer and leave it in the pool.
        warm = bouncer.conn(dbname="fake", user="postgres", sslmode="disable")
        try:
            warm.pgconn.send_query(b"SELECT warm")
            warm.pgconn.flush()
            assert backend.query().startswith(b"SELECT warm")
            backend.send(query_result([b"warm"]))
            result = _get_result(warm)
            assert result.status == pq.ExecStatus.TUPLES_OK
            assert result.get_value(0, 0) == b"warm"
            assert warm.pgconn.get_result() is None
        finally:
            warm.close()

        # Sockets accepted after the reload use the smaller buffer.
        bouncer.write_ini("tcp_socket_buffer = 4096")
        bouncer.admin("RELOAD")

        conn = bouncer.conn(dbname="fake", user="postgres", sslmode="disable")
        try:
            with socket.fromfd(
                conn.pgconn.socket, socket.AF_INET, socket.SOCK_STREAM
            ) as client_socket:
                client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8192)
                client_port = client_socket.getsockname()[1]

            conn.pgconn.send_query(b"SELECT controlled")
            conn.pgconn.flush()
            assert backend.query().startswith(b"SELECT controlled")

            # Do not read from the client until the destination write blocks.
            backend.send(b"".join(segments))

            target_lines = _client_epoll_trace(bouncer, trace_path, client_port)
            first_write_wait = next(
                index
                for index, line in enumerate(target_lines)
                if "EPOLLIN|EPOLLOUT" in line
            )
            watcher_trace = target_lines[first_write_wait:]
            write_wait_removed = any(
                "EPOLL_CTL_MOD" in line and "{events=EPOLLIN," in line
                for line in watcher_trace[1:]
            )

            backend.send(query_end(len(expected)))

            result = _get_result(conn)
            assert result.status == pq.ExecStatus.TUPLES_OK
            assert [
                result.get_value(row, 0) for row in range(result.ntuples)
            ] == expected
            assert conn.pgconn.get_result() is None
            assert not write_wait_removed, "\n".join(watcher_trace)
        finally:
            with socket.fromfd(
                conn.pgconn.socket, socket.AF_INET, socket.SOCK_STREAM
            ) as client_socket:
                client_socket.shutdown(socket.SHUT_RDWR)
            conn.close()
