import asyncio
import os
import re
import socket
import struct
import subprocess
import time
from pathlib import Path

import psycopg
import pytest
from psycopg.rows import dict_row

from .utils import (
    PG_MAJOR_VERSION,
    TEST_DIR,
    USE_UNIX_SOCKETS,
    WINDOWS,
    Bouncer,
    Postgres,
    run,
)

REPLICA_SOCKET_DIR = Path("/tmp/pgbouncer-test-replica")
requires_replica = pytest.mark.skipif(
    PG_MAJOR_VERSION < 14 or not USE_UNIX_SOCKETS,
    reason="target role tests require PostgreSQL 14+ and Unix sockets",
)


@pytest.fixture(scope="session")
def target_replica(pg, tmp_path_factory):
    pg.reset_hba()
    os.truncate(pg.pgdata / "postgresql.auto.conf", 0)
    if pg.restarted:
        pg.restart()
        pg.restarted = False
    else:
        pg.reload()

    replica = Postgres(tmp_path_factory.getbasetemp() / "pgdata_target_replica")
    replica.port_lock.release()
    replica.port = pg.port
    replica.host = str(REPLICA_SOCKET_DIR)

    REPLICA_SOCKET_DIR.mkdir(exist_ok=True)
    run(
        [
            "pg_basebackup",
            "--pgdata",
            str(replica.pgdata),
            "--write-recovery-conf",
            "--checkpoint=fast",
            "--no-sync",
            "--host",
            pg.host,
            "--port",
            str(pg.port),
            "--username",
            "postgres",
        ],
        env={**os.environ, "PGSSLMODE": "disable"},
        stdout=subprocess.DEVNULL,
    )
    with replica.conf_path.open("a") as conf:
        conf.write(f"port = {pg.port}\n")
        conf.write("listen_addresses = ''\n")
        conf.write(f"unix_socket_directories = '{REPLICA_SOCKET_DIR}'\n")
        conf.write("default_transaction_read_only = on\n")
    replica.start()

    try:
        yield replica
    finally:
        replica.stop()


@pytest.fixture(autouse=True)
def target_retry_settings(bouncer):
    bouncer.admin("SET server_login_retry=1")
    bouncer.admin("SET client_login_timeout=5")


@pytest.fixture
async def untracked_dtr_bouncer(pg, tmp_path):
    base_ini = tmp_path / "untracked-dtr.ini"
    updated, replacements = re.subn(
        r"^track_extra_parameters = search_path, intervalstyle, "
        r"default_transaction_read_only$",
        "track_extra_parameters = search_path, intervalstyle",
        (TEST_DIR / "test.ini").read_text(),
        count=1,
        flags=re.MULTILINE,
    )
    assert replacements == 1
    base_ini.write_text(updated)

    bouncer = Bouncer(
        pg,
        tmp_path / "u",
        base_ini_path=base_ini,
    )
    await bouncer.start()
    try:
        yield bouncer
    finally:
        await bouncer.cleanup()


def selected_role(bouncer, dbname):
    return bouncer.sql_value(
        "SELECT pg_is_in_recovery()", dbname=dbname, connect_timeout=10
    )


def recv_message(sock):
    message_type = recv_exact(sock, 1)
    message_length = struct.unpack("!I", recv_exact(sock, 4))[0]
    return message_type, recv_exact(sock, message_length - 4)


def recv_exact(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            pytest.fail("server closed the connection")
        data += chunk
    return data


def parameter_status_message(message):
    key, value, _ = message.split(b"\0")
    return key.decode(), value.decode()


def protocol_connection(bouncer, dbname, *, user="bouncer", replication=None):
    replication_parameter = (
        f"replication\0{replication}\0".encode() if replication else b""
    )
    startup = (
        struct.pack("!I", 196608)
        + f"user\0{user}\0".encode()
        + f"database\0{dbname}\0".encode()
        + replication_parameter
        + b"\0"
    )
    sock = socket.create_connection((bouncer.host, bouncer.port), timeout=10)
    sock.sendall(struct.pack("!I", len(startup) + 4) + startup)
    parameters = []
    while True:
        message_type, message = recv_message(sock)
        if message_type == b"S":
            parameters.append(parameter_status_message(message))
        elif message_type == b"E":
            sock.close()
            pytest.fail(f"startup failed: {message!r}")
        elif message_type == b"Z":
            return sock, parameters


def protocol_query(sock, query):
    payload = query.encode() + b"\0"
    sock.sendall(b"Q" + struct.pack("!I", len(payload) + 4) + payload)

    parameters = []
    value = None
    while True:
        message_type, message = recv_message(sock)
        if message_type == b"S":
            parameters.append(parameter_status_message(message))
        elif message_type == b"D":
            columns = struct.unpack("!H", message[:2])[0]
            assert columns == 1
            length = struct.unpack("!I", message[2:6])[0]
            value = message[6 : 6 + length].decode()
        elif message_type == b"E":
            pytest.fail(f"query failed: {message!r}")
        elif message_type == b"Z":
            return value, parameters


def startup_parameters(bouncer, dbname):
    sock, parameters = protocol_connection(bouncer, dbname)
    sock.close()
    return parameters


def test_target_session_attrs_admin_output(bouncer):
    databases = {
        row["name"]: row
        for row in bouncer.admin("SHOW DATABASES", row_factory=dict_row)
    }
    assert databases["tsa_version_any"]["target_session_attrs"] == "any"
    assert databases["tsa_version_primary"]["target_session_attrs"] == "primary"
    assert databases["tsa_default_any"]["target_session_attrs"] == "any"

    bouncer.test(dbname="tsa_version_any")
    bouncer.test(dbname="p0")
    if PG_MAJOR_VERSION >= 14:
        bouncer.test(dbname="tsa_version_primary")
    pools = {
        row["database"]: row
        for row in bouncer.admin("SHOW POOLS", row_factory=dict_row)
    }
    assert pools["tsa_version_any"]["target_session_attrs"] == "any"
    assert pools["p0"]["target_session_attrs"] == "any"
    if PG_MAJOR_VERSION >= 14:
        assert pools["tsa_version_primary"]["target_session_attrs"] == "primary"


@requires_replica
@pytest.mark.parametrize(
    "dbname,expected",
    [
        ("tsa_primary", False),
        ("tsa_standby", True),
        ("tsa_read_write", False),
        ("tsa_read_only", True),
        ("tsa_any", True),
    ],
)
def test_target_session_attrs_selects_matching_server(
    bouncer, target_replica, dbname, expected
):
    assert selected_role(bouncer, dbname) is expected


@requires_replica
async def test_target_session_attrs_default_any_uses_both_servers(
    bouncer, target_replica
):
    with bouncer.log_contains(r'parameter "in_hot_standby" cannot be changed', times=0):
        results = await asyncio.gather(
            bouncer.asql(
                "SELECT pg_is_in_recovery(), pg_sleep(0.5)",
                dbname="tsa_default_any",
            ),
            bouncer.asql(
                "SELECT pg_is_in_recovery(), pg_sleep(0.5)",
                dbname="tsa_default_any",
            ),
        )
    assert {rows[0][0] for rows in results} == {False, True}


@pytest.mark.skipif(
    PG_MAJOR_VERSION < 14,
    reason="default_transaction_read_only was not reported before PostgreSQL 14",
)
def test_target_session_attrs_read_only_primary(bouncer, pg):
    pg.sql("ALTER DATABASE p0 SET default_transaction_read_only=on")
    try:
        assert selected_role(bouncer, "tsa_read_only_primary") is False
        assert selected_role(bouncer, "tsa_primary_read_only") is False
        bouncer.admin("SET client_login_timeout=2")
        with pytest.raises(
            psycopg.OperationalError,
            match=r"client_login_timeout \(server down\)",
        ):
            bouncer.test(dbname="tsa_read_write_primary", connect_timeout=10)
    finally:
        pg.sql("ALTER DATABASE p0 RESET default_transaction_read_only")


@pytest.mark.skipif(
    PG_MAJOR_VERSION < 14,
    reason="default_transaction_read_only was not reported before PostgreSQL 14",
)
def test_target_session_attrs_observes_connect_query(bouncer):
    with bouncer.conn(dbname="tsa_connect_query", connect_timeout=10) as conn:
        assert conn.execute("SELECT pg_is_in_recovery()").fetchone() == (False,)
        assert conn.info.parameter_status("in_hot_standby") == "off"
        assert conn.info.parameter_status("default_transaction_read_only") == "on"


@requires_replica
def test_target_session_attrs_reports_accepted_server(bouncer, target_replica):
    with bouncer.conn(dbname="tsa_primary", connect_timeout=10) as conn:
        assert conn.execute("SELECT pg_is_in_recovery()").fetchone() == (False,)
        assert conn.info.parameter_status("in_hot_standby") == "off"
        assert conn.info.parameter_status("default_transaction_read_only") == "off"


@requires_replica
def test_target_session_attrs_replication_retries_matching_server(
    bouncer, target_replica
):
    with (
        bouncer.log_contains("server does not satisfy target_session_attrs"),
        bouncer.conn(
            dbname="tsa_replication_read_write",
            user="postgres",
            replication="database",
            connect_timeout=10,
        ) as conn,
    ):
        assert conn.execute("SELECT pg_is_in_recovery()").fetchone() == (False,)
        assert conn.info.parameter_status("default_transaction_read_only") == "off"
        assert conn.info.parameter_status("in_hot_standby") == "off"


@requires_replica
def test_cold_replication_does_not_repeat_server_parameters(bouncer, target_replica):
    sock, parameters = protocol_connection(
        bouncer,
        "parameter_status_replication",
        user="postgres",
        replication="database",
    )
    try:
        value, changes = protocol_query(sock, "SELECT 1")
        parameters.extend(changes)
        names = [name for name, _ in parameters]

        assert value == "1"
        assert len(names) == len(set(names))
    finally:
        sock.close()


@requires_replica
def test_warm_replication_reports_assigned_server_parameters(bouncer, target_replica):
    with bouncer.conn(
        dbname="parameter_status_replication",
        user="postgres",
        connect_timeout=10,
    ) as conn:
        assert conn.execute("SELECT pg_is_in_recovery()").fetchone() == (False,)
        assert conn.info.parameter_status("in_hot_standby") == "off"

    with bouncer.conn(
        dbname="parameter_status_replication",
        user="postgres",
        replication="database",
        connect_timeout=10,
    ) as conn:
        in_recovery = conn.execute("SELECT pg_is_in_recovery()").fetchone()[0]
        expected = "on" if in_recovery else "off"

        assert in_recovery is True
        assert conn.info.parameter_status("in_hot_standby") == expected


@requires_replica
def test_reconnect_replication_reports_assigned_server_parameters(
    bouncer, target_replica
):
    with bouncer.conn(
        dbname="parameter_status_replication",
        user="postgres",
        connect_timeout=10,
    ) as conn:
        assert conn.execute("SELECT pg_is_in_recovery()").fetchone() == (False,)

    with bouncer.conn(
        dbname="parameter_status_replication",
        user="postgres",
        replication="database",
        connect_timeout=10,
    ) as conn:
        assert conn.info.parameter_status("in_hot_standby") == "off"
        bouncer.admin("RECONNECT parameter_status_replication")

        in_recovery = conn.execute("SELECT pg_is_in_recovery()").fetchone()[0]
        expected = "on" if in_recovery else "off"

        assert in_recovery is True
        assert conn.info.parameter_status("in_hot_standby") == expected


@requires_replica
def test_rejected_server_parameters_are_not_cached(bouncer, target_replica):
    parameters = startup_parameters(bouncer, "tsa_primary")
    names = [name for name, _ in parameters]

    assert len(names) == len(set(names))
    assert dict(parameters)["in_hot_standby"] == "off"
    assert dict(parameters)["default_transaction_read_only"] == "off"


@requires_replica
async def test_parameter_status_follows_assigned_server(bouncer, target_replica):
    bouncer.admin("SET server_round_robin=1")
    results = await asyncio.gather(
        bouncer.asql(
            "SELECT pg_is_in_recovery() FROM pg_sleep(0.5)",
            dbname="parameter_status_hosts",
        ),
        bouncer.asql(
            "SELECT pg_is_in_recovery() FROM pg_sleep(0.5)",
            dbname="parameter_status_hosts",
        ),
    )
    assert {rows[0][0] for rows in results} == {False, True}

    sock, startup = protocol_connection(bouncer, "parameter_status_hosts")
    try:
        current = dict(startup)["in_hot_standby"]
        seen = {current}
        for _ in range(4):
            reported, parameters = protocol_query(sock, "SHOW in_hot_standby")
            changes = dict(parameters)
            if reported == current:
                assert "in_hot_standby" not in changes
            else:
                assert changes["in_hot_standby"] == reported
            current = reported
            seen.add(reported)
        assert seen == {"on", "off"}
    finally:
        sock.close()


def test_parameter_status_does_not_repeat_unchanged_values(bouncer):
    sock, startup = protocol_connection(bouncer, "parameter_status_single")
    try:
        reported, parameters = protocol_query(sock, "SHOW server_version")
        assert reported == dict(startup)["server_version"]
        assert "server_version" not in dict(parameters)
    finally:
        sock.close()


@requires_replica
async def test_untracked_dtr_follows_assigned_server(
    untracked_dtr_bouncer, target_replica
):
    bouncer = untracked_dtr_bouncer
    bouncer.admin("SET server_round_robin=1")
    results = await asyncio.gather(
        bouncer.asql(
            "SELECT pg_is_in_recovery() FROM pg_sleep(0.5)",
            dbname="parameter_status_hosts",
        ),
        bouncer.asql(
            "SELECT pg_is_in_recovery() FROM pg_sleep(0.5)",
            dbname="parameter_status_hosts",
        ),
    )
    assert {rows[0][0] for rows in results} == {False, True}

    sock, startup = protocol_connection(bouncer, "parameter_status_hosts")
    try:
        current = dict(startup)["default_transaction_read_only"]
        seen = {current}
        for _ in range(4):
            reported, parameters = protocol_query(
                sock, "SHOW default_transaction_read_only"
            )
            changes = dict(parameters)
            if reported == current:
                assert "default_transaction_read_only" not in changes
            else:
                assert changes["default_transaction_read_only"] == reported
            current = reported
            seen.add(reported)
        assert seen == {"on", "off"}
    finally:
        sock.close()


@pytest.mark.skipif(
    PG_MAJOR_VERSION < 14,
    reason="default_transaction_read_only was not reported before PostgreSQL 14",
)
def test_untracked_runtime_parameter_status_is_remembered(untracked_dtr_bouncer):
    key = "default_transaction_read_only"
    sock, startup = protocol_connection(
        untracked_dtr_bouncer, "parameter_status_single"
    )
    try:
        assert dict(startup)[key] == "off"

        value, parameters = protocol_query(sock, "SET default_transaction_read_only=on")
        assert value is None
        assert dict(parameters)[key] == "on"

        value, parameters = protocol_query(sock, "SHOW default_transaction_read_only")
        assert value == "on"
        assert key not in dict(parameters)
    finally:
        sock.close()


@pytest.mark.skipif(
    PG_MAJOR_VERSION < 14,
    reason="default_transaction_read_only was not reported before PostgreSQL 14",
)
async def test_tracked_dtr_is_replayed_without_backend_notification(bouncer):
    bouncer.admin("SET server_round_robin=1")
    await asyncio.gather(
        bouncer.asql("SELECT pg_sleep(0.5)", dbname="p0"),
        bouncer.asql("SELECT pg_sleep(0.5)", dbname="p0"),
    )

    with bouncer.conn(dbname="p0") as conn:
        conn.execute("SET default_transaction_read_only=on")
        assert conn.info.parameter_status("default_transaction_read_only") == "on"

        backend_pids = set()
        for _ in range(4):
            value, backend_pid = conn.execute(
                "SELECT current_setting('default_transaction_read_only'), "
                "pg_backend_pid()"
            ).fetchone()
            assert value == "on"
            assert conn.info.parameter_status("default_transaction_read_only") == "on"
            backend_pids.add(backend_pid)
        assert len(backend_pids) == 2


@pytest.mark.skipif(
    WINDOWS or not USE_UNIX_SOCKETS,
    reason="takeover test requires Unix sockets",
)
async def test_parameter_status_takeover_replaces_unknown_server(bouncer):
    async with bouncer.acur(dbname="parameter_status_takeover") as cur:
        await cur.execute("SELECT pg_backend_pid()")
        first_pid = (await cur.fetchone())[0]

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            servers = bouncer.admin("SHOW SERVERS", row_factory=dict_row)
            if any(
                server["database"] == "parameter_status_takeover"
                and server["state"] == "idle"
                for server in servers
            ):
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail("server did not become idle before takeover")

        await bouncer.reboot()

        await cur.execute("SELECT pg_backend_pid()")
        second_pid = (await cur.fetchone())[0]
        assert second_pid != first_pid


def terminate_idle_server(bouncer, pg, dbname):
    pid = bouncer.sql_value("SELECT pg_backend_pid()", dbname=dbname)
    assert pg.sql_value("SELECT pg_terminate_backend(%s)", (pid,)) is True

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        servers = bouncer.admin("SHOW SERVERS", row_factory=dict_row)
        if not any(server["database"] == dbname for server in servers):
            return
        time.sleep(0.05)
    pytest.fail(f"server for {dbname} was not removed")


@requires_replica
async def test_target_mismatch_fast_fails_later_client(bouncer, target_replica, pg):
    bouncer.admin("SET server_login_retry=2")
    bouncer.admin("SET client_login_timeout=8")
    terminate_idle_server(bouncer, pg, "tsa_retry_primary")
    log_offset = bouncer.log_path.stat().st_size

    first = bouncer.asql(
        "SELECT pg_is_in_recovery()", dbname="tsa_retry_primary", connect_timeout=10
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with bouncer.log_path.open() as log:
            log.seek(log_offset)
            if "server does not satisfy target_session_attrs" in log.read():
                break
        await asyncio.sleep(0.05)
    else:
        pytest.fail("target mismatch was not logged")

    with pytest.raises(
        psycopg.OperationalError,
        match=(
            r"server login has been failing, cached error: "
            r"server does not satisfy target_session_attrs"
        ),
    ):
        await bouncer.asql(
            "SELECT pg_is_in_recovery()",
            dbname="tsa_retry_primary",
            connect_timeout=10,
        )
    assert await first == [(False,)]


@requires_replica
async def test_target_mismatch_preserves_real_connection_failure(
    bouncer, target_replica, pg
):
    bouncer.admin("SET server_login_retry=1")
    bouncer.admin("SET client_login_timeout=8")
    terminate_idle_server(bouncer, pg, "tsa_mixed_failure")
    log_offset = bouncer.log_path.stat().st_size

    first = bouncer.asql(
        "SELECT pg_is_in_recovery()", dbname="tsa_mixed_failure", connect_timeout=10
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with bouncer.log_path.open() as log:
            log.seek(log_offset)
            content = log.read()
        if (
            "connect failed" in content
            and "server does not satisfy target_session_attrs" in content
        ):
            break
        await asyncio.sleep(0.05)
    else:
        pytest.fail("login failure followed by target mismatch was not logged")

    with pytest.raises(
        psycopg.OperationalError,
        match=r"server login has been failing, cached error: connect failed",
    ):
        await bouncer.asql(
            "SELECT pg_is_in_recovery()",
            dbname="tsa_mixed_failure",
            connect_timeout=10,
        )
    assert await first == [(False,)]


@requires_replica
def test_all_target_candidates_mismatch_times_out(bouncer, target_replica):
    bouncer.admin("SET client_login_timeout=3")
    with pytest.raises(
        psycopg.OperationalError, match=r"client_login_timeout \(server down\)"
    ):
        bouncer.test(dbname="tsa_all_mismatch", connect_timeout=10)


def test_target_session_attrs_version_boundary(bouncer):
    bouncer.test(dbname="tsa_version_any")
    if PG_MAJOR_VERSION >= 14:
        bouncer.test(dbname="tsa_version_primary")
    else:
        bouncer.admin("SET client_login_timeout=3")
        with pytest.raises(
            psycopg.OperationalError,
            match=r"client_login_timeout \(server down\)",
        ):
            bouncer.test(dbname="tsa_version_primary", connect_timeout=10)


@requires_replica
def test_target_session_attrs_reload_replaces_server(bouncer, target_replica):
    first_pid, first_in_recovery = bouncer.sql(
        "SELECT pg_backend_pid(), pg_is_in_recovery()", dbname="tsa_reload"
    )[0]
    assert first_in_recovery is True

    original = bouncer.ini_path.read_text()
    updated, replacements = re.subn(
        r"^(tsa_reload.*target_session_attrs=)any$",
        r"\1primary",
        original,
        flags=re.MULTILINE,
    )
    assert replacements == 1
    bouncer.ini_path.write_text(updated)
    bouncer.admin("RELOAD")
    bouncer.admin("SET server_login_retry=1")
    bouncer.admin("SET client_login_timeout=5")

    database = next(
        row
        for row in bouncer.admin("SHOW DATABASES", row_factory=dict_row)
        if row["name"] == "tsa_reload"
    )
    assert database["target_session_attrs"] == "primary"

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        servers = bouncer.admin("SHOW SERVERS", row_factory=dict_row)
        if not any(server["remote_pid"] == first_pid for server in servers):
            break
        time.sleep(0.05)
    else:
        pytest.fail("server from the old database configuration was not removed")

    second_pid, second_in_recovery = bouncer.sql(
        "SELECT pg_backend_pid(), pg_is_in_recovery()",
        dbname="tsa_reload",
        connect_timeout=10,
    )[0]
    assert second_in_recovery is False
    assert second_pid != first_pid


@requires_replica
async def test_target_session_attrs_takeover_reconnects_unknown_server(
    bouncer, target_replica
):
    first_pid, first_in_recovery = bouncer.sql(
        "SELECT pg_backend_pid(), pg_is_in_recovery()", dbname="tsa_takeover"
    )[0]
    assert first_in_recovery is True

    original = bouncer.ini_path.read_text()
    updated, replacements = re.subn(
        r"^(tsa_takeover.*target_session_attrs=)any$",
        r"\1primary",
        original,
        flags=re.MULTILINE,
    )
    assert replacements == 1
    bouncer.ini_path.write_text(updated)
    await bouncer.reboot()
    bouncer.admin("SET server_login_retry=1")
    bouncer.admin("SET client_login_timeout=5")

    second_pid, second_in_recovery = bouncer.sql(
        "SELECT pg_backend_pid(), pg_is_in_recovery()",
        dbname="tsa_takeover",
        connect_timeout=10,
    )[0]
    assert second_in_recovery is False
    assert second_pid != first_pid


def test_target_session_attrs_rejects_prefer_standby(bouncer):
    original = bouncer.ini_path.read_text()
    invalid, replacements = re.subn(
        r"target_session_attrs=primary$",
        "target_session_attrs=prefer-standby",
        original,
        count=1,
        flags=re.MULTILINE,
    )
    assert replacements == 1
    bouncer.ini_path.write_text(invalid)
    try:
        with pytest.raises(psycopg.errors.ConfigFileError):
            bouncer.admin("RELOAD")
    finally:
        bouncer.ini_path.write_text(original)
        bouncer.admin("RELOAD")
