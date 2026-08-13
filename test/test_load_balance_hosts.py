import asyncio
import re
import time

import psycopg
import pytest

from .utils import wait_until


def _database_entry(bouncer, host, *, pool_size=None):
    entry = (
        f"host={host} port={bouncer.pg.port} dbname=p0 user=bouncer "
        "load_balance_hosts=disable"
    )
    if pool_size is not None:
        entry += f" pool_size={pool_size}"
    return entry


def _wait_for_dns_addresses(bouncer, hostname, expected):
    deadline = time.monotonic() + 5
    rows = []
    while time.monotonic() < deadline:
        rows = bouncer.admin("SHOW DNS_HOSTS")
        row = next((row for row in rows if row[0] == hostname), None)
        if row and {value.rsplit(":", 1)[0] for value in row[2].split(",")} == expected:
            return
        time.sleep(0.1)
    pytest.fail(f"DNS results did not refresh: {rows}")


async def test_load_balance_hosts_disable_good_first(bouncer):
    with bouncer.log_contains(r"127.0.0.1:\d+ new connection to server", 2):
        await bouncer.asleep(dbname="hostlist_good_first", duration=0.5, times=2)


async def test_load_balance_hosts_disable_bad_first(bouncer):
    bouncer.admin(f"set server_login_retry=1")
    with bouncer.log_contains(r"closing because: server DNS lookup failed", 1):
        with bouncer.log_contains(r"127.0.0.1:\d+ new connection to server", 2):
            # Execute two concurrent sleeps to force two backend connections.
            # The first connection will attempt the "bad" host and retry on
            # the "good" host.
            # The second connection will honor `load_balance_hosts` and use the
            # `disable` host.
            await bouncer.asleep(dbname="hostlist_bad_first", duration=0.5, times=2)


def test_load_balance_hosts_reload(bouncer):
    with bouncer.admin_runner.cur() as cur:
        results = cur.execute("show databases").fetchall()
        result = [r for r in results if r[0] == "load_balance_hosts_update"][0]
        assert "disable" in result

    with bouncer.ini_path.open() as f:
        original = f.read()
    with bouncer.ini_path.open("w") as f:
        f.write(
            re.sub(
                r"^(load_balance_hosts_update.*load_balance_hosts=)disable",
                "\\1round-robin",
                original,
                flags=re.MULTILINE,
            )
        )

    bouncer.admin("reload")

    with bouncer.admin_runner.cur() as cur:
        results = cur.execute("show databases").fetchall()
        result = [r for r in results if r[0] == "load_balance_hosts_update"][0]
        assert "round-robin" in result


async def test_dns_address_affinity(bouncer, dns_hosts_file, loopback_pg):
    hostname = dns_hosts_file.names["multi"]
    common = (
        f"host={hostname} port={bouncer.pg.port} dbname=p0 user=bouncer pool_size=3"
    )
    config = bouncer.config_with_databases(
        {
            "dns_address_round_robin": f"{common} load_balance_hosts=round-robin",
            "dns_address_disable": f"{common} load_balance_hosts=disable",
        }
    )

    async def addresses(dbname):
        results = await asyncio.gather(
            *[
                bouncer.asql(
                    "SELECT host(inet_server_addr()), pg_sleep(0.5)",
                    dbname=dbname,
                )
                for _ in range(3)
            ]
        )
        return [result[0][0] for result in results]

    with bouncer.run_with_config(config):
        round_robin_addresses = await addresses("dns_address_round_robin")
        disable_addresses = await addresses("dns_address_disable")

    assert set(round_robin_addresses) == {"127.0.0.2", "127.0.0.3"}
    assert len(set(disable_addresses)) == 1


def test_dns_address_exhaustion(bouncer, dns_hosts_file, loopback_pg):
    hostname = dns_hosts_file.names["multi"]
    config = bouncer.config_with_databases(
        {
            "dns_address_exhaustion": _database_entry(bouncer, f"{hostname},127.0.0.1"),
        }
    ).replace("[pgbouncer]\n", "[pgbouncer]\nserver_login_retry = 1\n", 1)

    with bouncer.run_with_config(config):
        first_address = bouncer.sql_value(
            "SELECT host(inet_server_addr())", dbname="dns_address_exhaustion"
        )
        assert first_address in {"127.0.0.2", "127.0.0.3"}

        loopback_pg.stop()
        bouncer.admin("RECONNECT dns_address_exhaustion")
        started = time.monotonic()
        address = bouncer.sql_value(
            "SELECT host(inet_server_addr())", dbname="dns_address_exhaustion"
        )

    assert address == "127.0.0.1"
    assert time.monotonic() - started >= 2


def test_dns_duplicate_addresses_do_not_exhaust(bouncer, dns_hosts_file, loopback_pg):
    hostname = dns_hosts_file.names["multi"]
    dns_hosts_file.replace(
        {
            hostname: ["127.0.0.2", "127.0.0.2", "127.0.0.3", "127.0.0.3"],
        }
    )
    config = bouncer.config_with_databases(
        {
            "dns_duplicate_addresses": _database_entry(bouncer, hostname),
        }
    ).replace("[pgbouncer]\n", "[pgbouncer]\nserver_login_retry = 1\n", 1)

    with bouncer.run_with_config(config):
        preferred = bouncer.sql_value(
            "SELECT host(inet_server_addr())", dbname="dns_duplicate_addresses"
        )
        other = "127.0.0.3" if preferred == "127.0.0.2" else "127.0.0.2"
        rows = bouncer.admin("SHOW DNS_HOSTS")
        row = next(row for row in rows if row[0] == hostname)
        resolved = [value.rsplit(":", 1)[0] for value in row[2].split(",")]
        if resolved.count(preferred) < 2:
            pytest.skip("DNS backend deduplicates repeated addresses")

        loopback_pg.stop()
        with loopback_pg.conf_path.open("a") as pgconf:
            pgconf.write(f"listen_addresses = '{other}'\n")
        loopback_pg.start()

        for _ in wait_until("old PgBouncer backend did not close"):
            servers = bouncer.admin("SHOW SERVERS")
            if not any(
                row[2] == "dns_duplicate_addresses" and row[5] == preferred
                for row in servers
            ):
                break

        started = time.monotonic()
        address = bouncer.sql_value(
            "SELECT host(inet_server_addr())", dbname="dns_duplicate_addresses"
        )

    assert address == other
    assert time.monotonic() - started >= 1


def test_dns_lookup_failure_advances_host(bouncer):
    config = bouncer.config_with_databases(
        {
            "dns_lookup_failure": _database_entry(
                bouncer, "unresolvable-hostname,127.0.0.1"
            ),
        }
    ).replace("[pgbouncer]\n", "[pgbouncer]\nserver_login_retry = 1\n", 1)

    with bouncer.run_with_config(config):
        started = time.monotonic()
        address = bouncer.sql_value(
            "SELECT host(inet_server_addr())", dbname="dns_lookup_failure"
        )

    assert address == "127.0.0.1"
    assert time.monotonic() - started >= 1


def test_literal_address_failure_advances_host(bouncer):
    config = bouncer.config_with_databases(
        {
            "literal_address_failure": _database_entry(bouncer, "127.0.0.3,127.0.0.1"),
        }
    ).replace("[pgbouncer]\n", "[pgbouncer]\nserver_login_retry = 1\n", 1)

    with bouncer.run_with_config(config):
        started = time.monotonic()
        address = bouncer.sql_value(
            "SELECT host(inet_server_addr())", dbname="literal_address_failure"
        )

    assert address == "127.0.0.1"
    assert time.monotonic() - started >= 1


async def _test_dns_address_refresh(
    bouncer, dns_hosts_file, loopback_pg, test_name, replacement
):
    hostname = dns_hosts_file.names["multi"]
    database = f"dns_address_refresh_{test_name}"
    config = bouncer.config_with_databases(
        {
            database: _database_entry(bouncer, hostname, pool_size=3)
            + " pool_mode=session",
        }
    ).replace("[pgbouncer]\n", "[pgbouncer]\ndns_max_ttl = 1\n", 1)

    with bouncer.run_with_config(config):
        with bouncer.cur(dbname=database) as preferred_cur:
            preferred = preferred_cur.execute(
                "SELECT host(inet_server_addr())"
            ).fetchone()[0]
            addresses = replacement or [
                address
                for address in ["127.0.0.2", "127.0.0.3"]
                if address != preferred
            ]
            expected = set(addresses)
            dns_hosts_file.replace({hostname: addresses})

            expires_after = time.monotonic() + 1.1
            while time.monotonic() < expires_after:
                time.sleep(0.05)

            results = await asyncio.gather(
                *[
                    bouncer.asql(
                        "SELECT host(inet_server_addr()), pg_sleep(0.5)",
                        dbname=database,
                    )
                    for _ in range(2)
                ]
            )
            _wait_for_dns_addresses(bouncer, hostname, expected)
            new_addresses = {result[0][0] for result in results}

    if replacement:
        assert new_addresses == {preferred}
    else:
        assert new_addresses == expected


async def test_dns_address_refresh_retains_preference(
    bouncer, dns_hosts_file, loopback_pg
):
    await _test_dns_address_refresh(
        bouncer,
        dns_hosts_file,
        loopback_pg,
        "retains_preference",
        ["127.0.0.3", "127.0.0.2"],
    )


async def test_dns_address_refresh_removes_preference(
    bouncer, dns_hosts_file, loopback_pg
):
    await _test_dns_address_refresh(
        bouncer,
        dns_hosts_file,
        loopback_pg,
        "removes_preference",
        None,
    )
