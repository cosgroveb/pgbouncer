import psycopg
import pytest
import re
import time

from .utils import capture, run


@pytest.mark.asyncio
async def test_load_balance_hosts_disable_good_first(bouncer):
    with bouncer.log_contains(r"127.0.0.1:\d+ new connection to server", 2):
        await bouncer.asleep(dbname="hostlist_good_first", duration=0.5, times=2)


@pytest.mark.asyncio
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
        result = [r for r in results if r[0] == 'load_balance_hosts_update'][0]
        assert "disable" in result

    with bouncer.ini_path.open() as f:
        original = f.read()
    with bouncer.ini_path.open("w") as f:
        f.write(re.sub(r"^(load_balance_hosts_update.*load_balance_hosts=)disable", "\\1round-robin", original, flags=re.MULTILINE))

    bouncer.admin("reload")

    with bouncer.admin_runner.cur() as cur:
        results = cur.execute("show databases").fetchall()
        result = [r for r in results if r[0] == 'load_balance_hosts_update'][0]
        assert "round-robin" in result

def test_load_balance_hosts_disable_with_dns(bouncer, pg):
    bouncer.default_db = "dns_load_balance_hosts_disable"
    hosts = f"""
127.0.0.10       dnsdbhost
127.0.0.11       dnsdbhost
127.0.0.12       dnsdbhost
    """

    # I have modified utils.py to set listen_addresses='*' for this
    #  demonstrationsbut would use pg.configure/pg.reload for a real test
    with bouncer.run_with_appended_etc_hosts(hosts):
        subprocess_result = capture(
            ["getent", "hosts", "dnsdbhost"],
        )
        getent_result = subprocess_result.split("\n")
        assert "127.0.0.10      dnsdbhost" == getent_result[0]
        assert "127.0.0.11      dnsdbhost" == getent_result[1]
        assert "127.0.0.12      dnsdbhost" == getent_result[2]

        bouncer.sql(query="SELECT 1", user="bouncer", password="zzzz", dbname="dns_load_balance_hosts_disable")
        with bouncer.admin_runner.cur() as cur:
            results = cur.execute("SHOW DNS_HOSTS").fetchall()
            result = [r for r in results if r[0] == 'dnsdbhost'][0]

            assert result[2] != "127.0.0.10:0"
