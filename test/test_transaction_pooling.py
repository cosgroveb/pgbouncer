import psycopg
import pytest

from .utils import Bouncer

def test_set_local_is_passed_through(bouncer):
    bouncer.default_db = "transaction_pooled_db"
    bouncer.admin("RELOAD")
    with bouncer.transaction() as client1:
        assert client1.execute("SHOW statement_timeout").fetchone()[0] == "0"
        client1.execute("SET LOCAL statement_timeout = 99")
        assert client1.execute("SHOW statement_timeout").fetchone()[0] == "99ms"
    with bouncer.transaction() as client2:
        assert client2.execute("SHOW statement_timeout").fetchone()[0] == "0"

def test_set_becomes_set_local_when_enabled(bouncer):
    bouncer.default_db = "transaction_pooled_db"
    # todo: implement this flag
    bouncer.write_ini(";server_transaction_pool_set_local=1")
    bouncer.admin("RELOAD")
    with bouncer.transaction() as client1:
        assert client1.execute("SHOW statement_timeout").fetchone()[0] == "0"
        client1.execute("SET statement_timeout = 99")
        assert client1.execute("SHOW statement_timeout").fetchone()[0] == "99ms"
    with bouncer.transaction() as client2:
        assert client2.execute("SHOW statement_timeout").fetchone()[0] != "99ms"
