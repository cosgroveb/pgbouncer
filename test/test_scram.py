import base64
import hashlib
import shutil
import ssl
import subprocess

import pytest

from .scram_client import ScramClient, ScramFailure
from .utils import (
    DIRECT_TLS_SUPPORT,
    PG_SUPPORTS_SCRAM,
    TLS_SUPPORT,
    USE_UNIX_SOCKETS,
    WINDOWS,
)

pytestmark = pytest.mark.skipif("not PG_SUPPORTS_SCRAM")

ORDINARY = b"SCRAM-SHA-256"
PLUS = b"SCRAM-SHA-256-PLUS"
GS2_N = b"n,,"
GS2_Y = b"y,,"
GS2_PLUS = b"p=tls-server-end-point,,"


@pytest.fixture
def restore_bouncer_password(pg):
    yield
    pg.sql("ALTER USER bouncer PASSWORD NULL")


def configure_scram(bouncer):
    bouncer.admin("set auth_type = 'scram-sha-256'")


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


def tls_scram_client(bouncer, context, **kwargs):
    return ScramClient(
        "localhost",
        bouncer.port,
        use_tls=True,
        ssl_context=context,
        **kwargs,
    )


def authentication_result(client, mechanism, gs2_header):
    try:
        return client.authenticate(mechanism, gs2_header)
    except ScramFailure:
        return "rejected"


def test_non_tls_advertises_only_ordinary(bouncer):
    configure_scram(bouncer)
    with ScramClient("127.0.0.1", bouncer.port) as client:
        assert client.mechanisms == [ORDINARY]


def test_non_scram_auth_does_not_advertise_sasl(bouncer):
    bouncer.admin("set auth_type = 'plain'")
    client = ScramClient("127.0.0.1", bouncer.port)
    with pytest.raises(
        ScramFailure, match="expected authentication code 10, received 3"
    ):
        client.connect()


@pytest.mark.skipif("not USE_UNIX_SOCKETS", reason="Unix sockets are unavailable")
def test_unix_socket_advertises_only_ordinary(bouncer):
    configure_scram(bouncer)
    with ScramClient(
        "localhost", bouncer.port, unix_socket_dir=bouncer.admin_host
    ) as client:
        assert client.mechanisms == [ORDINARY]


@pytest.mark.parametrize(
    "tls_version", [None, ssl.TLSVersion.TLSv1_2, ssl.TLSVersion.TLSv1_3]
)
def test_tls_advertises_plus_and_authenticates(bouncer, cert_dir, tls_version):
    context = configure_tls_scram(bouncer, cert_dir)
    if tls_version is not None:
        context.minimum_version = tls_version
        context.maximum_version = tls_version
    with tls_scram_client(bouncer, context) as client:
        assert client.mechanisms == [PLUS, ORDINARY]
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
    ("user", "password", "database", "auth_query", "expected"),
    [
        ("bouncer", "zzzz", "p0", False, "accepted"),
        ("scramuser1", "foo", "p62", False, "accepted"),
        ("scramuser3", "baz", "p61", False, "accepted"),
        ("someuser", "anypasswd", "authdb", True, "accepted"),
        ("nosuchuser", "whatever", "p0", False, "rejected"),
    ],
)
def test_plus_credential_sources(
    bouncer, cert_dir, user, password, database, auth_query, expected
):
    context = configure_tls_scram(bouncer, cert_dir)
    if auth_query:
        bouncer.write_ini("auth_user = pswcheck")
        bouncer.write_ini(
            "auth_query = SELECT usename, passwd FROM pg_shadow where usename = $1"
        )
        bouncer.admin("reload")
    with tls_scram_client(
        bouncer, context, user=user, password=password, database=database
    ) as client:
        assert client.authenticate(PLUS, GS2_PLUS) == expected


@pytest.mark.parametrize(
    ("user", "password", "database", "auth_query", "accepted"),
    [
        ("scramuser1", "foo", "p62", False, True),
        ("someuser", "anypasswd", "authdb", True, True),
        ("nosuchuser", "whatever", "p0", False, False),
    ],
)
def test_libpq_requires_plus(
    bouncer, cert_dir, user, password, database, auth_query, accepted
):
    configure_tls_scram(bouncer, cert_dir)
    if auth_query:
        bouncer.write_ini("auth_user = pswcheck")
        bouncer.write_ini(
            "auth_query = SELECT usename, passwd FROM pg_shadow where usename = $1"
        )
        bouncer.admin("reload")
    kwargs = {
        "host": "localhost",
        "user": user,
        "password": password,
        "dbname": database,
        "sslmode": "verify-full",
        "sslrootcert": cert_dir / "TestCA1" / "ca.crt",
        "channel_binding": "require",
    }
    if accepted:
        bouncer.psql_test(**kwargs)
    else:
        with pytest.raises(subprocess.CalledProcessError):
            bouncer.psql_test(**kwargs)


@pytest.mark.parametrize(
    ("use_tls", "mechanism", "gs2_header", "expected"),
    [
        (True, ORDINARY, GS2_N, "accepted"),
        (True, ORDINARY, GS2_Y, "rejected"),
        (True, ORDINARY, GS2_PLUS, "rejected"),
        (True, PLUS, GS2_PLUS, "accepted"),
        (True, PLUS, GS2_N, "rejected"),
        (True, PLUS, GS2_Y, "rejected"),
        (True, PLUS, b"p=tls-unique,,", "rejected"),
        (False, ORDINARY, GS2_N, "accepted"),
        (False, ORDINARY, GS2_Y, "accepted"),
        (False, PLUS, GS2_PLUS, "rejected"),
    ],
)
def test_mechanism_and_gs2_matrix(
    bouncer, cert_dir, use_tls, mechanism, gs2_header, expected
):
    if use_tls:
        context = configure_tls_scram(bouncer, cert_dir)
        client = tls_scram_client(bouncer, context)
    else:
        configure_scram(bouncer)
        client = ScramClient("127.0.0.1", bouncer.port)
    with client:
        assert authentication_result(client, mechanism, gs2_header) == expected


def test_mechanism_results_match_postgres(
    bouncer, pg, cert_dir, restore_bouncer_password
):
    context = configure_tls_scram(bouncer, cert_dir)
    pg.sql(
        "set password_encryption = 'scram-sha-256'; alter user bouncer password 'zzzz'"
    )
    pg.ssl_access("p0", "scram-sha-256", user="bouncer")
    pg.configure("ssl=on")
    if WINDOWS:
        pg.restart()
    else:
        pg.reload()
    cases = [
        (ORDINARY, GS2_N, "accepted"),
        (ORDINARY, GS2_Y, "rejected"),
        (PLUS, GS2_PLUS, "accepted"),
        (PLUS, GS2_N, "rejected"),
        (PLUS, GS2_Y, "rejected"),
        (PLUS, b"p=tls-unique,,", "rejected"),
    ]
    for mechanism, gs2_header, expected in cases:
        with ScramClient(
            "localhost", pg.port, use_tls=True, ssl_context=context
        ) as postgres:
            postgres_result = authentication_result(postgres, mechanism, gs2_header)
        with tls_scram_client(bouncer, context) as pgbouncer:
            pgbouncer_result = authentication_result(pgbouncer, mechanism, gs2_header)
        assert postgres_result == pgbouncer_result == expected


@pytest.mark.parametrize("channel_binding", [b"biws", b"eSws"])
def test_ordinary_rejects_mismatched_channel_binding(bouncer, channel_binding):
    configure_scram(bouncer)
    gs2_header = GS2_Y if channel_binding == b"biws" else GS2_N
    with ScramClient("127.0.0.1", bouncer.port) as client:
        client.begin(ORDINARY, gs2_header)
        assert client.finish(gs2_header, channel_binding=channel_binding) == "rejected"


@pytest.mark.parametrize(
    "case",
    [
        "empty",
        "not-base64",
        "n-header",
        "y-header",
        "other-type",
        "missing-padding",
        "extra-padding",
        "embedded-nul",
        "wrong",
        "short",
        "long",
    ],
)
def test_plus_rejects_invalid_channel_binding(bouncer, cert_dir, case):
    context = configure_tls_scram(bouncer, cert_dir)
    with tls_scram_client(bouncer, context) as client:
        client.begin(PLUS, GS2_PLUS)
        digest = bytearray(client.tls_server_end_point())
        if case == "empty":
            channel_binding = b""
        elif case == "not-base64":
            channel_binding = b"not-base64"
        elif case == "n-header":
            channel_binding = base64.b64encode(GS2_N + digest)
        elif case == "y-header":
            channel_binding = base64.b64encode(GS2_Y + digest)
        elif case == "other-type":
            channel_binding = base64.b64encode(b"p=tls-unique,," + digest)
        else:
            if case == "wrong":
                digest[0] ^= 1
            elif case == "short":
                digest.pop()
            elif case == "long":
                digest.append(0)
            channel_binding = base64.b64encode(GS2_PLUS + digest)
            if case == "missing-padding":
                channel_binding = channel_binding.rstrip(b"=")
            elif case == "extra-padding":
                channel_binding += b"="
            elif case == "embedded-nul":
                channel_binding += b"\0trailing"
        assert client.finish(GS2_PLUS, channel_binding=channel_binding) == "rejected"


def test_plus_rejects_binding_with_wrong_digest_algorithm(bouncer, cert_dir):
    context = configure_tls_scram(bouncer, cert_dir)
    with tls_scram_client(bouncer, context) as client:
        client.begin(PLUS, GS2_PLUS)
        digest = hashlib.sha384(client.peer_certificate).digest()
        channel_binding = base64.b64encode(GS2_PLUS + digest)
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
        old_hash = old_client.tls_server_end_point()
        shutil.copyfile(cert_dir / "TestCA2" / "sites" / "01-localhost.key", active_key)
        shutil.copyfile(
            cert_dir / "TestCA2" / "sites" / "01-localhost.crt", active_cert
        )
        bouncer.admin("reload")
        assert old_client.finish(GS2_PLUS) == "accepted"

        with ScramClient(
            "localhost", bouncer.port, use_tls=True, ssl_context=context
        ) as new_client:
            assert new_client.tls_server_end_point() != old_hash
            assert new_client.authenticate(PLUS, GS2_PLUS) == "accepted"

        with ScramClient(
            "localhost", bouncer.port, use_tls=True, ssl_context=context
        ) as mismatched_client:
            mismatched_client.begin(PLUS, GS2_PLUS)
            binding = base64.b64encode(GS2_PLUS + old_hash)
            assert (
                mismatched_client.finish(GS2_PLUS, channel_binding=binding)
                == "rejected"
            )


@pytest.mark.parametrize(
    "extensions", [b",x=", b",x=contains-p=marker", b",x=one,y=two"]
)
def test_plus_accepts_extensions_in_exact_transcript(bouncer, cert_dir, extensions):
    context = configure_tls_scram(bouncer, cert_dir)
    with tls_scram_client(bouncer, context) as client:
        client.begin(PLUS, GS2_PLUS)
        assert client.finish(GS2_PLUS, extensions=extensions) == "accepted"


@pytest.mark.parametrize(
    "trailing",
    [
        b",x=after",
        b",p=" + base64.b64encode(bytes(32)),
        b"\0trailing",
    ],
)
def test_plus_rejects_data_after_proof(bouncer, cert_dir, trailing):
    context = configure_tls_scram(bouncer, cert_dir)
    with tls_scram_client(bouncer, context) as client:
        client.begin(PLUS, GS2_PLUS)
        assert client.finish(GS2_PLUS, trailing=trailing) == "rejected"


@pytest.mark.parametrize(
    "proof",
    [base64.b64encode(b"x" * length) for length in (0, 1, 31, 33)]
    + [b"not-base64", b"====", b"AA=A"],
)
def test_plus_rejects_invalid_proof(bouncer, cert_dir, proof):
    context = configure_tls_scram(bouncer, cert_dir)
    with tls_scram_client(bouncer, context) as client:
        client.begin(PLUS, GS2_PLUS)
        assert client.finish(GS2_PLUS, proof=proof) == "rejected"


@pytest.mark.parametrize(
    "case",
    [
        "missing-c",
        "repeated-c",
        "missing-r",
        "repeated-r",
        "missing-proof",
        "trailing-comma",
        "embedded-nul",
    ],
)
def test_plus_final_message_boundaries(bouncer, cert_dir, case):
    context = configure_tls_scram(bouncer, cert_dir)
    with tls_scram_client(bouncer, context) as client:
        client.begin(PLUS, GS2_PLUS)
        binding = base64.b64encode(GS2_PLUS + client.tls_server_end_point())
        nonce = client.server_nonce
        proof = base64.b64encode(b"x" * 32)
        messages = {
            "missing-c": b"r=" + nonce + b",p=" + proof,
            "repeated-c": b"c="
            + binding
            + b",c="
            + binding
            + b",r="
            + nonce
            + b",p="
            + proof,
            "missing-r": b"c=" + binding + b",p=" + proof,
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


def test_initial_response_rejects_declared_length_one(bouncer):
    configure_scram(bouncer)
    with ScramClient("127.0.0.1", bouncer.port) as client, pytest.raises(ScramFailure):
        client.begin(ORDINARY, GS2_N, declared_length=1)


@pytest.mark.parametrize(
    "gs2_header",
    [
        b"",
        b"p=tls-server-end-point",
        b"p=TLS-SERVER-END-POINT,,",
        b"p=tls-server-end-point,authz,",
        GS2_PLUS + b"\0",
    ],
)
def test_plus_rejects_invalid_gs2_header(bouncer, cert_dir, gs2_header):
    context = configure_tls_scram(bouncer, cert_dir)
    with tls_scram_client(bouncer, context) as client, pytest.raises(ScramFailure):
        client.begin(PLUS, gs2_header)


@pytest.mark.parametrize(
    "client_first_bare",
    [
        b"r=clientnonce",
        b"n=bouncer",
        b"n=bouncer,r=not\x01printable",
        b"m=required,n=bouncer,r=clientnonce",
        b"n=bouncer,r=clientnonce\0trailing",
    ],
)
def test_plus_rejects_invalid_client_first(bouncer, cert_dir, client_first_bare):
    context = configure_tls_scram(bouncer, cert_dir)
    with tls_scram_client(bouncer, context) as client, pytest.raises(ScramFailure):
        client.begin(PLUS, GS2_PLUS, client_first_bare=client_first_bare)


def test_plus_accepts_empty_client_nonce_like_postgres(bouncer, cert_dir):
    context = configure_tls_scram(bouncer, cert_dir)
    with tls_scram_client(bouncer, context) as client:
        client.begin(PLUS, GS2_PLUS, client_first_bare=b"n=bouncer,r=")
        assert client.finish(GS2_PLUS) == "accepted"


@pytest.mark.parametrize(
    ("packet_length", "accepted"), [(99, True), (100, True), (101, False)]
)
def test_initial_response_at_packet_limit(bouncer, packet_length, accepted):
    configure_scram(bouncer)
    with bouncer.admin_runner.cur() as admin_cur:
        admin_cur.execute("set max_packet_size = 100")
        mechanism_overhead = len(ORDINARY) + 1 + 4
        protocol_overhead = 5
        initial_length = packet_length - mechanism_overhead - protocol_overhead
        prefix = b"n=bouncer,r="
        client_first = prefix + b"x" * (initial_length - len(GS2_N) - len(prefix))
        try:
            with ScramClient("127.0.0.1", bouncer.port) as client:
                if accepted:
                    client.begin(ORDINARY, GS2_N, client_first_bare=client_first)
                else:
                    with pytest.raises(ScramFailure):
                        client.begin(ORDINARY, GS2_N, client_first_bare=client_first)
        finally:
            admin_cur.execute("set max_packet_size = 2147483647")


@pytest.mark.parametrize(
    ("packet_length", "accepted"), [(255, True), (256, True), (257, False)]
)
def test_plus_final_message_at_packet_limit(bouncer, cert_dir, packet_length, accepted):
    context = configure_tls_scram(bouncer, cert_dir)
    with bouncer.admin_runner.cur() as admin_cur:
        admin_cur.execute("set max_packet_size = 256")
        try:
            with tls_scram_client(bouncer, context) as client:
                client.begin(PLUS, GS2_PLUS)
                binding = base64.b64encode(GS2_PLUS + client.tls_server_end_point())
                overhead = len(
                    b"p"
                    + bytes(4)
                    + b"c="
                    + binding
                    + b",r="
                    + client.server_nonce
                    + b",x=,p="
                    + base64.b64encode(bytes(32))
                )
                extensions = b",x=" + b"x" * (packet_length - overhead)
                assert (
                    client.finish(GS2_PLUS, extensions=extensions) == "accepted"
                ) is accepted
        finally:
            admin_cur.execute("set max_packet_size = 2147483647")
