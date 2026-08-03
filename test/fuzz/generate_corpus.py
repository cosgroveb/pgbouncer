import base64
from pathlib import Path

CORPUS_DIR = Path(__file__).parent / "corpus"
ORDINARY = b"\x00"
Y = b"\x02"
PLUS = b"\x05"
DOWNGRADE = b"\x06"
PROOF = base64.b64encode(bytes(32))
PLUS_BINDING = base64.b64encode(b"p=tls-server-end-point,," + bytes(32))

client_first = {
    "authzid": PLUS + b"p=tls-server-end-point,a=other,n=user,r=clientnonce",
    "downgrade": DOWNGRADE + b"y,,n=user,r=clientnonce",
    "embedded_nul": PLUS + b"p=tls-server-end-point,,n=user,r=client\0nonce",
    "empty_nonce": PLUS + b"p=tls-server-end-point,,n=user,r=",
    "extensions": PLUS + b"p=tls-server-end-point,,n=user,r=clientnonce,x=one,y=two",
    "missing_username": PLUS + b"p=tls-server-end-point,,r=clientnonce",
    "ordinary": ORDINARY + b"n,,n=user,r=clientnonce",
    "oversized_nonce": ORDINARY + b"n,,n=user,r=" + b"x" * 32768,
    "plus": PLUS + b"p=tls-server-end-point,,n=user,r=clientnonce",
    "trunc_binding_equals": PLUS + b"p=",
    "trunc_binding_name": PLUS + b"p",
    "trunc_binding_value": PLUS + b"p=tls-server-end-point",
    "trunc_extension_equals": ORDINARY + b"n,,n=user,r=clientnonce,x=",
    "trunc_extension_name": ORDINARY + b"n,,n=user,r=clientnonce,x",
    "trunc_extension_value": ORDINARY + b"n,,n=user,r=clientnonce,x=one",
    "trunc_gs2_first_comma": ORDINARY + b"n,",
    "trunc_gs2_second_comma": ORDINARY + b"n,,",
    "trunc_nonce_comma": ORDINARY + b"n,,n=user,r=clientnonce,",
    "trunc_nonce_equals": ORDINARY + b"n,,n=user,r=",
    "trunc_nonce_name": ORDINARY + b"n,,n=user,r",
    "trunc_plus_first_comma": PLUS + b"p=tls-server-end-point,",
    "trunc_plus_second_comma": PLUS + b"p=tls-server-end-point,,",
    "trunc_username_comma": ORDINARY + b"n,,n=user,",
    "trunc_username_equals": ORDINARY + b"n,,n=",
    "trunc_username_name": ORDINARY + b"n,,n",
    "trunc_username_value": ORDINARY + b"n,,n=user",
    "truncated": ORDINARY + b"n",
}

ordinary_final = b"c=biws,r=nonce,p=" + PROOF
plus_final = b"c=" + PLUS_BINDING + b",r=nonce,p=" + PROOF
client_final = {
    "bad_binding": PLUS + b"c=not-base64,r=nonce,p=" + PROOF,
    "embedded_nul": ORDINARY + b"c=biws,r=nonce\0,p=" + PROOF,
    "extensions": ORDINARY + b"c=biws,r=nonce,x=one,y=two,p=" + PROOF,
    "malformed": PLUS + b"c=not-base64,r=,p=",
    "missing_binding": PLUS + b"r=nonce,p=" + PROOF,
    "ordinary": ORDINARY + ordinary_final,
    "oversized_extension": ORDINARY
    + b"c=biws,r=nonce,x="
    + b"x" * 32768
    + b",p="
    + PROOF,
    "plus": PLUS + plus_final,
    "trailing": PLUS + plus_final + b",x=after",
    "trunc_binding_equals": ORDINARY + b"c=",
    "trunc_binding_name": ORDINARY + b"c",
    "trunc_binding_value": ORDINARY + b"c=biws",
    "trunc_binding_comma": ORDINARY + b"c=biws,",
    "trunc_extension_comma": ORDINARY + b"c=biws,r=nonce,x=one,",
    "trunc_extension_equals": ORDINARY + b"c=biws,r=nonce,x=",
    "trunc_extension_name": ORDINARY + b"c=biws,r=nonce,x",
    "trunc_extension_value": ORDINARY + b"c=biws,r=nonce,x=one",
    "trunc_nonce_equals": ORDINARY + b"c=biws,r=",
    "trunc_nonce_name": ORDINARY + b"c=biws,r",
    "trunc_nonce_value": ORDINARY + b"c=biws,r=nonce",
    "trunc_nonce_comma": ORDINARY + b"c=biws,r=nonce,",
    "trunc_proof_equals": ORDINARY + b"c=biws,r=nonce,p=",
    "trunc_proof_name": ORDINARY + b"c=biws,r=nonce,p",
    "truncated": PLUS + b"c=" + PLUS_BINDING + b",r=nonce,p",
    "y": Y + b"c=eSws,r=nonce,p=" + PROOF,
}

corpora = {"client_first": client_first, "client_final": client_final}
for corpus_name, seeds in corpora.items():
    corpus_dir = CORPUS_DIR / corpus_name
    unexpected = {path.name for path in corpus_dir.iterdir()} - seeds.keys()
    if unexpected:
        raise SystemExit(f"unexpected {corpus_name} seeds: {sorted(unexpected)}")
    for seed_name, data in seeds.items():
        if not 1 <= len(data) <= 65536:
            raise SystemExit(f"invalid {corpus_name}/{seed_name} size: {len(data)}")
        seed_path = corpus_dir / seed_name
        seed_path.write_bytes(data)
        if seed_path.read_bytes() != data:
            raise SystemExit(f"incorrect bytes in {corpus_name}/{seed_name}")
        if data.endswith(b"\n"):
            raise SystemExit(f"incidental LF in {corpus_name}/{seed_name}")

print(f"wrote {sum(len(seeds) for seeds in corpora.values())} corpus seeds")
