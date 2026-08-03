#! /bin/sh

set -eu

output_dir="$(dirname "$0")/channel-binding"

certificate_hash()
{
	certificate=$1
	digest=$2
	openssl x509 -in "$certificate" -outform DER |
		openssl dgst "-$digest" -binary |
		od -An -v -tx1 |
		tr -d ' \n'
}

check_fixtures()
{
	failed=0
	for expected in "$output_dir"/*.hex; do
		name=$(basename "$expected" .hex)
		case "$name" in
		md5|sha1) digest=sha256 ;;
		*) digest=$name ;;
		esac
		if test "$name" = rsa-pss; then
			digest=sha256
		fi
		actual=$(certificate_hash "$output_dir/$name.crt" "$digest")
		expected_value=$(tr -d ' \n' < "$expected")
		if test "$actual" != "$expected_value"; then
			echo "$name certificate hash does not match $expected" >&2
			failed=1
		fi
	done
	return "$failed"
}

if test "${1-}" = --check; then
	check_fixtures
	exit
fi

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/pgbouncer-channel-binding.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM

rm -rf "$output_dir"
mkdir -p "$output_dir"

for digest in sha256 sha384 sha512 sha1 md5; do
	openssl req -new -x509 -newkey rsa:2048 -nodes -days 3650 \
		-subj "/CN=PgBouncer channel binding $digest" \
		-"$digest" \
		-keyout "$tmp_dir/$digest.key" \
		-out "$output_dir/$digest.crt" >/dev/null 2>&1
	selected_digest=$digest
	case "$digest" in
	md5|sha1) selected_digest=sha256 ;;
	esac
	certificate_hash "$output_dir/$digest.crt" "$selected_digest" \
		> "$output_dir/$digest.hex"
done

if openssl req -new -x509 -newkey rsa:2048 -nodes -days 3650 \
	-subj "/CN=PgBouncer channel binding RSA-PSS" \
	-sha256 -sigopt rsa_padding_mode:pss \
	-keyout "$tmp_dir/rsa-pss.key" \
	-out "$output_dir/rsa-pss.crt" >/dev/null 2>&1; then
	certificate_hash "$output_dir/rsa-pss.crt" sha256 \
		> "$output_dir/rsa-pss.hex"
else
	echo "OpenSSL cannot generate an RSA-PSS certificate; skipping" >&2
fi

if ! openssl req -new -x509 -newkey ed25519 -nodes -days 3650 \
	-subj "/CN=PgBouncer channel binding unsupported signature" \
	-keyout "$tmp_dir/ed25519.key" \
	-out "$output_dir/ed25519.crt" >/dev/null 2>&1; then
	echo "OpenSSL cannot generate an Ed25519 certificate; skipping" >&2
fi

check_fixtures
