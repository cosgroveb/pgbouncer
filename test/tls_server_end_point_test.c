#include <stdio.h>

#include <usual/logging.h>
#include <usual/tls/tls.h>

#ifdef USUAL_LIBSSL_FOR_TLS

#include <openssl/ssl.h>

#include <usual/tls/tls_internal.h>

#ifdef TEST_WRAP_X509_DIGEST
static bool fail_x509_digest;
#endif

struct test_tls {
	SSL_CTX *ssl_ctx;
	SSL *ssl;
	struct tls tls_ctx;
};

static int init_test_tls(struct test_tls *test, const char *certificate_path)
{
	if ((test->ssl_ctx = SSL_CTX_new(SSLv23_server_method())) == NULL)
		return -1;
	if (certificate_path != NULL &&
	    SSL_CTX_use_certificate_chain_file(test->ssl_ctx, certificate_path) != 1)
		return 0;
	if ((test->ssl = SSL_new(test->ssl_ctx)) == NULL)
		return -1;
	test->tls_ctx.flags = TLS_SERVER_CONN;
	test->tls_ctx.ssl_conn = test->ssl;
	return 1;
}

static int finish_tls(struct test_tls *test, const char *name, const char *error)
{
#ifdef TEST_WRAP_X509_DIGEST
	fail_x509_digest = false;
#endif
	if (error != NULL) {
		fprintf(stderr, "%s%s%s\n", name == NULL ? "" : name,
			name == NULL ? "" : ": ", error);
	}
	SSL_free(test->ssl);
	SSL_CTX_free(test->ssl_ctx);
	return error != NULL;
}

static bool hash_rejected(struct tls *tls_ctx, size_t result_size)
{
	uint8_t result[EVP_MAX_MD_SIZE];
	size_t result_len = sizeof(result);

	return tls_get_server_end_point_hash(tls_ctx, result, result_size,
					     &result_len) != 0 &&
	       result_len == 0;
}

#ifdef TEST_WRAP_X509_DIGEST
int __real_X509_digest(const X509 *data, const EVP_MD *type,
		       unsigned char *md, unsigned int *len);
int __wrap_X509_digest(const X509 *data, const EVP_MD *type,
		       unsigned char *md, unsigned int *len);

int __wrap_X509_digest(const X509 *data, const EVP_MD *type,
		       unsigned char *md, unsigned int *len)
{
	if (fail_x509_digest)
		return 0;
	return __real_X509_digest(data, type, md, len);
}
#endif

static int test_certificate(const char *name, int expected_digest_nid)
{
	char certificate_path[256];
	uint8_t expected[EVP_MAX_MD_SIZE], actual[EVP_MAX_MD_SIZE];
	size_t actual_len;
	struct test_tls test = {0};
	X509 *certificate;
	const EVP_MD *expected_md;
	const char *path = NULL;
	unsigned int expected_len;
	int init_result;

	if (name != NULL) {
		snprintf(certificate_path, sizeof(certificate_path),
			 "ssl/channel-binding/%s.crt", name);
		path = certificate_path;
	}
	init_result = init_test_tls(&test, path);
	if (init_result == 0 && expected_digest_nid == NID_undef) {
		printf("%s: could not load certificate fixture; skipping\n", name);
		return finish_tls(&test, NULL, NULL);
	}
	if (init_result != 1)
		return finish_tls(&test, name, "could not initialize TLS test");
	if (expected_digest_nid == NID_undef) {
		if (hash_rejected(&test.tls_ctx, sizeof(actual)))
			return finish_tls(&test, NULL, NULL);
		return finish_tls(&test, name, name == NULL
					  ? "missing certificate left a result"
					  : "unsupported signature algorithm left a result");
	}
	expected_md = EVP_get_digestbynid(expected_digest_nid);
	if (expected_md == NULL)
		return finish_tls(&test, name, "expected digest is unavailable");
	certificate = SSL_get_certificate(test.ssl);
	if (certificate == NULL ||
	    X509_digest(certificate, expected_md, expected, &expected_len) != 1)
		return finish_tls(&test, name, "could not calculate expected hash");

	if (tls_get_server_end_point_hash(&test.tls_ctx, actual, sizeof(actual),
					  &actual_len) != 0)
		return finish_tls(&test, name, tls_error(&test.tls_ctx));
	if (actual_len != expected_len || memcmp(actual, expected, actual_len) != 0)
		return finish_tls(&test, name, "certificate hash mismatch");
	if (!hash_rejected(&test.tls_ctx, expected_len - 1))
		return finish_tls(&test, name, "undersized output buffer left a result");
	if (tls_get_server_end_point_hash(&test.tls_ctx, actual, sizeof(actual),
					  NULL) == 0)
		return finish_tls(&test, name, "null result length was accepted");
#ifdef TEST_WRAP_X509_DIGEST
	fail_x509_digest = true;
	if (!hash_rejected(&test.tls_ctx, sizeof(actual)))
		return finish_tls(&test, name, "X509_digest failure left a result");
#endif
	return finish_tls(&test, NULL, NULL);
}

#endif

int main(void)
{
#ifdef USUAL_LIBSSL_FOR_TLS
	static const struct {
		const char *name;
		int digest_nid;
	} certificates[] = {{"sha256", NID_sha256},
			    {"sha384", NID_sha384},
			    {"sha512", NID_sha512},
			    {"sha1", NID_sha256},
			    {"md5", NID_sha256},
#ifdef HAVE_X509_GET_SIGNATURE_INFO
			    {"rsa-pss", NID_sha256},
#endif
	};
	size_t i;
	int failures = 0;

	for (i = 0; i < sizeof(certificates) / sizeof(certificates[0]); i++)
		failures += test_certificate(certificates[i].name, certificates[i].digest_nid);
	failures += test_certificate(NULL, NID_undef);
	failures += test_certificate("ed25519", NID_undef);
#ifndef HAVE_X509_GET_SIGNATURE_INFO
	failures += test_certificate("rsa-pss", NID_undef);
#endif
	if (failures != 0)
		return 1;
	printf("TLS server end point test OK\n");
#else
	uint8_t actual[1];
	size_t actual_len = sizeof(actual);

	if (tls_get_server_end_point_hash(NULL, actual, sizeof(actual),
					  &actual_len) == 0 ||
	    actual_len != 0) {
		fprintf(stderr, "TLS compatibility stub left a result\n");
		return 1;
	}
	printf("TLS server end point compatibility stub test OK\n");
#endif
	return 0;
}
