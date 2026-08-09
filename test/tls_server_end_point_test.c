#include <stdio.h>

#include <usual/tls/tls.h>

#ifdef USUAL_LIBSSL_FOR_TLS

#include <openssl/ssl.h>

#include <usual/tls/tls_internal.h>

#ifdef TEST_WRAP_X509_DIGEST
static bool fail_x509_digest;

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

static int read_expected_hash(const char *path, uint8_t *result, size_t result_size,
			      size_t *result_len)
{
	FILE *file;
	unsigned int byte;
	size_t len = 0;

	file = fopen(path, "r");
	if (file == NULL)
		return -1;
	while (fscanf(file, "%2x", &byte) == 1) {
		if (len == result_size) {
			fclose(file);
			return -1;
		}
		result[len++] = byte;
	}
	if (!feof(file)) {
		fclose(file);
		return -1;
	}
	fclose(file);
	*result_len = len;
	return 0;
}

static int test_certificate(const char *name)
{
	char certificate_path[256];
	char expected_path[256];
	uint8_t expected[EVP_MAX_MD_SIZE];
	uint8_t actual[EVP_MAX_MD_SIZE];
	size_t expected_len;
	size_t actual_len;
	size_t undersized_len;
	SSL_CTX *ssl_ctx = NULL;
	SSL *ssl = NULL;
	struct tls tls_ctx = {0};
	int result = 1;

	snprintf(certificate_path, sizeof(certificate_path),
		 "ssl/channel-binding/%s.crt", name);
	snprintf(expected_path, sizeof(expected_path),
		 "ssl/channel-binding/%s.hex", name);
	if (read_expected_hash(expected_path, expected, sizeof(expected),
			       &expected_len) != 0) {
		fprintf(stderr, "%s: could not read expected hash\n", name);
		goto done;
	}

	ssl_ctx = SSL_CTX_new(SSLv23_server_method());
	if (ssl_ctx == NULL ||
	    SSL_CTX_use_certificate_chain_file(ssl_ctx, certificate_path) != 1) {
		fprintf(stderr, "%s: could not load certificate\n", name);
		goto done;
	}
	ssl = SSL_new(ssl_ctx);
	if (ssl == NULL) {
		fprintf(stderr, "%s: could not create SSL connection\n", name);
		goto done;
	}
	tls_ctx.flags = TLS_SERVER_CONN;
	tls_ctx.ssl_conn = ssl;

	if (tls_get_server_end_point_hash(&tls_ctx, actual, sizeof(actual),
					  &actual_len) != 0) {
		fprintf(stderr, "%s: helper failed: %s\n", name, tls_error(&tls_ctx));
		goto done;
	}
	if (actual_len != expected_len || memcmp(actual, expected, actual_len) != 0) {
		fprintf(stderr, "%s: certificate hash mismatch\n", name);
		goto done;
	}
	undersized_len = actual_len - 1;
	actual_len = sizeof(actual);
	if (tls_get_server_end_point_hash(&tls_ctx, actual, undersized_len,
					  &actual_len) == 0 ||
	    actual_len != 0) {
		fprintf(stderr, "%s: undersized output buffer left a result\n", name);
		goto done;
	}
	if (tls_get_server_end_point_hash(&tls_ctx, actual, sizeof(actual), NULL) == 0) {
		fprintf(stderr, "%s: null result length was accepted\n", name);
		goto done;
	}
#ifdef TEST_WRAP_X509_DIGEST
	fail_x509_digest = true;
	actual_len = sizeof(actual);
	if (tls_get_server_end_point_hash(&tls_ctx, actual, sizeof(actual),
					  &actual_len) == 0 ||
	    actual_len != 0) {
		fprintf(stderr, "%s: X509_digest failure left a result\n", name);
		goto done;
	}
	fail_x509_digest = false;
#endif

	result = 0;
done:
#ifdef TEST_WRAP_X509_DIGEST
	fail_x509_digest = false;
#endif
	SSL_free(ssl);
	SSL_CTX_free(ssl_ctx);
	return result;
}

static int test_missing_certificate(void)
{
	uint8_t actual[EVP_MAX_MD_SIZE];
	size_t actual_len;
	SSL_CTX *ssl_ctx;
	SSL *ssl;
	struct tls tls_ctx = {0};
	int result = 1;

	ssl_ctx = SSL_CTX_new(SSLv23_server_method());
	if (ssl_ctx == NULL)
		return 1;
	ssl = SSL_new(ssl_ctx);
	if (ssl == NULL)
		goto done;
	tls_ctx.flags = TLS_SERVER_CONN;
	tls_ctx.ssl_conn = ssl;
	actual_len = sizeof(actual);
	if (tls_get_server_end_point_hash(&tls_ctx, actual, sizeof(actual),
					  &actual_len) == 0 ||
	    actual_len != 0) {
		fprintf(stderr, "missing certificate left a result\n");
		goto done;
	}
	result = 0;
done:
	SSL_free(ssl);
	SSL_CTX_free(ssl_ctx);
	return result;
}

static int test_unsupported_certificate(const char *name)
{
	char certificate_path[256];
	uint8_t actual[EVP_MAX_MD_SIZE];
	size_t actual_len;
	SSL_CTX *ssl_ctx;
	SSL *ssl = NULL;
	struct tls tls_ctx = {0};
	int result = 1;

	snprintf(certificate_path, sizeof(certificate_path),
		 "ssl/channel-binding/%s.crt", name);
	ssl_ctx = SSL_CTX_new(SSLv23_server_method());
	if (ssl_ctx == NULL)
		return 1;
	if (SSL_CTX_use_certificate_chain_file(ssl_ctx, certificate_path) != 1) {
		printf("%s: unsupported certificate fixture unavailable; skipping\n", name);
		result = 0;
		goto done;
	}
	ssl = SSL_new(ssl_ctx);
	if (ssl == NULL)
		goto done;
	tls_ctx.flags = TLS_SERVER_CONN;
	tls_ctx.ssl_conn = ssl;
	actual_len = sizeof(actual);
	if (tls_get_server_end_point_hash(&tls_ctx, actual, sizeof(actual),
					  &actual_len) == 0 ||
	    actual_len != 0) {
		fprintf(stderr, "%s: unsupported signature algorithm left a result\n", name);
		goto done;
	}
	result = 0;
done:
	SSL_free(ssl);
	SSL_CTX_free(ssl_ctx);
	return result;
}

#endif

int main(void)
{
#ifdef USUAL_LIBSSL_FOR_TLS
	static const char *certificates[] = {
		"sha256",
		"sha384",
		"sha512",
		"sha1",
		"md5",
#ifdef HAVE_X509_GET_SIGNATURE_INFO
		"rsa-pss",
#endif
	};
	size_t i;
	int failures = 0;

	for (i = 0; i < sizeof(certificates) / sizeof(certificates[0]); i++)
		failures += test_certificate(certificates[i]);
	failures += test_missing_certificate();
	failures += test_unsupported_certificate("ed25519");
#ifndef HAVE_X509_GET_SIGNATURE_INFO
	failures += test_unsupported_certificate("rsa-pss");
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
