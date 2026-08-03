#include "bouncer.h"
#include "scram.h"
#include "common/sha2.h"

#include <usual/logging.h>

static bool hash_failure;

void log_generic(enum LogLevel level, void *ctx, const char *fmt, ...)
{
}

int tls_get_server_end_point_hash(struct tls *ctx, uint8_t *result,
				  size_t result_size, size_t *result_len)
{
	if (hash_failure || result_size < PG_SHA256_DIGEST_LENGTH)
		return -1;
	memset(result, 0, PG_SHA256_DIGEST_LENGTH);
	*result_len = PG_SHA256_DIGEST_LENGTH;
	return 0;
}

const char *tls_error(struct tls *ctx)
{
	return "injected certificate hash failure";
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
	PgSocket client = {0};
	char *input;

	if (size < 1 || size > 65536)
		return 0;
	input = malloc(size);
	if (input == NULL)
		return 0;
	memcpy(input, data + 1, size - 1);
	input[size - 1] = '\0';

	hash_failure = (data[0] & 8) != 0;
	client.scram_state.channel_binding_in_use = (data[0] & 1) != 0;
	client.scram_state.cbind_flag = client.scram_state.channel_binding_in_use
		? 'p' : ((data[0] & 2) != 0 ? 'y' : 'n');
	if ((data[0] & 4) != 0) {
		client.sbuf.tls = (struct tls *)&client;
		client.sbuf.tls_state = SBUF_TLS_OK;
	}

#ifdef FUZZ_CLIENT_FIRST
	read_client_first_message(&client, input);
#else
	{
		const char *nonce = NULL;
		char *proof = NULL;

		read_client_final_message(&client, data + 1, input, &nonce, &proof);
		free(proof);
	}
#endif

	free_scram_state(&client.scram_state);
	free(input);
	return 0;
}
