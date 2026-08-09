#include "bouncer.h"
#include "scram.h"
#include "common/postgres_compat.h"
#include "common/base64.h"
#include "common/scram-common.h"
#include "common/sha2.h"

#include <usual/logging.h>

#define PLUS_HEADER "p=tls-server-end-point,,"

static bool hash_failure;
static unsigned int hash_calls;

void log_generic(enum LogLevel level, void *ctx, const char *fmt, ...)
{
}

int tls_get_server_end_point_hash(struct tls *ctx, uint8_t *result,
				  size_t result_size, size_t *result_len)
{
	hash_calls++;
	if (result_len != NULL)
		*result_len = 0;
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

static int populate_remaining_state(ScramState *state)
{
	state->client_final_message_without_proof = strdup("client-final");
	state->server_nonce = strdup("server-nonce");
	state->server_first_message = strdup("server-first");
	state->salt = malloc(1);
	state->SaltedPassword = malloc(1);
	state->encoded_salt = strdup("encoded-salt");
	if (state->client_final_message_without_proof == NULL ||
	    state->server_nonce == NULL || state->server_first_message == NULL ||
	    state->salt == NULL || state->SaltedPassword == NULL ||
	    state->encoded_salt == NULL)
		return -1;

	state->iterations = 1;
	state->hash_type = PG_SHA256;
	state->key_length = SCRAM_SHA_256_KEY_LEN;
	state->saltlen = 1;
	state->adhoc = true;
	state->salt[0] = 1;
	state->SaltedPassword[0] = 1;
	memset(state->ClientKey, 1, sizeof(state->ClientKey));
	memset(state->StoredKey, 1, sizeof(state->StoredKey));
	memset(state->ServerKey, 1, sizeof(state->ServerKey));
	return 0;
}

static int assert_state_cleared(const ScramState *state, const char *exchange)
{
	ScramState empty;

	memset(&empty, 0, sizeof(empty));

	if (memcmp(state, &empty, sizeof(empty)) != 0) {
		fprintf(stderr, "%s SCRAM state was not cleared\n", exchange);
		return 1;
	}
	return 0;
}

static int reject_ordinary_exchange(PgSocket *client)
{
	char first[] = "n,,n=user,r=clientnonce";
	const uint8_t raw_final[] = "c=biws,r=clientnonce,p=not-base64";
	char final[sizeof(raw_final)];
	const char *nonce = NULL;
	char *proof = NULL;

	memcpy(final, raw_final, sizeof(raw_final));
	client->scram_state.channel_binding_in_use = false;
	if (!read_client_first_message(client, first)) {
		fprintf(stderr, "ordinary client-first-message was rejected\n");
		return 1;
	}
	if (read_client_final_message(client, raw_final, final, &nonce, &proof)) {
		fprintf(stderr, "ordinary malformed proof was accepted\n");
		free(proof);
		return 1;
	}
	if (proof != NULL) {
		fprintf(stderr, "ordinary rejected proof was retained\n");
		free(proof);
		return 1;
	}
	if (populate_remaining_state(&client->scram_state) != 0)
		return 1;

	free_scram_state(&client->scram_state);
	return assert_state_cleared(&client->scram_state, "ordinary");
}

static int reject_plus_hash_failure(PgSocket *client)
{
	uint8_t binding_data[sizeof(PLUS_HEADER) - 1 + PG_SHA256_DIGEST_LENGTH];
	uint8_t proof_data[SCRAM_SHA_256_KEY_LEN] = {0};
	char binding[pg_b64_enc_len(sizeof(binding_data)) + 1];
	char proof[pg_b64_enc_len(sizeof(proof_data)) + 1];
	char first[] = PLUS_HEADER "n=user,r=clientnonce";
	char raw_final[256];
	char final[sizeof(raw_final)];
	const char *nonce = NULL;
	char *decoded_proof = NULL;
	int binding_len;
	int proof_len;

	memcpy(binding_data, PLUS_HEADER, sizeof(PLUS_HEADER) - 1);
	memset(binding_data + sizeof(PLUS_HEADER) - 1, 0,
	       PG_SHA256_DIGEST_LENGTH);
	binding_len = pg_b64_encode(binding_data, sizeof(binding_data), binding,
				    sizeof(binding) - 1);
	proof_len = pg_b64_encode(proof_data, sizeof(proof_data), proof,
				  sizeof(proof) - 1);
	if (binding_len < 0 || proof_len < 0)
		return 1;
	binding[binding_len] = '\0';
	proof[proof_len] = '\0';
	snprintf(raw_final, sizeof(raw_final), "c=%s,r=clientnonce,p=%s",
		 binding, proof);
	memcpy(final, raw_final, sizeof(raw_final));

	client->sbuf.tls = (struct tls *)client;
	client->sbuf.tls_state = SBUF_TLS_OK;
	client->scram_state.channel_binding_in_use = true;
	if (!read_client_first_message(client, first)) {
		fprintf(stderr, "PLUS client-first-message was rejected\n");
		return 1;
	}

	hash_failure = true;
	hash_calls = 0;
	if (read_client_final_message(client, (const uint8_t *)raw_final, final,
				      &nonce, &decoded_proof)) {
		fprintf(stderr, "PLUS certificate hash failure was accepted\n");
		free(decoded_proof);
		return 1;
	}
	if (hash_calls != 1) {
		fprintf(stderr, "PLUS certificate hash was called %u times\n",
			hash_calls);
		return 1;
	}
	if (!client->scram_state.channel_binding_in_use ||
	    client->scram_state.cbind_flag != 'p') {
		fprintf(stderr, "PLUS hash failure reset mechanism state\n");
		return 1;
	}
	if (client->scram_state.client_first_message_bare == NULL ||
	    client->scram_state.client_nonce == NULL || decoded_proof != NULL) {
		fprintf(stderr, "PLUS hash failure continued or discarded exchange state\n");
		free(decoded_proof);
		return 1;
	}
	if (populate_remaining_state(&client->scram_state) != 0)
		return 1;

	free_scram_state(&client->scram_state);
	hash_failure = false;
	return assert_state_cleared(&client->scram_state, "PLUS");
}

int main(void)
{
	PgSocket client = {0};

	if (reject_ordinary_exchange(&client) != 0)
		return 1;
	if (reject_plus_hash_failure(&client) != 0)
		return 1;
	return 0;
}
