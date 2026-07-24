/*
 * Operations on ParameterStatus values reported by PostgreSQL.
 */

#include "bouncer.h"

#include <usual/strpool.h>

struct ParameterStatus {
	struct PStr *name;
	struct PStr *value;
	struct ParameterStatus *next;
};

static struct StrPool *parameter_pool;

bool parameter_status_set(ParameterStatus **parameters,
			  const char *name,
			  const char *value)
{
	ParameterStatus *parameter;
	struct PStr *new_name;
	struct PStr *new_value;

	for (parameter = *parameters; parameter; parameter = parameter->next) {
		if (strcmp(parameter->name->str, name) == 0)
			break;
	}

	if (parameter && strcmp(parameter->value->str, value) == 0)
		return true;

	if (!parameter_pool) {
		parameter_pool = strpool_create(USUAL_ALLOC);
		if (!parameter_pool)
			return false;
	}

	new_value = strpool_get(parameter_pool, value, -1);
	if (!new_value)
		return false;

	if (parameter) {
		strpool_decref(parameter->value);
		parameter->value = new_value;
		return true;
	}

	new_name = strpool_get(parameter_pool, name, -1);
	if (!new_name) {
		strpool_decref(new_value);
		return false;
	}

	parameter = malloc(sizeof *parameter);
	if (!parameter) {
		strpool_decref(new_name);
		strpool_decref(new_value);
		return false;
	}

	parameter->name = new_name;
	parameter->value = new_value;
	parameter->next = *parameters;
	*parameters = parameter;
	return true;
}

const char *parameter_status_get(const ParameterStatus *parameters,
				 const char *name)
{
	for (; parameters; parameters = parameters->next) {
		if (strcmp(parameters->name->str, name) == 0)
			return parameters->value->str;
	}
	return NULL;
}

bool parameter_status_copy(ParameterStatus **dst, const ParameterStatus *src)
{
	ParameterStatus *copy = NULL;

	for (; src; src = src->next) {
		if (!parameter_status_set(&copy, src->name->str, src->value->str)) {
			parameter_status_clean(&copy);
			return false;
		}
	}

	parameter_status_clean(dst);
	*dst = copy;
	return true;
}

bool parameter_status_send_changes(const PgSocket *server, PgSocket *client)
{
	const ParameterStatus *parameter;
	const char *client_value;
	PktBuf *pkt = pktbuf_temp();
	bool send = false;

	for (parameter = server->parameters; parameter; parameter = parameter->next) {
		if (varcache_is_tracked(parameter->name->str))
			continue;

		client_value = parameter_status_get(client->parameters,
						    parameter->name->str);
		if (client_value &&
		    strcmp(client_value, parameter->value->str) == 0)
			continue;

		pktbuf_write_ParameterStatus(pkt,
					     parameter->name->str,
					     parameter->value->str);
		if (pkt->failed)
			return false;
		if (!parameter_status_set(&client->parameters,
					  parameter->name->str,
					  parameter->value->str))
			return false;
		send = true;
	}

	return !send || pktbuf_send_immediate(pkt, client);
}

void parameter_status_clean(ParameterStatus **parameters)
{
	ParameterStatus *parameter;

	while (*parameters) {
		parameter = *parameters;
		*parameters = parameter->next;
		strpool_decref(parameter->name);
		strpool_decref(parameter->value);
		free(parameter);
	}
}

void parameter_status_deinit(void)
{
	strpool_free(parameter_pool);
	parameter_pool = NULL;
}
