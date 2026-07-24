typedef struct ParameterStatus ParameterStatus;

bool parameter_status_set(ParameterStatus **parameters,
			  const char *name,
			  const char *value) _MUSTCHECK;
const char *parameter_status_get(const ParameterStatus *parameters,
				 const char *name);
bool parameter_status_copy(ParameterStatus **dst,
			   const ParameterStatus *src) _MUSTCHECK;
bool parameter_status_send_changes(const PgSocket *server,
				   PgSocket *client) _MUSTCHECK;
bool parameter_status_queue_changes(PgSocket *server,
				    PgSocket *client) _MUSTCHECK;
void parameter_status_clean(ParameterStatus **parameters);
void parameter_status_deinit(void);
