#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef struct FlexQL_DB_Internal FlexQL_DB;
typedef FlexQL_DB FlexQL;

#define FLEXQL_OK 0
#define FLEXQL_ERROR 1

int flexql_open(const char* host, int port, FlexQL_DB** db);
int flexql_close(FlexQL_DB* db);
int flexql_exec(FlexQL_DB* db,
                const char* sql,
                int (*callback)(void*, int, char**, char**),
                void* arg,
                char** errmsg);
void flexql_free(void* ptr);

#ifdef __cplusplus
}
#endif
