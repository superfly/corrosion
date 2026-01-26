#ifndef CRSQLITE_H
#define CRSQLITE_H

#include "sqlite3.h"

/**
 * Extension initialization routine that is run once per connection.
 */
int sqlite3_crsqlite_init(sqlite3 *db, char **pzErrMsg,
                          const sqlite3_api_routines *pApi);

#endif