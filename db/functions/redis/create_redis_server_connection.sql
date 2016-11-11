CREATE OR REPLACE FUNCTION redis.create_redis_server_connection(
  IN server_name      TEXT,
  IN host             TEXT DEFAULT 'localhost',
  IN port             INTEGER DEFAULT 6379,
  IN database_number  INTEGER DEFAULT 0
)
RETURNS BOOLEAN AS $BODY$
DECLARE
BEGIN
  CREATE EXTENSION IF NOT EXISTS redis_fdw;

  -- Create the server
  EXECUTE format($$
    CREATE SERVER %1$s_redis_server
      FOREIGN DATA WRAPPER redis_fdw
      OPTIONS (host '%2$s', port '%3$s');
  $$, server_name, host, port, database_number);

  -- Create the user mapping
  EXECUTE format($$
    CREATE USER MAPPING FOR PUBLIC
      SERVER %1$s_redis_server;
  $$, server_name, host, port, database_number);

  -- Create the cache entries table
  EXECUTE format($$
    CREATE FOREIGN TABLE redis.%1$s_cache_entries (
      "key" TEXT,
      "value" TEXT
    )
      SERVER %1$s_redis_server
      OPTIONS (tabletype 'string', database '%4$s');
  $$, server_name, host, port, database_number);

  EXECUTE format($$
    ALTER FOREIGN TABLE redis.%1$s_cache_entries
      ALTER COLUMN "value"
      OPTIONS (ADD redis 'value');
  $$, server_name, host, port, database_number);

  EXECUTE format($$
    CREATE FOREIGN TABLE redis.%1$s_cache_keys(
      "key" TEXT
    )
      SERVER %1$s_redis_server
      OPTIONS (tabletype 'keys', database '%4$s');
  $$, server_name, host, port, database_number);

  RETURN TRUE;
END;
$BODY$ LANGUAGE plpgsql;