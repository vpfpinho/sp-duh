CREATE OR REPLACE FUNCTION redis.delete_redis_server_connection(
  IN server_name      TEXT
)
RETURNS BOOLEAN AS $BODY$
DECLARE
BEGIN
  -- Create the server
  EXECUTE format($$
    DROP SERVER IF EXISTS %1$s_redis_server CASCADE;
  $$, server_name);

  RETURN TRUE;
END;
$BODY$ LANGUAGE plpgsql;