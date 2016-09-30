CREATE OR REPLACE FUNCTION redis.delete_trigger_to_delete_cache_keys(
  IN server_name      TEXT,
  IN table_name       TEXT
)
RETURNS BOOLEAN AS $BODY$
DECLARE
  table_schema TEXT;
BEGIN

  table_schema := COALESCE(NULLIF(regexp_replace(table_name, '^(?:(.*?\.))?(.*?)$', '\1'), ''), 'public');
  table_name := regexp_replace(table_name, '^(?:.*?\.)?(.*?)$', '\1');

  EXECUTE format($$
    DROP TRIGGER trg_clear_redis_cache_from_%1$s ON %2$I.%3$I;
  $$, server_name, table_schema, table_name);

  RETURN TRUE;
END;
$BODY$ LANGUAGE plpgsql;
