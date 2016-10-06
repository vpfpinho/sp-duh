CREATE OR REPLACE FUNCTION redis.create_trigger_to_delete_cache_keys(
  IN server_name      TEXT,
  IN table_name       TEXT,
  IN key_pattern      TEXT
)
RETURNS BOOLEAN AS $BODY$
DECLARE
  table_schema TEXT;
BEGIN
  SELECT "name", "schema"
  FROM common.get_table_schema_and_name(table_name)
  INTO table_name, table_schema;

  EXECUTE format($$
    CREATE CONSTRAINT TRIGGER trg_clear_redis_cache_from_%1$s
    AFTER UPDATE OR DELETE ON %2$I.%3$I
      DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
      EXECUTE PROCEDURE redis.trf_delete_affected_optimizations_cache_entries('%1$s', '%4$s');
  $$, server_name, table_schema, table_name, key_pattern);

  RETURN TRUE;
END;
$BODY$ LANGUAGE plpgsql;
