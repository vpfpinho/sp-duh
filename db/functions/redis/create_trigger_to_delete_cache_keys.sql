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

  RAISE NOTICE 'table: %.%', table_schema, table_name;

  EXECUTE format($$
    CREATE TRIGGER trg_mark_redis_cache_to_clear_from_%1$s
    BEFORE UPDATE OR DELETE ON %2$I.%3$I
    FOR EACH ROW
      EXECUTE PROCEDURE redis.trf_mark_optimizations_cache_entries_for_deletion('%1$s', '%4$s');
  $$, server_name, table_schema, table_name, key_pattern);

  EXECUTE format($$
    CREATE TRIGGER trg_clear_redis_cache_from_%1$s
    AFTER UPDATE OR DELETE ON %2$I.%3$I
    FOR EACH STATEMENT
      EXECUTE PROCEDURE redis.trf_delete_affected_optimizations_cache_entries('%1$s', '%4$s');
  $$, server_name, table_schema, table_name, key_pattern);

  RETURN TRUE;
END;
$BODY$ LANGUAGE plpgsql;
