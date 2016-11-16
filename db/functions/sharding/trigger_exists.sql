DROP FUNCTION IF EXISTS sharding.trigger_exists(TEXT, TEXT);

CREATE OR REPLACE FUNCTION sharding.trigger_exists (
  IN p_relation_name TEXT,
  IN p_trigger_name TEXT
)
RETURNS BOOLEAN AS $BODY$
DECLARE
  query TEXT;
  result BOOLEAN;
  schema_name TEXT;
  table_name TEXT;
BEGIN
  -- RAISE NOTICE 'SELECT sharding.trigger_exists(''%'', ''%'');', p_relation_name, p_trigger_name;

  IF (SELECT EXISTS (SELECT 1 FROM regexp_matches(p_relation_name, '^.+\..+$'))) THEN
    SELECT (regexp_matches(p_relation_name, '^(.+?)\..+?'))[1] INTO schema_name;
    SELECT regexp_replace(p_relation_name, schema_name || '.', '') INTO table_name;
  ELSE
    schema_name := NULL;
    table_name := p_relation_name;
  END IF;

  query := format(
    $$
      SELECT EXISTS (
      SELECT 1
      FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        %1$s
      WHERE NOT t.tgisinternal
        AND c.relname = %3$L
        AND t.tgname = %4$L
        %2$s
      );
    $$,
    CASE WHEN schema_name IS NOT NULL THEN 'JOIN pg_namespace n ON c.relnamespace = n.oid' END,
    CASE WHEN schema_name IS NOT NULL THEN format('AND n.nspname = %1$L', schema_name) END,
    table_name,
    p_trigger_name
  );

  EXECUTE query INTO result;

  RETURN result;
END;
$BODY$ LANGUAGE 'plpgsql';
