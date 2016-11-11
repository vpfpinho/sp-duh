DROP FUNCTION IF EXISTS sharding.table_exists(TEXT);

CREATE OR REPLACE FUNCTION sharding.table_exists (
  IN p_relation_name TEXT
)
RETURNS BOOLEAN AS $BODY$
DECLARE
  query TEXT;
  result BOOLEAN;
  schema_name TEXT;
  table_name TEXT;
BEGIN
  -- RAISE NOTICE 'SELECT sharding.table_exists(''%'', ''%'');', p_relation_name, p_trigger_name;

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
      FROM pg_catalog.pg_class c
        %1$s

      WHERE (c.relname %2$s) = (%4$L %3$s)
      );
    $$,
    CASE WHEN schema_name IS NOT NULL THEN 'LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace' END,
    CASE WHEN schema_name IS NOT NULL THEN ', n.nspname' END,
    CASE WHEN schema_name IS NOT NULL THEN format(', %1$L', schema_name) END,
    table_name
  );

  EXECUTE query INTO result;

  RETURN result;
END;
$BODY$ LANGUAGE 'plpgsql';
