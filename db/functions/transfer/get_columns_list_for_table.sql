DROP FUNCTION IF EXISTS transfer.get_columns_list_for_table(text, text);
CREATE OR REPLACE FUNCTION transfer.get_columns_list_for_table(
  schema_name   text,
  table_name    text
) RETURNS text[] AS $BODY$
DECLARE
  columns_list  text[];
BEGIN

  WITH table_columns AS (
    SELECT
      a.attname AS name
    FROM pg_catalog.pg_attribute a
      JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_catalog.pg_tables t ON c.oid = (t.schemaname || '.' || t.tablename)::regclass::oid
    WHERE a.attnum > 0
      AND NOT a.attisdropped
      AND n.nspname = schema_name
      AND t.tablename = table_name
  )
  SELECT array_agg(tc.name) FROM table_columns tc
  INTO columns_list;

  RETURN columns_list;

END;
$BODY$ LANGUAGE 'plpgsql';