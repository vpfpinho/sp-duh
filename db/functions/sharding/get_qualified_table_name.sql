-- DROP FUNCTION IF EXISTS sharding.get_qualified_table_name(INTEGER, TEXT);

CREATE OR REPLACE FUNCTION sharding.get_qualified_table_name(
  IN  company_id      INTEGER,
  IN  table_name      TEXT,
  OUT qualified_table TEXT)
RETURNS TEXT AS $BODY$
BEGIN

  qualified_table := sharding.get_schema_name_for_table(company_id, table_name) || '.' || _table_name;

  RETURN;
END;
$BODY$ LANGUAGE 'plpgsql';
