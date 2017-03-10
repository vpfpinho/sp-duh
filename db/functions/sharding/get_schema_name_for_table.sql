-- DROP FUNCTION IF EXISTS sharding.get_schema_name_for_table(INTEGER, TEXT);

CREATE OR REPLACE FUNCTION sharding.get_schema_name_for_table(
  IN  company_id          INTEGER,
  IN  table_name          TEXT,
  OUT table_schema_name   TEXT)
RETURNS TEXT AS $BODY$
DECLARE
  _company_id ALIAS FOR company_id;
  _table_name ALIAS FOR table_name;
BEGIN

  IF ( sharding.get_auxiliary_table_information()->'unsharded_tables' ? _table_name ) THEN
    table_schema_name := 'public';
  ELSE
    table_schema_name := sharding.get_sharded_schema_name(_company_id);
  END IF;

  RETURN;
END;
$BODY$ LANGUAGE 'plpgsql';
