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
    SELECT CASE WHEN c.use_sharded_company THEN c.schema_name ELSE 'public' END
      FROM public.companies c
     WHERE c.id = _company_id
    INTO table_schema_name;
  END IF;

  RETURN;
END;
$BODY$ LANGUAGE 'plpgsql';
