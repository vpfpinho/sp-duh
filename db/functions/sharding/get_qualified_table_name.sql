DROP FUNCTION IF EXISTS sharding.get_qualified_table_name(INTEGER, TEXT);

CREATE OR REPLACE FUNCTION sharding.get_qualified_table_name(
  IN  company_id      INTEGER,
  IN  table_name      TEXT,
  OUT qualified_table TEXT)
RETURNS TEXT AS $BODY$
DECLARE
  _company_id ALIAS FOR company_id;
  _table_name ALIAS FOR table_name;
BEGIN

  IF ( sharding.get_auxiliary_table_information()->'unsharded_tables' ? _table_name ) THEN
    qualified_table := 'public.'|| _table_name;
  ELSE
    SELECT (CASE WHEN use_sharded_company THEN schema_name ELSE 'public' END )||'.'||_table_name
      FROM public.companies
      WHERE id = _company_id
    INTO qualified_table;
  END IF;

  RETURN;
END;
$BODY$ LANGUAGE 'plpgsql';
