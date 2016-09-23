DROP FUNCTION IF EXISTS sharding.get_sharded_schema_name(INTEGER);

CREATE OR REPLACE FUNCTION sharding.get_sharded_schema_name (
  IN  company_id          INTEGER,
  OUT schema_name         TEXT)
RETURNS TEXT AS $BODY$
DECLARE
  _company_id  ALIAS FOR company_id;
  _schema_name ALIAS FOR schema_name;
BEGIN

  SELECT CASE WHEN c.use_sharded_company THEN c.schema_name ELSE 'public' END
    FROM public.companies c
    WHERE c.id = _company_id
  INTO STRICT _schema_name;

  RETURN;
END;
$BODY$ LANGUAGE 'plpgsql';
