-- DROP FUNCTION IF EXISTS common.get_tablespace_name(TEXT);

CREATE OR REPLACE FUNCTION common.get_tablespace_name(
  IN a_schema_name TEXT
)
RETURNS TEXT AS $BODY$
DECLARE
  _tablespace_name TEXT;
BEGIN

  IF left(a_schema_name,11) = 'pt999999990' THEN
    _tablespace_name := right(regexp_replace(a_schema_name, '^pt\d{6}(\d{3}).*?(\d{1,3})$', '\1\2'),3);
  ELSE
    _tablespace_name := regexp_replace(a_schema_name, '^pt\d{6}(\d{3}).*', 'tablespace_\1');
  END IF;

  RETURN _tablespace_name;
END;
$BODY$ LANGUAGE 'plpgsql' IMMUTABLE;
