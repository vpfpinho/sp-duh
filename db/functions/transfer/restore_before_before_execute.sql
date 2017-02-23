DROP FUNCTION IF EXISTS transfer.restore_before_before_execute(bigint);
CREATE OR REPLACE FUNCTION transfer.restore_before_before_execute(
  company_id                  bigint
) RETURNS text AS $BODY$
DECLARE
  meta_schema                 text;
BEGIN

  EXECUTE
    FORMAT('SELECT * FROM transfer.create_meta_schema(%1$L)', company_id)
  INTO STRICT meta_schema;

  RETURN meta_schema;

END;
$BODY$ LANGUAGE 'plpgsql';