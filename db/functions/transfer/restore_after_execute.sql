DROP FUNCTION IF EXISTS transfer.restore_after_execute(bigint);
CREATE OR REPLACE FUNCTION transfer.restore_after_execute(
  company_id      bigint
) RETURNS VOID AS $BODY$
DECLARE
BEGIN

  -- Assert that the company was restored and is valid!
  PERFORM transfer.validate_company(company_id);

END;
$BODY$ LANGUAGE 'plpgsql';