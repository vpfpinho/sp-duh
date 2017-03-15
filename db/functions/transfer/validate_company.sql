DROP FUNCTION IF EXISTS transfer.validate_company(bigint);
CREATE OR REPLACE FUNCTION transfer.validate_company(
  company_id      bigint
) RETURNS VOID AS $BODY$
DECLARE
  is_sharded      boolean;
BEGIN

  -- Assert that the company exists!

  IF NOT EXISTS(SELECT 1 FROM public.companies WHERE id = company_id) THEN
    RAISE EXCEPTION 'The company does not exist.'
      USING ERRCODE = 'BR001';
  END IF;

  -- Assert that the company can be backed up: only sharded companies can!

  EXECUTE
    FORMAT('SELECT use_sharded_company FROM public.companies WHERE id = %1$L', company_id)
  INTO STRICT is_sharded;

  IF NOT is_sharded THEN
    RAISE EXCEPTION 'Only sharded companies can be backed up and transferred.'
      USING ERRCODE = 'BR002';
  END IF;

END;
$BODY$ LANGUAGE 'plpgsql';