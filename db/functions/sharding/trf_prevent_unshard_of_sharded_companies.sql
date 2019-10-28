-- DROP FUNCTION IF EXISTS sharding.trf_prevent_unshard_of_sharded_companies();

CREATE OR REPLACE FUNCTION sharding.trf_prevent_unshard_of_sharded_companies()
RETURNS TRIGGER AS $BODY$
DECLARE
BEGIN

  RAISE restrict_violation
    USING MESSAGE = format('Company %1$L has already been sharded, can''t be unsharded' , NEW.id),
          TABLE = TG_TABLE_NAME;

  RETURN OLD; -- not returning NEW
END;
$BODY$ LANGUAGE 'plpgsql';

-- DROP TRIGGER IF EXISTS trg_prevent_unshard_of_sharded_companies ON public.companies;
-- CREATE TRIGGER trg_prevent_unshard_of_sharded_companies
--   AFTER UPDATE OF use_sharded_company ON public.companies
--   FOR EACH ROW
--     WHEN (OLD.use_sharded_company = TRUE AND OLD.use_sharded_company IS DISTINCT FROM NEW.use_sharded_company)
--   EXECUTE PROCEDURE sharding.trf_prevent_unshard_of_sharded_companies();