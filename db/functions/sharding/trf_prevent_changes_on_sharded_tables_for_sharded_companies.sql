DROP FUNCTION IF EXISTS sharding.trf_prevent_changes_on_sharded_tables_for_sharded_companies() CASCADE;

CREATE OR REPLACE FUNCTION sharding.trf_prevent_changes_on_sharded_tables_for_sharded_companies()
RETURNS TRIGGER AS $BODY$
DECLARE
  _stack         text;
BEGIN

  GET DIAGNOSTICS _stack = PG_CONTEXT;
  IF _stack ~ 'sharding\.trf_shard_existing_data()' THEN
    RETURN NEW;
  END IF;

  IF (SELECT use_sharded_company FROM public.companies WHERE id = NEW.company_id) THEN
    RAISE restrict_violation
      USING MESSAGE = format('Company %1$L has already been sharded, can''t INSERT or UPDATE records on unsharded tables' , NEW.company_id),
            TABLE = TG_TABLE_NAME;
  END IF;

  RETURN NEW;
END;
$BODY$ LANGUAGE 'plpgsql';
