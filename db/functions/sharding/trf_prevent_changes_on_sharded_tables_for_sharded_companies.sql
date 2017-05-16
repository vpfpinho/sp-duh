-- DROP FUNCTION IF EXISTS sharding.trf_prevent_changes_on_sharded_tables_for_sharded_companies() CASCADE;

CREATE OR REPLACE FUNCTION sharding.trf_prevent_changes_on_sharded_tables_for_sharded_companies()
RETURNS TRIGGER AS $BODY$
DECLARE
  _stack         text;
  _company_id    integer;
BEGIN

  GET DIAGNOSTICS _stack = PG_CONTEXT;
  IF _stack ~ 'sharding\.trf_shard_existing_data()' THEN
    RETURN CASE TG_OP WHEN 'DELETE' THEN OLD ELSE NEW END;
  END IF;

  EXECUTE 'SELECT ($1).company_id::integer' INTO _company_id USING (CASE TG_OP WHEN 'DELETE' THEN OLD ELSE NEW END);

  IF (SELECT use_sharded_company FROM public.companies WHERE id = _company_id) THEN
    RAISE restrict_violation
      USING MESSAGE = format('Company %1$L has already been sharded, can''t INSERT or UPDATE or DELETE records on unsharded tables' , _company_id),
            TABLE = TG_TABLE_NAME;
  END IF;

  RETURN CASE TG_OP WHEN 'DELETE' THEN OLD ELSE NEW END;
END;
$BODY$ LANGUAGE 'plpgsql';
