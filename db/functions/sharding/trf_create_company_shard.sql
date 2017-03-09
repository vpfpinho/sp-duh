-- -- DROP FUNCTION IF EXISTS sharding.trf_create_company_shard() CASCADE;

CREATE OR REPLACE FUNCTION sharding.trf_create_company_shard()
RETURNS TRIGGER AS $BODY$
DECLARE
  old_search_path text;
BEGIN
  RAISE NOTICE 'Sharding company [%] % - % - %', NEW.id, NEW.tax_registration_number, COALESCE(NEW.business_name, NEW.company_name, '<unnamed>'), NEW.use_sharded_company;

  IF NEW.use_sharded_company THEN

    IF NULLIF(NEW.schema_name,'') IS NULL THEN
      NEW.schema_name := format('pt%1$s_c%2$s', NEW.tax_registration_number, NEW.id);
    END IF;

    -- Create company schema if necessary
    RAISE DEBUG 'Creating new schema "%"', NEW.schema_name;
    EXECUTE ('CREATE SCHEMA IF NOT EXISTS "' || NEW.schema_name || '";');
    PERFORM common.create_table_schema_migrations(NEW.schema_name);

    -- Shard company
    PERFORM sharding.create_company_shard(NEW.id, NEW.schema_name, lower(TG_OP)::sharding.sharding_triggered_by);

    SHOW search_path INTO old_search_path;
    EXECUTE 'SET search_path to '||NEW.schema_name||', '||old_search_path||'';

    RAISE DEBUG 'Creating new schema "%" ... DONE!', NEW.schema_name;
  END IF;

  RETURN NEW;
END;
$BODY$ LANGUAGE 'plpgsql';