-- DROP FUNCTION IF EXISTS sharding.trf_create_company_shard();
CREATE OR REPLACE FUNCTION sharding.trf_create_company_shard()
RETURNS TRIGGER AS $BODY$
DECLARE
  old_search_path text;
  _current_cluster integer;
BEGIN

  SHOW cloudware.cluster INTO _current_cluster;

  IF NOT NEW.use_sharded_company THEN
    RAISE EXCEPTION 'The new company with % tax registration number must be created with sharded option', NEW.tax_registration_number;
  END IF;

  IF NULLIF(NEW.schema_name,'') IS NULL THEN
    NEW.schema_name := format('pt%1$s_c%2$s', NEW.tax_registration_number, NEW.id);
  END IF;

  IF sharding.moving_existing_data() THEN
    NEW.cluster := COALESCE(NEW.cluster, _current_cluster);
  END IF;
  IF _current_cluster IS DISTINCT FROM NEW.cluster THEN
    RAISE EXCEPTION 'Company is configured for cluster % [current cluster id %]', NEW.cluster, _current_cluster;
    RETURN NEW;
  END IF;

  -- Create company schema if necessary
  RAISE DEBUG 'Creating new schema "%"', NEW.schema_name;
  EXECUTE ('CREATE SCHEMA IF NOT EXISTS "' || NEW.schema_name || '";');
  PERFORM common.create_table_schema_migrations(NEW.schema_name);


  -- Shard company
  IF NEW.use_sharded_company AND NOT sharding.moving_existing_data() THEN
    PERFORM sharding.create_company_shard(NEW.id, NEW.schema_name);
  END IF;

  SHOW search_path INTO old_search_path;
  EXECUTE 'SET search_path to '||NEW.schema_name||', '||old_search_path||'';

  RAISE DEBUG 'Creating new schema "%" ... DONE!', NEW.schema_name;

  RETURN NEW;
END;
$BODY$ LANGUAGE 'plpgsql';