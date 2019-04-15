-- DROP FUNCTION IF EXISTS sharding.trf_virtual_fk_reference_delete_restrict() CASCADE;

CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_restrict()
RETURNS TRIGGER AS $BODY$
DECLARE
  _current_cluster integer;
  company_schema_name TEXT;
  culprit_schemas TEXT[];
  referencing_columns TEXT[];
  referencing_table TEXT;
  referenced_columns TEXT[];
  referenced_values TEXT[];
  trigger_condition JSONB;
BEGIN
  -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_restrict() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
  -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_restrict() -        OLD: %', OLD;

  referencing_columns := TG_ARGV[0];
  referencing_table := TG_ARGV[1];
  referenced_columns := TG_ARGV[2];
  trigger_condition := TG_ARGV[3];

  IF TG_TABLE_NAME = 'users' THEN
    IF OLD.company_id IS NULL THEN
      RETURN OLD;
    END IF;
  END IF;

  IF trigger_condition IS NOT NULL THEN
    trigger_condition := sharding.merge_jsonb_with_arrays_of_keys_and_values(trigger_condition, referencing_columns, referenced_values);
  END IF;

  -- Try to get the company schema from th referencing table (in case it's supplied as <schema>.<table>)
  IF (SELECT EXISTS (SELECT 1 FROM regexp_matches(referencing_table, '^.+\..+$'))) THEN
    SELECT (regexp_matches(referencing_table, '^(.+?)\..+?'))[1] INTO company_schema_name;
    SELECT regexp_replace(referencing_table, company_schema_name || '.', '') INTO referencing_table;
  ELSIF TG_TABLE_NAME = 'companies' THEN
    IF ( sharding.get_auxiliary_table_information()->'unsharded_tables' ? referencing_table ) THEN
      company_schema_name := 'public';
    ELSIF OLD.use_sharded_company THEN 
      company_schema_name := OLD.schema_name;
    ELSE
      company_schema_name := 'public';
    END IF;
  ELSE
      company_schema_name := COALESCE(sharding.get_schema_name_for_table(OLD.company_id, referencing_table),'public');
  END IF;

  -- RAISE DEBUG 'company_schema_name = %', company_schema_name;
  IF company_schema_name IS NOT NULL THEN
    -- This table has a company_id column, check just the associated schema
      IF sharding.check_record_existence(format('%1$I.%2$I', company_schema_name, referencing_table), trigger_condition) THEN
        culprit_schemas := culprit_schemas || company_schema_name;
      END IF;
  ELSE
    -- The table does not have a company_id column, check all company schemas
    SHOW cloudware.cluster INTO _current_cluster;
    FOR company_schema_name IN
      SELECT schema_name FROM public.companies WHERE use_sharded_company AND NOT is_deleted AND cluster = _current_cluster
    LOOP
      -- RAISE DEBUG 'company_schema_name = %', company_schema_name;
      IF sharding.check_record_existence(format('%1$I.%2$I', company_schema_name, referencing_table), trigger_condition) THEN
        culprit_schemas := culprit_schemas || company_schema_name;
      END IF;
    END LOOP;
  END IF;

  IF array_length(culprit_schemas, 1) > 0 THEN
    RAISE foreign_key_violation
      USING MESSAGE = format('Can''t delete record. Tuple (%1$s) exists in %2$s schema(s): %3$s', array_to_string(referenced_values, ', '), array_length(culprit_schemas, 1), array_to_string(culprit_schemas, ', ')),
            SCHEMA = TG_TABLE_SCHEMA,
            TABLE = TG_TABLE_NAME
    ;
  END IF;

  -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_restrict() - RETURN OLD: %', OLD;
  RETURN OLD;
END;
$BODY$ LANGUAGE 'plpgsql';
