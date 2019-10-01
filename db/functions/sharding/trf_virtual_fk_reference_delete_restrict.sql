-- JOANA: tested on public.tax_exemption_reasons / pt999999990_c2425.documents
-- DROP FUNCTION IF EXISTS sharding.trf_virtual_fk_reference_delete_restrict();
CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_restrict()
RETURNS TRIGGER AS $BODY$
DECLARE
  _current_cluster integer;
  specific_company_id integer;
  specific_schema_name TEXT;
  company_schema_name TEXT;
  culprit_schemas TEXT[];
  referencing_columns TEXT[];
  referencing_table TEXT;
  referenced_columns TEXT[];
  referenced_values TEXT[];
  trigger_condition JSONB;
BEGIN
  RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_restrict() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
  RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_restrict() -        OLD: %', OLD;

  referencing_columns := TG_ARGV[0];
  referencing_table := TG_ARGV[1];
  referenced_columns := TG_ARGV[2];
  trigger_condition := TG_ARGV[3];

  -- Extract the values from the OLD record into the referenced_values variable
  -- Extract the values from the NEW record into the referenced_values variable
  EXECUTE format(
    format('SELECT ARRAY[%s]::TEXT[]',
      (SELECT array_to_string((SELECT array_agg('($1).%I'::TEXT) FROM (SELECT generate_series(1::integer, array_length(referenced_columns, 1)::integer)) bogus), ', '))
    ),
    VARIADIC referenced_columns
  ) USING OLD INTO referenced_values;

  trigger_condition := sharding.merge_jsonb_with_arrays_of_keys_and_values(trigger_condition, referencing_columns, referenced_values);

  RAISE DEBUG 'trigger_condition: %', trigger_condition;

  -- Try to get the company schema from the referencing table (in case it's supplied as <schema>.<table>)
  IF (SELECT EXISTS (SELECT 1 FROM regexp_matches(referencing_table, '^.+\..+$'))) THEN
    SELECT (regexp_matches(referencing_table, '^(.+?)\..+?'))[1] INTO specific_schema_name;
    SELECT regexp_replace(referencing_table, specific_schema_name || '.', '') INTO referencing_table;
  ELSIF ( sharding.get_auxiliary_table_information()->'unsharded_tables' ? referencing_table ) THEN
    specific_schema_name := 'public';
  ELSIF TG_TABLE_NAME = 'companies' THEN
    specific_company_id := OLD.id;
  ELSE
    BEGIN
      specific_company_id := OLD.company_id;
      EXCEPTION
        WHEN undefined_column THEN
          specific_company_id := NULL;
    END;
  END IF;

  SHOW cloudware.cluster INTO _current_cluster;
  FOR company_schema_name IN
    SELECT pg_namespace.nspname
      FROM pg_catalog.pg_class
      JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
      LEFT JOIN public.companies ON NOT companies.is_deleted AND companies.schema_name = pg_namespace.nspname AND companies.cluster = _current_cluster
     WHERE pg_class.relkind = 'r' AND pg_class.relname = referencing_table
       AND ( pg_namespace.nspname = 'public' OR companies.id IS NOT NULL )
       AND ( specific_schema_name IS NULL OR pg_namespace.nspname = specific_schema_name )
       AND ( specific_company_id IS NULL OR companies.id = specific_company_id )
  LOOP
      RAISE DEBUG 'company_schema_name = %', company_schema_name;
      IF sharding.check_record_existence(format('%1$I.%2$I', company_schema_name, referencing_table), trigger_condition) THEN
        culprit_schemas := culprit_schemas || company_schema_name;
      END IF;
  END LOOP;

  IF array_length(culprit_schemas, 1) > 0 THEN
    RAISE foreign_key_violation
      USING MESSAGE = format('Can''t delete record. Tuple (%1$s) exists in %2$s schema(s): %3$s', array_to_string(referenced_values, ', '), array_length(culprit_schemas, 1), array_to_string(culprit_schemas, ', ')),
            SCHEMA = TG_TABLE_SCHEMA,
            TABLE = TG_TABLE_NAME
    ;
  END IF;

  RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_restrict() - RETURN OLD: %', OLD;
  RETURN OLD;
END;
$BODY$ LANGUAGE 'plpgsql';
