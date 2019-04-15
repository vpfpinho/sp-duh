-- DROP FUNCTION IF EXISTS sharding.trf_virtual_fk_reference_delete_cascade() CASCADE;

CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_cascade()
RETURNS TRIGGER AS $BODY$
DECLARE
  _current_cluster integer;
  company_schema_name TEXT;
  referencing_columns TEXT[];
  referencing_table TEXT;
  referenced_columns TEXT[];
  referenced_values TEXT[];
  trigger_condition_clause TEXT;
  query TEXT;
BEGIN
  -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_cascade() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
  -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_cascade() -        OLD: %', OLD;

  referencing_columns := TG_ARGV[0];
  referencing_table := TG_ARGV[1];
  referenced_columns := TG_ARGV[2];
  trigger_condition_clause := TG_ARGV[3];

  IF TG_TABLE_NAME = 'users' THEN
    IF OLD.company_id IS NULL THEN
      RETURN OLD;
    END IF;
  END IF;

  -- Extract the values from the NEW record into the referenced_values variable
  EXECUTE format(
    format('SELECT ARRAY[%s]::TEXT[]',
      (SELECT array_to_string((SELECT array_agg('($1).%I'::TEXT) FROM (SELECT generate_series(1::integer, array_length(referenced_columns, 1)::integer)) bogus), ', '))
    ),
    VARIADIC referenced_columns
  ) USING OLD INTO referenced_values;

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

  IF company_schema_name IS NOT NULL THEN
    IF NOT common.schema_exists(company_schema_name) THEN
      company_schema_name := NULL;
    END IF;
    -- RAISE DEBUG 'The table has a company_id column, delete just from the associated schema';
    IF company_schema_name IS NOT NULL THEN
      query := format('DELETE FROM %1$I.%2$I WHERE %3$s',
        company_schema_name,
        referencing_table,
        array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
      );

      IF trigger_condition_clause IS NOT NULL THEN
        query := query || ' AND ' || trigger_condition_clause;
      END IF;

      -- RAISE DEBUG 'query: %', query;
      EXECUTE query;
    END IF;
  ELSE
    -- RAISE DEBUG 'The table does not have a company_id column, update all schemas';
    SHOW cloudware.cluster INTO _current_cluster;
    FOR company_schema_name IN
      SELECT schema_name FROM public.companies WHERE use_sharded_company AND NOT is_deleted AND cluster = _current_cluster
    LOOP
      -- RAISE DEBUG 'company_schema_name = %', company_schema_name;
      query := format('DELETE FROM %1$I.%2$I WHERE %3$s',
        company_schema_name,
        referencing_table,
        array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
      );

      IF trigger_condition_clause IS NOT NULL THEN
        query := query || ' AND ' || trigger_condition_clause;
      END IF;

      -- RAISE DEBUG 'query: %', query;
      EXECUTE query;
    END LOOP;
  END IF;

  -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_cascade() - RETURN OLD: %', OLD;
  RETURN OLD;
END;
$BODY$ LANGUAGE 'plpgsql';
