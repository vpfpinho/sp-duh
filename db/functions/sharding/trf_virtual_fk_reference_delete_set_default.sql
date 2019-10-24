-- DROP FUNCTION IF EXISTS sharding.trf_virtual_fk_reference_delete_set_default();

CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_set_default()
RETURNS TRIGGER AS $BODY$
DECLARE
  specific_company_id integer;
  specific_schema_name TEXT;
  table_to_update TEXT;
  referencing_columns TEXT[];
  referencing_table TEXT;
  referenced_columns TEXT[];
  referenced_values TEXT[];
  trigger_condition_clause TEXT;
  query TEXT;
BEGIN
  -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_default() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
  -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_default() -        OLD: %', OLD;

  referencing_columns := TG_ARGV[0];
  referencing_table := TG_ARGV[1];
  referenced_columns := TG_ARGV[2];
  trigger_condition_clause := TG_ARGV[3];

  -- Extract the values from the OLD record into the referenced_values variable
  EXECUTE format(
    format('SELECT ARRAY[%s]::TEXT[]',
      (SELECT array_to_string((SELECT array_agg('($1).%I'::TEXT) FROM (SELECT generate_series(1::integer, array_length(referenced_columns, 1)::integer)) bogus), ', '))
    ),
    VARIADIC referenced_columns
  ) USING OLD INTO referenced_values;

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

  FOR table_to_update IN
    SELECT format('%I.%I', pg_namespace.nspname, pg_class.relname)
      FROM pg_catalog.pg_class
      JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
      LEFT JOIN public.companies ON companies.schema_name = pg_namespace.nspname
     WHERE pg_class.relkind = 'r' AND pg_class.relname = referencing_table
       AND ( pg_namespace.nspname = 'public' OR companies.id IS NOT NULL )
       AND ( specific_schema_name IS NULL OR pg_namespace.nspname = specific_schema_name )
       AND ( specific_company_id IS NULL OR companies.id = specific_company_id )
  LOOP
    -- RAISE DEBUG 'table_to_update = %', table_to_update;
    query := format('UPDATE %s SET %s WHERE %s',
      table_to_update,
      array_to_string((SELECT array_agg(format('%I = %s', columns, COALESCE(sharding.get_column_default_value(referencing_table, columns, schema_to_update), 'NULL'))) FROM unnest(referencing_columns) columns), ', '),
      array_to_string((SELECT array_agg(format('%I = %L', filters.column_name, filters.column_value)) FROM (SELECT unnest(referencing_columns) AS column_name, unnest(referenced_values) AS column_value) filters), ' AND ')
    );

    IF trigger_condition_clause IS NOT NULL THEN
      query := query || ' AND ' || trigger_condition_clause;
    END IF;

    -- RAISE DEBUG 'query: %', query;
    EXECUTE query;

  END LOOP;

  -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_default() - RETURN OLD: %', OLD;
  RETURN OLD;
END;
$BODY$ LANGUAGE 'plpgsql';
