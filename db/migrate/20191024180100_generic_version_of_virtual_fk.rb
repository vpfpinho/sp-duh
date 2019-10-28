class GenericVersionOfVirtualFk < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.get_virtual_fk_referencing_tables (
          IN  referenced_schema    text,
          IN  referencing_table    text,
          IN  specific_company_id  integer DEFAULT NULL,
          IN  specific_schema_name text    DEFAULT NULL,
          OUT referencing_schema   text
      ) RETURNS SETOF text AS $BODY$
      BEGIN

          IF specific_schema_name IS NOT NULL THEN

              RETURN QUERY
              SELECT specific_schema_name;

          ELSIF specific_company_id IS NOT NULL THEN

              RETURN QUERY
              SELECT pg_namespace.nspname::text
                FROM pg_catalog.pg_namespace
                JOIN public.companies    ON pg_namespace.nspname = companies.schema_name
                JOIN pg_catalog.pg_class ON pg_class.relnamespace = pg_namespace.oid
               WHERE companies.id = specific_company_id
                AND pg_class.relname = referencing_table;

          ELSE

          RETURN QUERY
          SELECT pg_namespace.nspname::text
            FROM pg_catalog.pg_class
            JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
            LEFT JOIN public.companies   ON companies.schema_name = pg_namespace.nspname
           WHERE pg_class.relkind = 'r' AND pg_class.relname = referencing_table
             AND ( companies.id IS NOT NULL OR pg_namespace.nspname IN ('accounting','fixedassets','payroll','purchases','public') )
             ;

          END IF;

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.check_record_existence(
        IN p_table_name TEXT,
        IN p_columns_and_values JSONB
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        query TEXT;
        record_exists BOOLEAN;
        clauses TEXT;
        clause_fields TEXT[];

        clause_format_expression TEXT;
        clause_columns_extract_expression TEXT;
        clause_columns_definition_expression TEXT;
      BEGIN
        record_exists := FALSE;

        IF p_table_name IS NULL OR p_columns_and_values IS NULL THEN
          RAISE EXCEPTION 'Invalid arguments calling: %', format('sharding.check_record_existence(%L,%L)', p_table_name, p_columns_and_values);
        END IF;

        clause_fields := (SELECT array_agg(jsonb_object_keys) FROM jsonb_object_keys(p_columns_and_values));

        SELECT
          array_to_string(array_agg(format('%1$s TEXT', unnest)), ', ')
        INTO
          clause_columns_definition_expression
        FROM unnest(clause_fields);

        SELECT
          format('''(%1$s)'', %2$s', array_to_string(array_agg('%' || i || '$L'), ', '), array_to_string(array_agg(field), ', ')),
          array_to_string(
            array_agg(format(
              '(SELECT jsonb_array_elements_text(%1$s::JSONB) AS %1$s FROM jsonb_to_record($1) AS data(%2$s)) data_%1$s',
              field,
              clause_columns_definition_expression
            )),
            E'\nCROSS JOIN '
          )
        INTO
          clause_format_expression,
          clause_columns_extract_expression
        FROM unnest(clause_fields) WITH ORDINALITY AS fields(field, i);

        EXECUTE format($$SELECT
          array_to_string((
            SELECT array_agg(format(%1$s))
            FROM %2$s
          ),
          ', ')
        $$,
          clause_format_expression,
          clause_columns_extract_expression,
          clause_columns_definition_expression,
          p_columns_and_values
        ) INTO clauses USING p_columns_and_values;

        query := format('SELECT EXISTS (SELECT 1 FROM %1$s WHERE (%2$s) IN (%3$s))', p_table_name, array_to_string(clause_fields, ', '), clauses);
        -- RAISE DEBUG 'query: %', query;
        EXECUTE query INTO record_exists;

        RETURN record_exists;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_before_insert_or_update()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        referencing_columns TEXT[];
        referencing_values TEXT[];
        referenced_tables TEXT[];
        referenced_table TEXT;
        referenced_columns TEXT[];
        record_existence_check_data JSONB;
      BEGIN
        -- RAISE DEBUG 'sharding.trf_virtual_fk_before_insert_or_update() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_before_insert_or_update() -        NEW: %', NEW;

        referencing_columns := TG_ARGV[0];
        referenced_tables := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];

        -- Extract the values from the NEW record into the referencing_values variable
        EXECUTE format(
          format('SELECT ARRAY[%s]::TEXT[]',
            (SELECT array_to_string((SELECT array_agg('($1).%I'::text) FROM (SELECT generate_series(1::integer, array_length(referencing_columns, 1)::integer)) bogus), ', '))
          ),
          VARIADIC referencing_columns
        ) USING NEW INTO referencing_values;

        FOR referenced_table IN
          SELECT * FROM unnest(referenced_tables)
        LOOP
          record_existence_check_data := (
            SELECT format('{ %s }',
              array_to_string((
                SELECT array_agg(format('"%1$s": ["%2$s"]', field, val))
                FROM (
                  SELECT * FROM unnest(referenced_columns, referencing_values)
                ) AS data(field, val)
              ), ', '))
          );

          -- RAISE DEBUG 'checking %', referenced_table;
          IF sharding.check_record_existence(referenced_table, record_existence_check_data) THEN
            -- If supplying more than one referenced table, the first one where the values are found validates the 'foreign key'
            -- RAISE INFO 'key (%)=(%) exists on table %(%)', array_to_string(referencing_columns, ', '), array_to_string(referencing_values, ', '), referenced_table, array_to_string(referenced_columns, ', ');
            -- RAISE DEBUG 'sharding.trf_virtual_fk_before_insert_or_update() - RETURN NEW: %', NEW;
            RETURN NEW;
          END IF;
        END LOOP;

        -- If we reach this point, the value was not found on any referenced table
        RAISE foreign_key_violation USING 
          MESSAGE = format('insert or update on table %I.%I violates "%s"', TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NAME),
          DETAIL = format('key (%s)=(%s) is not present on %s (%s)', array_to_string(referencing_columns, ', '), array_to_string(referencing_values, ', '), referenced_tables, array_to_string(referenced_columns, ', '));

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_cascade()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        specific_company_id integer;
        specific_schema_name TEXT;
        table_to_delete TEXT;
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

        FOR table_to_delete IN
          SELECT format('%I.%I', referencing_schema, referencing_table)
            FROM sharding.get_virtual_fk_referencing_tables(TG_TABLE_SCHEMA, referencing_table, specific_company_id, specific_schema_name)
        LOOP
          -- RAISE DEBUG 'table_to_delete = %', table_to_delete;
          query := format('DELETE FROM %s WHERE %s',
            table_to_delete,
            array_to_string((SELECT array_agg(format('%I = %L', filters.column_name, filters.column_value)) FROM (SELECT unnest(referencing_columns) AS column_name, unnest(referenced_values) AS column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          -- RAISE DEBUG 'query: %', query;
          EXECUTE query;
        END LOOP; 

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_cascade() - RETURN OLD: %', OLD;
        RETURN OLD;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_restrict()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        specific_company_id integer;
        specific_schema_name TEXT;
        table_to_check TEXT;
        referencing_columns TEXT[];
        referencing_table TEXT;
        referenced_columns TEXT[];
        referenced_values TEXT[];
        trigger_condition JSONB;
        culprit_tables TEXT[];
      BEGIN
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_restrict() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_restrict() -        OLD: %', OLD;

        referencing_columns := TG_ARGV[0];
        referencing_table := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];
        trigger_condition := TG_ARGV[3];

        -- Extract the values from the OLD record into the referenced_values variable
        EXECUTE format(
          format('SELECT ARRAY[%s]::TEXT[]',
            (SELECT array_to_string((SELECT array_agg('($1).%I'::TEXT) FROM (SELECT generate_series(1::integer, array_length(referenced_columns, 1)::integer)) bogus), ', '))
          ),
          VARIADIC referenced_columns
        ) USING OLD INTO referenced_values;
        trigger_condition := sharding.merge_jsonb_with_arrays_of_keys_and_values(trigger_condition, referencing_columns, referenced_values);

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

        culprit_tables := '{}';
        FOR table_to_check IN
          SELECT format('%I.%I', referencing_schema, referencing_table)
            FROM sharding.get_virtual_fk_referencing_tables(TG_TABLE_SCHEMA, referencing_table, specific_company_id, specific_schema_name)
        LOOP
            -- RAISE DEBUG 'table_to_check = %', table_to_check;
            IF sharding.check_record_existence(table_to_check, trigger_condition) THEN
              -- the first value found invalidates the operation
              RAISE foreign_key_violation USING
                MESSAGE = format('delete on table %I.%I violates "%s"', TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NAME),
                DETAIL = format('tuple (%s) is still referenced from table %s with condition: %s', array_to_string(referenced_values, ', '), table_to_check, trigger_condition);
              -- we may comment raise above and check all schemas with references
              IF NOT table_to_check = ANY (culprit_tables) THEN
                culprit_tables := array_append(culprit_tables, table_to_check);
              END IF;
            END IF;
        END LOOP;

        IF cardinality(culprit_tables) > 0 THEN
          RAISE foreign_key_violation USING 
            MESSAGE = format('delete on table %I.%I violates "%s"', TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NAME),
            DETAIL = format('tuple (%s) is referenced in %s table(s): %s', array_to_string(referenced_values, ', '), cardinality(culprit_tables), array_to_string(culprit_tables, ', '));
        END IF;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_restrict() - RETURN OLD: %', OLD;
        RETURN OLD;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_set_default()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        specific_company_id integer;
        specific_schema_name TEXT;
        schema_to_update TEXT;
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

        FOR table_to_update, schema_to_update IN
          SELECT format('%I.%I', referencing_schema, referencing_table), referencing_schema
            FROM sharding.get_virtual_fk_referencing_tables(TG_TABLE_SCHEMA, referencing_table, specific_company_id, specific_schema_name)
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
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_set_null()
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
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_null() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_null() -        OLD: %', OLD;

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
          SELECT format('%I.%I', referencing_schema, referencing_table)
            FROM sharding.get_virtual_fk_referencing_tables(TG_TABLE_SCHEMA, referencing_table, specific_company_id, specific_schema_name)
        LOOP
          -- RAISE DEBUG 'table_to_update = %', table_to_update;
          query := format('UPDATE %s SET %s WHERE %s',
            table_to_update,
            array_to_string((SELECT array_agg(format('%I = NULL', columns)) FROM unnest(referencing_columns) columns), ', '),
            array_to_string((SELECT array_agg(format('%I = %L', filters.column_name, filters.column_value)) FROM (SELECT unnest(referencing_columns) AS column_name, unnest(referenced_values) AS column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          -- RAISE DEBUG 'query: %', query;
          EXECUTE query;
        END LOOP;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_null() - RETURN OLD: %', OLD;
        RETURN OLD;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_update_cascade()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        specific_company_id integer;
        specific_schema_name TEXT;
        table_to_update TEXT;
        referencing_columns TEXT[];
        referencing_table TEXT;
        referenced_columns TEXT[];
        referenced_values TEXT[];
        new_values TEXT[];
        trigger_condition_clause TEXT;
        query TEXT;
      BEGIN
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() -        OLD: %', OLD;

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

        -- Extract the values from the NEW record into the new_values variable
        EXECUTE format(
          format('SELECT ARRAY[%s]::TEXT[]',
            (SELECT array_to_string((SELECT array_agg('($1).%I'::TEXT) FROM (SELECT generate_series(1::integer, array_length(referenced_columns, 1)::integer)) bogus), ', '))
          ),
          VARIADIC referenced_columns
        ) USING NEW INTO new_values;

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
          SELECT format('%I.%I', referencing_schema, referencing_table)
            FROM sharding.get_virtual_fk_referencing_tables(TG_TABLE_SCHEMA, referencing_table, specific_company_id, specific_schema_name)
        LOOP
          -- RAISE DEBUG 'table_to_update = %', table_to_update;
          query := format('UPDATE %s SET %s WHERE %s',
            table_to_update,
            array_to_string((SELECT array_agg(format('%I = %L', filters.column_name, filters.column_value)) FROM (SELECT unnest(referencing_columns) AS column_name, unnest(new_values) AS column_value) filters), ', '),
            array_to_string((SELECT array_agg(format('%I = %L', filters.column_name, filters.column_value)) FROM (SELECT unnest(referencing_columns) AS column_name, unnest(referenced_values) AS column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          -- RAISE DEBUG 'query: %', query;
          EXECUTE query;
        END LOOP;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() - RETURN NEW: %', NEW;
        RETURN NEW;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_update_restrict()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        specific_company_id integer;
        specific_schema_name TEXT;
        table_to_check TEXT;
        referencing_columns TEXT[];
        referencing_table TEXT;
        referenced_columns TEXT[];
        referenced_values TEXT[];
        trigger_condition JSONB;
        culprit_tables TEXT[];
      BEGIN
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_restrict() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_restrict() -        OLD: %', OLD;

        referencing_columns := TG_ARGV[0];
        referencing_table := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];
        trigger_condition := TG_ARGV[3];

        -- Extract the values from the OLD record into the referenced_values variable
        EXECUTE format(
          format('SELECT ARRAY[%s]::TEXT[]',
            (SELECT array_to_string((SELECT array_agg('($1).%I'::TEXT) FROM (SELECT generate_series(1::integer, array_length(referenced_columns, 1)::integer)) bogus), ', '))
          ),
          VARIADIC referenced_columns
        ) USING OLD INTO referenced_values;
        trigger_condition := sharding.merge_jsonb_with_arrays_of_keys_and_values(trigger_condition, referencing_columns, referenced_values);

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

        culprit_tables := '{}';
        FOR table_to_check IN
          SELECT format('%I.%I', referencing_schema, referencing_table)
            FROM sharding.get_virtual_fk_referencing_tables(TG_TABLE_SCHEMA, referencing_table, specific_company_id, specific_schema_name)
        LOOP
            -- RAISE DEBUG 'table_to_check = %', table_to_check;
            IF sharding.check_record_existence(table_to_check, trigger_condition) THEN
              -- the first value found invalidates the operation
              RAISE foreign_key_violation USING
                MESSAGE = format('update on table %I.%I violates "%s"', TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NAME),
                DETAIL = format('tuple (%s) is still referenced from table %s with condition: %s', array_to_string(referenced_values, ', '), table_to_check, trigger_condition);
              -- we may comment raise above and check all schemas with references
              IF NOT table_to_check = ANY (culprit_tables) THEN
                culprit_tables := array_append(culprit_tables, table_to_check);
              END IF;
            END IF;
        END LOOP;

        IF cardinality(culprit_tables) > 0 THEN
          RAISE foreign_key_violation USING 
            MESSAGE = format('update on table %I.%I violates "%s"', TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NAME),
            DETAIL = format('tuple (%s) is referenced in %s table(s): %s', array_to_string(referenced_values, ', '), cardinality(culprit_tables), array_to_string(culprit_tables, ', '));
        END IF;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_restrict() - RETURN NEW: %', NEW;
        RETURN NEW;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_update_set_default()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        specific_company_id integer;
        specific_schema_name TEXT;
        schema_to_update TEXT;
        table_to_update TEXT;
        referencing_columns TEXT[];
        referencing_table TEXT;
        referenced_columns TEXT[];
        referenced_values TEXT[];
        trigger_condition_clause TEXT;
        query TEXT;
      BEGIN
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_set_default() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_set_default() -        OLD: %', OLD;

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

        FOR table_to_update, schema_to_update IN
          SELECT format('%I.%I', referencing_schema, referencing_table), referencing_schema
            FROM sharding.get_virtual_fk_referencing_tables(TG_TABLE_SCHEMA, referencing_table, specific_company_id, specific_schema_name)
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

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_set_default() - RETURN NEW: %', NEW;
        RETURN NEW;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_update_set_null()
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
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_set_null() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_set_null() -        OLD: %', OLD;

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
          SELECT format('%I.%I', referencing_schema, referencing_table)
            FROM sharding.get_virtual_fk_referencing_tables(TG_TABLE_SCHEMA, referencing_table, specific_company_id, specific_schema_name)
        LOOP
          -- RAISE DEBUG 'table_to_update = %', table_to_update;
          query := format('UPDATE %s SET %s WHERE %s',
            table_to_update,
            array_to_string((SELECT array_agg(format('%I = NULL', columns)) FROM unnest(referencing_columns) columns), ', '),
            array_to_string((SELECT array_agg(format('%I = %L', filters.column_name, filters.column_value)) FROM (SELECT unnest(referencing_columns) AS column_name, unnest(referenced_values) AS column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          -- RAISE DEBUG 'query: %', query;
          EXECUTE query;
        END LOOP;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_set_null() - RETURN NEW: %', NEW;
        RETURN NEW;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
  end

  def down
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS sharding.get_virtual_fk_referencing_tables(text, text, integer, text);
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.check_record_existence(
        IN p_table_name TEXT,
        IN p_columns_and_values JSONB
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        record_exists BOOLEAN;
        clauses TEXT;
        clause_fields TEXT[];

        clause_format_expression TEXT;
        clause_columns_extract_expression TEXT;
        clause_columns_definition_expression TEXT;
      BEGIN
        record_exists := FALSE;

        -- raise notice 'sharding.check_record_existence(''%'', ''%'');', p_table_name, p_columns_and_values;

        clause_fields := (SELECT array_agg(jsonb_object_keys) FROM jsonb_object_keys(p_columns_and_values));

        SELECT
          array_to_string(array_agg(format('%1$s TEXT', unnest)), ', ')
        INTO
          clause_columns_definition_expression
        FROM unnest(clause_fields);

        SELECT
          format('''(%1$s)'', %2$s', array_to_string(array_agg('%' || i || '$L'), ', '), array_to_string(array_agg(field), ', ')),
          array_to_string(
            array_agg(format(
              '(SELECT jsonb_array_elements_text(%1$s::JSONB) AS %1$s FROM jsonb_to_record($1) AS data(%2$s)) data_%1$s',
              field,
              clause_columns_definition_expression
            )),
            E'\nCROSS JOIN '
          )
        INTO
          clause_format_expression,
          clause_columns_extract_expression
        FROM unnest(clause_fields) WITH ORDINALITY AS fields(field, i);

        EXECUTE format($$SELECT
          array_to_string((
            SELECT array_agg(format(%1$s))
            FROM %2$s
          ),
          ', ')
        $$,
          clause_format_expression,
          clause_columns_extract_expression,
          clause_columns_definition_expression,
          p_columns_and_values
        ) INTO clauses USING p_columns_and_values;

        EXECUTE format('SELECT EXISTS (SELECT 1 FROM %1$s WHERE (%2$s) IN (%3$s))', p_table_name, array_to_string(clause_fields, ', '), clauses) INTO record_exists;

        RETURN record_exists;

      EXCEPTION
        WHEN OTHERS THEN
          RETURN false;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_before_insert_or_update()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        referencing_columns TEXT[];
        referencing_values TEXT[];
        referenced_tables TEXT[];
        referenced_table TEXT;
        referenced_columns TEXT[];
        record_existence_check_data JSONB;
      BEGIN
        referencing_columns := TG_ARGV[0];
        referenced_tables := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];
        -- RAISE DEBUG 'sharding.trf_virtual_fk_before_insert_or_update() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_before_insert_or_update() -        NEW: %', NEW;

        -- Extract the values from the NEW record into the referencing_values variable
        EXECUTE format(
          format('SELECT ARRAY[%s]::TEXT[]',
            (SELECT array_to_string((SELECT array_agg('($1).%I'::text) FROM (SELECT generate_series(1::integer, array_length(referencing_columns, 1)::integer)) bogus), ', '))
          ),
          VARIADIC referencing_columns
        ) USING NEW INTO referencing_values;

        FOR referenced_table IN SELECT * FROM unnest(referenced_tables) LOOP
          record_existence_check_data := (
            SELECT format('{ %s }',
              array_to_string((
                SELECT array_agg(format('"%1$s": ["%2$s"]', field, val))
                FROM (
                  SELECT * FROM unnest(referenced_columns, referencing_values)
                ) AS data(field, val)
              ), ', '))
          );

          -- Check for the existence of a record on the referenced_table with the referencing_values in the referenced_columns
          IF sharding.check_record_existence(referenced_table, record_existence_check_data) THEN
            -- If supplying more than one referenced table, the first one where the values are found validates the 'foreign key'
            -- RAISE NOTICE 'Tuple (%) exists on table %(%)', array_to_string(referencing_values, ', '), referenced_table, array_to_string(referenced_columns, ', ');
            -- RAISE DEBUG 'sharding.trf_virtual_fk_before_insert_or_update() - RETURN NEW: %', NEW;
            RETURN NEW;
          ELSE
          END IF;
        END LOOP;

        -- If we reach this point, the value was not found on any referenced table
        RAISE foreign_key_violation
          USING MESSAGE = format('Tuple (%1$s) was not found on %2$s(%3$s)', array_to_string(referencing_values, ', '), referenced_table, array_to_string(referenced_columns, ', ')),
                TABLE = referenced_table,
                COLUMN = referenced_columns
        ;

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
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
    SQL

    execute <<-'SQL'
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
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_set_default()
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
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_default() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_default() -        OLD: %', OLD;

        referencing_columns := TG_ARGV[0];
        referencing_table := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];
        trigger_condition_clause := TG_ARGV[3];

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
        ELSE
          BEGIN
            company_schema_name := sharding.get_schema_name_for_table(OLD.company_id, referencing_table);
          EXCEPTION
            WHEN OTHERS THEN
              company_schema_name := NULL;
          END;
        END IF;

        IF company_schema_name IS NOT NULL THEN
          -- This table has a company_id column, update just the associated schema
          query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
            company_schema_name,
            referencing_table,
            array_to_string((select array_agg(format('%1$I = %2$s', columns, COALESCE(sharding.get_column_default_value(referencing_table, columns, company_schema_name), 'NULL'))) FROM unnest(referencing_columns) columns), ', '),
            array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          -- RAISE DEBUG 'query: %', query;
          EXECUTE query;
        ELSE
          -- The table does not have a company_id column, update all cluster schemas
          SHOW cloudware.cluster INTO _current_cluster;
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company AND NOT is_deleted AND cluster = _current_cluster
          LOOP
            query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
              company_schema_name,
              referencing_table,
              array_to_string((select array_agg(format('%1$I = %2$s', columns, COALESCE(sharding.get_column_default_value(referencing_table, columns, company_schema_name), 'NULL'))) FROM unnest(referencing_columns) columns), ', '),
              array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
            );

            IF trigger_condition_clause IS NOT NULL THEN
              query := query || ' AND ' || trigger_condition_clause;
            END IF;

            -- RAISE DEBUG 'query: %', query;
            EXECUTE query;

          END LOOP;
        END IF;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_default() - RETURN OLD: %', OLD;
        RETURN OLD;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_set_null()
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
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_null() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_null() -        OLD: %', OLD;

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
        ELSE
          BEGIN
            company_schema_name := sharding.get_schema_name_for_table(OLD.company_id, referencing_table);
          EXCEPTION
            WHEN OTHERS THEN
              company_schema_name := NULL;
          END;
          IF TG_TABLE_NAME = 'users' THEN
            -- company_id IS NOT NULL (checked above) and company does not exist on public.companies
            IF company_schema_name IS NULL THEN
              RETURN OLD;
            END IF;
          END IF;
        END IF;

        IF company_schema_name IS NOT NULL THEN
          IF NOT common.schema_exists(company_schema_name) THEN
            company_schema_name := NULL;
          END IF;
          IF company_schema_name IS NOT NULL THEN
            -- This table has a company_id column, update just the associated schema
            query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
              company_schema_name,
              referencing_table,
              array_to_string((select array_agg(format('%1$I = NULL', columns)) FROM unnest(referencing_columns) columns), ', '),
              array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
            );

            IF trigger_condition_clause IS NOT NULL THEN
              query := query || ' AND ' || trigger_condition_clause;
            END IF;

            -- RAISE DEBUG 'query: %', query;
            EXECUTE query;
          END IF;
        ELSE
          -- The table does not have a company_id column, update all cluster schemas
          SHOW cloudware.cluster INTO _current_cluster;
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company AND NOT is_deleted AND cluster = _current_cluster
          LOOP
            query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
              company_schema_name,
              referencing_table,
              array_to_string((select array_agg(format('%1$I = NULL', columns)) FROM unnest(referencing_columns) columns), ', '),
              array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
            );

            IF trigger_condition_clause IS NOT NULL THEN
              query := query || ' AND ' || trigger_condition_clause;
            END IF;

            -- RAISE DEBUG 'query: %', query;
            EXECUTE query;
          END LOOP;
        END IF;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_null() - RETURN OLD: %', OLD;
        RETURN OLD;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_update_cascade()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        _current_cluster integer;
        company_schema_name TEXT;
        referencing_columns TEXT[];
        referencing_table TEXT;
        referenced_columns TEXT[];
        referenced_values TEXT[];
        new_values TEXT[];
        trigger_condition_clause TEXT;
        query TEXT;
      BEGIN
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() -        OLD: %', OLD;

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

        -- Extract the values from the NEW record into the new_values variable
        EXECUTE format(
          format('SELECT ARRAY[%s]::TEXT[]',
            (SELECT array_to_string((SELECT array_agg('($1).%I'::TEXT) FROM (SELECT generate_series(1::integer, array_length(referenced_columns, 1)::integer)) bogus), ', '))
          ),
          VARIADIC referenced_columns
        ) USING NEW INTO new_values;

        -- Try to get the company schema from th referencing table (in case it's supplied as <schema>.<table>)
        IF (SELECT EXISTS (SELECT 1 FROM regexp_matches(referencing_table, '^.+\..+$'))) THEN
          SELECT (regexp_matches(referencing_table, '^(.+?)\..+?'))[1] INTO company_schema_name;
          SELECT regexp_replace(referencing_table, company_schema_name || '.', '') INTO referencing_table;
        ELSE
          BEGIN
            company_schema_name := sharding.get_schema_name_for_table(OLD.company_id, referencing_table);
          EXCEPTION
            WHEN OTHERS THEN
              company_schema_name := NULL;
          END;
        END IF;

        IF company_schema_name IS NOT NULL THEN
          -- This table has a company_id column, update just the associated schema
          query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
            company_schema_name,
            referencing_table,
            array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(new_values) as column_value) filters), ', '),
            array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          -- RAISE DEBUG 'query: %', query;
          EXECUTE query;
        ELSE
          -- The table does not have a company_id column, update all cluster schemas
          SHOW cloudware.cluster INTO _current_cluster;
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company AND NOT is_deleted AND cluster = _current_cluster
          LOOP
            query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
              company_schema_name,
              referencing_table,
              array_to_string((select array_agg(format('%1$I = %2$s', columns, COALESCE(sharding.get_column_default_value(referencing_table, columns, company_schema_name), 'NULL'))) FROM unnest(referencing_columns) columns), ', '),
              array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
            );

              IF trigger_condition_clause IS NOT NULL THEN
                query := query || ' AND ' || trigger_condition_clause;
              END IF;

              -- RAISE DEBUG 'query: %', query;
              EXECUTE query;
          END LOOP;
        END IF;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() - RETURN NEW: %', NEW;
        RETURN NEW;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_update_restrict()
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
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_restrict() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() -        OLD: %', OLD;

        referencing_columns := TG_ARGV[0];
        referencing_table := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];
        trigger_condition := TG_ARGV[3];

        -- Extract the values from the NEW record into the referenced_values variable
        EXECUTE format(
          format('SELECT ARRAY[%s]::TEXT[]',
            (SELECT array_to_string((SELECT array_agg('($1).%I'::TEXT) FROM (SELECT generate_series(1::integer, array_length(referenced_columns, 1)::integer)) bogus), ', '))
          ),
          VARIADIC referenced_columns
        ) USING OLD INTO referenced_values;

        trigger_condition := sharding.merge_jsonb_with_arrays_of_keys_and_values(trigger_condition, referencing_columns, referenced_values);

        -- Try to get the company schema from th referencing table (in case it's supplied as <schema>.<table>)
        IF (SELECT EXISTS (SELECT 1 FROM regexp_matches(referencing_table, '^.+\..+$'))) THEN
          SELECT (regexp_matches(referencing_table, '^(.+?)\..+?'))[1] INTO company_schema_name;
          SELECT regexp_replace(referencing_table, company_schema_name || '.', '') INTO referencing_table;
        ELSE
          BEGIN
            company_schema_name := sharding.get_schema_name_for_table(OLD.company_id, referencing_table);
          EXCEPTION
            WHEN OTHERS THEN
              company_schema_name := NULL;
          END;
        END IF;

        IF company_schema_name IS NOT NULL THEN
          -- This table has a company_id column, check just the associated schema
            IF sharding.check_record_existence(format('%1$I.%2$I', company_schema_name, referencing_table), trigger_condition) THEN
              culprit_schemas := culprit_schemas || company_schema_name;
            END IF;
        ELSE
          -- The table does not have a company_id column, check all cluster schemas
          SHOW cloudware.cluster INTO _current_cluster;
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company AND NOT is_deleted AND cluster = _current_cluster
          LOOP
            IF sharding.check_record_existence(format('%1$I.%2$I', company_schema_name, referencing_table), trigger_condition) THEN
              culprit_schemas := culprit_schemas || company_schema_name;
            END IF;
          END LOOP;
        END IF;

        IF array_length(culprit_schemas, 1) > 0 THEN
          RAISE foreign_key_violation
            USING MESSAGE = format('Can''t update record. Tuple (%1$s) exists in %2$s schema(s): %3$s', array_to_string(referenced_values, ', '), array_length(culprit_schemas, 1), array_to_string(culprit_schemas, ', ')),
                  SCHEMA = TG_TABLE_SCHEMA,
                  TABLE = TG_TABLE_NAME
          ;
        END IF;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() - RETURN NEW: %', NEW;
        RETURN NEW;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_update_set_default()
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
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_set_default() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() -        OLD: %', OLD;

        referencing_columns := TG_ARGV[0];
        referencing_table := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];
        trigger_condition_clause := TG_ARGV[3];


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
        ELSE
          BEGIN
            company_schema_name := sharding.get_schema_name_for_table(OLD.company_id, referencing_table);
          EXCEPTION
            WHEN OTHERS THEN
              company_schema_name := NULL;
          END;
        END IF;

        IF company_schema_name IS NOT NULL THEN
          -- This table has a company_id column, update just the associated schema
          query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
            company_schema_name,
            referencing_table,
            array_to_string((select array_agg(format('%1$I = %2$s', columns, COALESCE(sharding.get_column_default_value(referencing_table, columns, company_schema_name), 'NULL'))) FROM unnest(referencing_columns) columns), ', '),
            array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          -- RAISE DEBUG 'query: %', query;
          EXECUTE query;
        ELSE
          -- The table does not have a company_id column, update all cluster schemas
          SHOW cloudware.cluster INTO _current_cluster;
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company AND NOT is_deleted AND cluster = _current_cluster
          LOOP
            query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
              company_schema_name,
              referencing_table,
              array_to_string((select array_agg(format('%1$I = %2$s', columns, COALESCE(sharding.get_column_default_value(referencing_table, columns, company_schema_name), 'NULL'))) FROM unnest(referencing_columns) columns), ', '),
              array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
            );

            IF trigger_condition_clause IS NOT NULL THEN
              query := query || ' AND ' || trigger_condition_clause;
            END IF;

            -- RAISE DEBUG 'query: %', query;
            EXECUTE query;
          END LOOP;
        END IF;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() - RETURN NEW: %', NEW;
        RETURN NEW;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_update_set_null()
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
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_set_null() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() -        OLD: %', OLD;

        referencing_columns := TG_ARGV[0];
        referencing_table := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];
        trigger_condition_clause := TG_ARGV[3];

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
        ELSE
          BEGIN
            company_schema_name := sharding.get_schema_name_for_table(OLD.company_id, referencing_table);
          EXCEPTION
            WHEN OTHERS THEN
              company_schema_name := NULL;
          END;
        END IF;

        IF company_schema_name IS NOT NULL THEN
          -- This table has a company_id column, update just the associated schema
          query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
            company_schema_name,
            referencing_table,
            array_to_string((select array_agg(format('%1$I = NULL', columns)) FROM unnest(referencing_columns) columns), ', '),
            array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          -- RAISE DEBUG 'query: %', query;
          EXECUTE query;
        ELSE
          -- The table does not have a company_id column, update all cluster schemas
          SHOW cloudware.cluster INTO _current_cluster;
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company AND NOT is_deleted AND cluster = _current_cluster
          LOOP
            query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
              company_schema_name,
              referencing_table,
              array_to_string((select array_agg(format('%1$I = NULL', columns)) FROM unnest(referencing_columns) columns), ', '),
              array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
            );

            IF trigger_condition_clause IS NOT NULL THEN
              query := query || ' AND ' || trigger_condition_clause;
            END IF;

            -- RAISE DEBUG 'query: %', query;
            EXECUTE query;
          END LOOP;
        END IF;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() - RETURN NEW: %', NEW;
        RETURN NEW;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
  end
end
