class RecoverDebugOnVirtualFk < ActiveRecord::Migration
  def up
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

          RAISE DEBUG 'checking %', referenced_table;
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
          query := format('DELETE FROM %s WHERE %s',
            table_to_delete,
            array_to_string((SELECT array_agg(format('%I = %L', filters.column_name, filters.column_value)) FROM (SELECT unnest(referencing_columns) AS column_name, unnest(referenced_values) AS column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          RAISE DEBUG 'query: %', query;
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
            RAISE DEBUG 'table_to_check = %', table_to_check;
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
          query := format('UPDATE %s SET %s WHERE %s',
            table_to_update,
            array_to_string((SELECT array_agg(format('%I = %s', columns, COALESCE(sharding.get_column_default_value(referencing_table, columns, schema_to_update), 'NULL'))) FROM unnest(referencing_columns) columns), ', '),
            array_to_string((SELECT array_agg(format('%I = %L', filters.column_name, filters.column_value)) FROM (SELECT unnest(referencing_columns) AS column_name, unnest(referenced_values) AS column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          RAISE DEBUG 'query: %', query;
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
          query := format('UPDATE %s SET %s WHERE %s',
            table_to_update,
            array_to_string((SELECT array_agg(format('%I = NULL', columns)) FROM unnest(referencing_columns) columns), ', '),
            array_to_string((SELECT array_agg(format('%I = %L', filters.column_name, filters.column_value)) FROM (SELECT unnest(referencing_columns) AS column_name, unnest(referenced_values) AS column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          RAISE DEBUG 'query: %', query;
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
          query := format('UPDATE %s SET %s WHERE %s',
            table_to_update,
            array_to_string((SELECT array_agg(format('%I = %L', filters.column_name, filters.column_value)) FROM (SELECT unnest(referencing_columns) AS column_name, unnest(new_values) AS column_value) filters), ', '),
            array_to_string((SELECT array_agg(format('%I = %L', filters.column_name, filters.column_value)) FROM (SELECT unnest(referencing_columns) AS column_name, unnest(referenced_values) AS column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          RAISE DEBUG 'query: %', query;
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
            RAISE DEBUG 'table_to_check = %', table_to_check;
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
          query := format('UPDATE %s SET %s WHERE %s',
            table_to_update,
            array_to_string((SELECT array_agg(format('%I = %s', columns, COALESCE(sharding.get_column_default_value(referencing_table, columns, schema_to_update), 'NULL'))) FROM unnest(referencing_columns) columns), ', '),
            array_to_string((SELECT array_agg(format('%I = %L', filters.column_name, filters.column_value)) FROM (SELECT unnest(referencing_columns) AS column_name, unnest(referenced_values) AS column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          RAISE DEBUG 'query: %', query;
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
          query := format('UPDATE %s SET %s WHERE %s',
            table_to_update,
            array_to_string((SELECT array_agg(format('%I = NULL', columns)) FROM unnest(referencing_columns) columns), ', '),
            array_to_string((SELECT array_agg(format('%I = %L', filters.column_name, filters.column_value)) FROM (SELECT unnest(referencing_columns) AS column_name, unnest(referenced_values) AS column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          RAISE DEBUG 'query: %', query;
          EXECUTE query;
        END LOOP;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_set_null() - RETURN NEW: %', NEW;
        RETURN NEW;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
  end

  def down
    puts "Not reverting we just added debug".green
  end
end
