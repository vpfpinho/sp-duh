class ExcludeUsersWithoutCompanyIdFromVirtualForeignKeyReferenceTriggerFunction < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_set_null()
      RETURNS TRIGGER AS $BODY$
      DECLARE
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
          -- The table doesn't have a company_id column, update all schemas
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company
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
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_cascade()
      RETURNS TRIGGER AS $BODY$
      DECLARE
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
        ELSE
          BEGIN
            company_schema_name := sharding.get_schema_name_for_table(OLD.company_id, referencing_table);
          EXCEPTION
            WHEN OTHERS THEN
              company_schema_name := NULL;
          END;
        END IF;

        IF company_schema_name IS NOT NULL THEN
          -- This table has a company_id column, delete just from the associated schema
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
        ELSE
          -- The table doesn't have a company_id column, update all schemas
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company
          LOOP
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
        company_schema_name TEXT;
        culprit_schemas TEXT[];
        referencing_columns TEXT[];
        referencing_table TEXT;
        referenced_columns TEXT[];
        referenced_values TEXT[];
        trigger_condition JSONB;
      BEGIN
        -- RAISE NOTICE 'sharding.trf_virtual_fk_reference_delete_restrict() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE NOTICE 'sharding.trf_virtual_fk_reference_delete_restrict() -        OLD: %', OLD;

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
          -- The table doesn't have a company_id column, check all company schemas
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company
          LOOP
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
  end

  def down
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_set_null()
      RETURNS TRIGGER AS $BODY$
      DECLARE
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
          -- The table doesn't have a company_id column, update all schemas
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company
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
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_cascade()
      RETURNS TRIGGER AS $BODY$
      DECLARE
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
          -- This table has a company_id column, delete just from the associated schema
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
        ELSE
          -- The table doesn't have a company_id column, update all schemas
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company
          LOOP
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
        company_schema_name TEXT;
        culprit_schemas TEXT[];
        referencing_columns TEXT[];
        referencing_table TEXT;
        referenced_columns TEXT[];
        referenced_values TEXT[];
        trigger_condition JSONB;
      BEGIN
        -- RAISE NOTICE 'sharding.trf_virtual_fk_reference_delete_restrict() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE NOTICE 'sharding.trf_virtual_fk_reference_delete_restrict() -        OLD: %', OLD;

        referencing_columns := TG_ARGV[0];
        referencing_table := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];
        trigger_condition := TG_ARGV[3];

        IF trigger_condition IS NOT NULL THEN
          trigger_condition := sharding.merge_jsonb_with_arrays_of_keys_and_values(trigger_condition, referencing_columns, referenced_values);
        END IF;

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
          -- The table doesn't have a company_id column, check all company schemas
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company
          LOOP
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
  end
end
