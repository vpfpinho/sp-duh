class IgnoreCompanyTablesWhileDeletingOnVfk < ActiveRecord::Migration
  def up
    # db/functions/sharding/trf_virtual_fk_reference_delete_restrict.sql
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
        deleted_schema TEXT;
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
        deleted_schema := '';
        IF (SELECT EXISTS (SELECT 1 FROM regexp_matches(referencing_table, '^.+\..+$'))) THEN
          SELECT (regexp_matches(referencing_table, '^(.+?)\..+?'))[1] INTO specific_schema_name;
          SELECT regexp_replace(referencing_table, specific_schema_name || '.', '') INTO referencing_table;
        ELSIF ( sharding.get_auxiliary_table_information()->'unsharded_tables' ? referencing_table ) THEN
          specific_schema_name := 'public';
        ELSIF TG_TABLE_NAME = 'companies' THEN
          specific_company_id := OLD.id;
          IF TG_OP = 'DELETE' THEN
            deleted_schema := OLD.schema_name;
          END IF;
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
            IF TG_TABLE_NAME = 'companies' AND TG_OP = 'DELETE' AND regexp_replace(table_to_check, '.'||referencing_table, '') = deleted_schema AND NOT common.schema_exists(deleted_schema) THEN
              RAISE DEBUG 'Ignoring table % (schema no longer exists)', table_to_check;
            ELSIF sharding.check_record_existence(table_to_check, trigger_condition) THEN
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
  end

  def down
    # db/functions/sharding/trf_virtual_fk_reference_delete_restrict.sql
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
  end
end

