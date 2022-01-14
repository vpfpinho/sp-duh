class CommercialImprovementsSpduh < ActiveRecord::Migration
  def up
    execute %Q[ DELETE FROM public.schema_migrations WHERE version = '20220114115941'; ] # Bumped
    # SQLFILE: sp-duh/db/functions/sharding/trf_virtual_fk_before_insert_or_update.sql
    execute <<-'SQL'
      -- DROP FUNCTION IF EXISTS sharding.trf_virtual_fk_before_insert_or_update();

      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_before_insert_or_update()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        referencing_columns TEXT[];
        referencing_values TEXT[];
        referenced_tables TEXT[];
        referenced_table TEXT;
        referenced_columns TEXT[];
        record_existence_check_data JSONB;
        _stack                      text;
      BEGIN
        -- RAISE DEBUG 'sharding.trf_virtual_fk_before_insert_or_update() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_before_insert_or_update() -        NEW: %', NEW;

        GET DIAGNOSTICS _stack = PG_CONTEXT;
        -- Blind faith in duplicate sales documents
        IF _stack ~ 'commercial.(duplicate|copy)_sales_documents' THEN
          RETURN NEW;
        END IF;


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

  end

  def down
    puts 'No down.'
  end
end
