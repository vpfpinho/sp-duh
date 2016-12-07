class PreventLockingPublicTablesWhenTryingToCreateExistingTriggersWhileSharding < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.get_create_virtual_polymorphic_foreign_key_queries(
        IN p_referencing_table TEXT,
        IN p_referenced_table TEXT,
        IN p_column_mappings JSONB, -- { "referencing_col_a": "referenced_col_a", "referencing_col_b": "referenced_col_b", "referencing_col_c": null }
        IN p_template_fk_name TEXT DEFAULT NULL,
        IN p_update_condition "char" DEFAULT NULL, -- DEFAULTS TO NO ACTION
        IN p_delete_condition "char" DEFAULT NULL, -- DEFAULTS TO NO ACTION
        IN p_trigger_conditions JSONB DEFAULT NULL -- { "local_col_c": [ "value_a", "value_b" ] }
      )
      RETURNS TEXT[] AS $BODY$
      DECLARE
        aux_array TEXT[];
        queries TEXT[];

        all_local_columns TEXT[];
        referencing_columns TEXT[];
        referenced_columns TEXT[];
        trigger_condition_clause TEXT;
      BEGIN
        -- RAISE NOTICE 'sharding.get_create_virtual_polymorphic_foreign_key_queries(''%'', ''%'', ''%'', ''%'', ''%'', ''%'', ''%'')',
        -- p_referencing_table,
        -- p_referenced_table,
        -- p_column_mappings,
        -- p_template_fk_name,
        -- p_update_condition,
        -- p_delete_condition,
        -- p_trigger_conditions;

        -- Load the referencing columns from the JSON column mappings
        all_local_columns := (SELECT array_agg(k) FROM jsonb_object_keys(p_column_mappings) k);
        referencing_columns := (SELECT array_agg("key") FROM jsonb_each_text(p_column_mappings) WHERE "value" IS NOT NULL);
        referenced_columns := (SELECT array_agg("value") FROM jsonb_each_text(p_column_mappings) WHERE "value" IS NOT NULL);

        IF p_template_fk_name IS NULL THEN
          p_template_fk_name := format('%1$s_%2$s_%3$s_%4$s',
            regexp_replace(regexp_replace(p_referencing_table, '^(?:.*?\.)?(.*?)$', '\1'),'(.).*?(_|$)', '\1\2', 'g'),
            regexp_replace(array_to_string(referencing_columns, '_'), '(.).*?(_|$)', '\1\2', 'g'),
            regexp_replace(p_referenced_table, '^(?:.*?\.)?(.*?)$', '\1'),
            regexp_replace(array_to_string(referenced_columns, '_'), '(.).*?(_|$)', '\1\2', 'g')
          );
        END IF;

        trigger_condition_clause := array_to_string((
          SELECT array_agg('NEW.' || col_name || ' IN (''' || array_to_string(col_values, ''', ''') || ''')')
          FROM (
            SELECT col_name, array_agg(col_values) AS col_values
            FROM (SELECT "key" AS col_name, jsonb_array_elements_text("value"::JSONB) AS "col_values" FROM (SELECT * FROM jsonb_each_text(p_trigger_conditions)) x) y
            GROUP BY col_name
          ) z),
          ' AND '
        );

        aux_array := ARRAY[
          array_to_string(referencing_columns, ', '),                                                                                         -- 1
          p_referenced_table,                                                                                                                 -- 2
          array_to_string(referenced_columns, ', '),                                                                                          -- 3
          '{' || array_to_string(referencing_columns, ', ') || '}',                                                                           -- 4
          array_to_string(all_local_columns, ', '),                                                                                           -- 5
          '{' || array_to_string(referenced_columns, ', ') || '}',                                                                            -- 6
          p_referencing_table,                                                                                                                -- 7
          regexp_replace(p_referencing_table, '^(?:.*?\.)?(.*?)$', '\1'),                                                                     -- 8
          p_template_fk_name,                                                                                                                 -- 9
          array_to_string((SELECT array_agg('NEW.' || f || ' IS NOT NULL') FROM unnest(all_local_columns) as f), ' AND '),                    -- 10
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(referenced_columns) as f), ' OR '),  -- 11
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(all_local_columns) as f), ' OR '),   -- 12
          array_to_string((SELECT array_agg('OLD.' || f || ' IS NOT NULL') FROM unnest(referenced_columns) as f), ' AND '),                   -- 13
          trigger_condition_clause,                                                                                                           -- 14
          regexp_replace(trigger_condition_clause, 'NEW\.', '', 'g'),                                                                         -- 15
          p_trigger_conditions::TEXT,                                                                                                         -- 16
          substring(format('trg_vfkpr_au_c_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                         -- 17
          substring(format('trg_vfkpr_au_sn_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 18
          substring(format('trg_vfkpr_au_sd_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 19
          substring(format('trg_vfkpr_bu_r_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                         -- 20
          substring(format('trg_vfkpr_au_na_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 21
          substring(format('trg_vfkpr_ad_c_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                         -- 22
          substring(format('trg_vfkpr_ad_sn_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 23
          substring(format('trg_vfkpr_ad_sd_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 24
          substring(format('trg_vfkpr_ad_r_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                         -- 25
          substring(format('trg_vfkpr_ad_na_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 26
          substring(format('trg_vfkp_bi_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 27
          substring(format('trg_vfkp_bu_%1$s', p_template_fk_name) FROM 1 FOR 63)                                                             -- 28
        ];

        -- Create before insert trigger
        queries := queries || format($$
          CREATE TRIGGER %27$I
            BEFORE INSERT ON %7$s
            FOR EACH ROW
              WHEN (%14$s AND %10$s)
            EXECUTE PROCEDURE sharding.trf_virtual_fk_before_insert_or_update('%4$s', '{%2$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before update trigger
        queries := queries || format($$
          CREATE TRIGGER %28$I
            BEFORE UPDATE OF %5$s ON %7$s
            FOR EACH ROW
              WHEN ((%12$s) AND %14$s AND %10$s)
            EXECUTE PROCEDURE sharding.trf_virtual_fk_before_insert_or_update('%4$s', '{%2$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before delete or update triggers on referenced table
        -- Check the ON UPDATE clause of the foreign key
        CASE p_update_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after update trigger on the referenced table to cascade the change to any referencing field
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %17$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_cascade('%4$s', '%7$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[17]);

          WHEN 'n' THEN -- SET NULL
            -- Create the after update trigger on the referenced table to set any referencing records to NULL
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %18$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_null('%4$s', '%7$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[18]);

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after update trigger on the referenced table to set any referencing records to their default value
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %19$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_default('%4$s', '%7$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[19]);

          WHEN 'r' THEN -- RESTRICT
            -- Create the before update trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %20$I
                 BEFORE UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%7$s', '%6$s', %16$L);
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[20]);

          ELSE -- If NULL, default to NO ACTION
            -- Create the after update trigger on the referenced table to prevent changing the key fields is they're being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT CONSTRAINT triggers that are deferred
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %21$I
                 BEFORE UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%7$s', '%6$s', %16$L);
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[21]);
        END CASE;

        -- Check the ON DELETE clause of the foreign key
        CASE p_delete_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after delete trigger on the referenced table to cascade the deletion to referenced rows
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %22$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_cascade('%4$s', '%7$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[22]);

          WHEN 'n' THEN -- SET NULL
            -- Create the after delete trigger on the referenced table to set any referencing records to NULL
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %23$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_null('%4$s', '%7$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[23]);

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after delete trigger on the referenced table to set any referencing records to their default value
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %24$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_default('%4$s', '%7$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[24]);

          WHEN 'r' THEN -- RESTRICT
            -- Create the before delete trigger on the referenced table to prevent deleting the row if the key fields are being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %25$I
                 BEFORE DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%7$s', '%6$s', %16$L);
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[25]);

          ELSE -- If NULL, default to NO ACTION
            -- Create the before delete trigger on the referenced table to prevent changing the key fields is they're being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT triggers
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %26$I
                 BEFORE DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%7$s', '%6$s', %16$L);
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[26]);
        END CASE;

        RETURN queries;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.get_create_virtual_foreign_key_to_inherited_table_queries(
        IN p_referencing_table TEXT, -- p_destination_schema_name.p_template_table_name
        IN p_parent_referenced_table TEXT,
        IN p_child_referenced_table TEXT,
        IN p_referencing_columns TEXT[], -- aux_array[1]
        IN p_referenced_columns TEXT[], -- aux_array[3]
        IN p_template_fk_name TEXT DEFAULT NULL, -- catalog_info.conname
        IN p_update_condition "char" DEFAULT NULL,
        IN p_delete_condition "char" DEFAULT NULL
      )
      RETURNS TEXT[] AS $BODY$
      DECLARE
        aux_array TEXT[];
        queries TEXT[];
      BEGIN
        -- RAISE NOTICE 'sharding.get_create_virtual_foreign_key_to_inherited_table_queries(''%'', ''%'', ''%'', ''%'', ''%'', ''%'', ''%'', ''%'');',
        --   p_referencing_table,
        --   p_parent_referenced_table,
        --   p_child_referenced_table,
        --   p_referencing_columns,
        --   p_referenced_columns,
        --   p_template_fk_name,
        --   p_update_condition,
        --   p_delete_condition;

        IF p_template_fk_name IS NULL THEN
          p_template_fk_name := format('%1$s_%2$s_%3$s',
            regexp_replace(array_to_string(p_referencing_columns, '_'), '(.).*?(_|$)', '\1\2', 'g'),
            regexp_replace(p_referenced_table, '^(?:.*?\.)?(.*?)$', '\1'),
            regexp_replace(array_to_string(p_referenced_columns, '_'), '(.).*?(_|$)', '\1\2', 'g')
          );
        END IF;

        aux_array := ARRAY[
          array_to_string(p_referencing_columns, ', '),                                                                                           -- 1
          p_parent_referenced_table,                                                                                                              -- 2
          array_to_string(p_referenced_columns, ', '),                                                                                            -- 3
          '{' || array_to_string(p_referencing_columns, ', ') || '}',                                                                             -- 4
          p_child_referenced_table,                                                                                                               -- 5
          '{' || array_to_string(p_referenced_columns, ', ') || '}',                                                                              -- 6
          p_referencing_table,                                                                                                                    -- 7
          regexp_replace(p_referencing_table, '^(?:.*?\.)?(.*?)$', '\1'),                                                                         -- 8
          p_template_fk_name,                                                                                                                     -- 9
          array_to_string((SELECT array_agg('NEW.' || f || ' IS NOT NULL') FROM unnest(p_referencing_columns) as f), ' AND '),                    -- 10
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(p_referenced_columns) as f), ' OR '),    -- 11
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(p_referencing_columns) as f), ' OR '),   -- 12
          array_to_string((SELECT array_agg('OLD.' || f || ' IS NOT NULL') FROM unnest(p_referenced_columns) as f), ' AND '),                     -- 13
          substring(format('trg_vfkir_au_c_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                             -- 14
          substring(format('trg_vfkir_au_sn_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 15
          substring(format('trg_vfkir_au_sd_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 16
          substring(format('trg_vfkir_bu_r_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                             -- 17
          substring(format('trg_vfkir_au_na_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 18
          substring(format('trg_vfkir_ad_c_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                             -- 19
          substring(format('trg_vfkir_ad_sn_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 20
          substring(format('trg_vfkir_ad_sd_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 21
          substring(format('trg_vfkir_ad_r_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                             -- 22
          substring(format('trg_vfkir_ad_na_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 23
          substring(format('trg_vfki_bi_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                                -- 25
          substring(format('trg_vfki_bu_%1$s', p_template_fk_name) FROM 1 FOR 63)                                                                 -- 25
        ];

        -- Create before insert trigger
        queries := queries || format($$
          CREATE TRIGGER %24$I
            BEFORE INSERT ON %7$s
            FOR EACH ROW
              WHEN (%10$s)
            EXECUTE PROCEDURE sharding.trf_virtual_fk_before_insert_or_update('%4$s', '{%2$s, %5$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before update trigger
        queries := queries || format($$
          CREATE TRIGGER %25$I
            BEFORE UPDATE OF %1$s ON %7$s
            FOR EACH ROW
              WHEN ((%12$s) AND %10$s)
            EXECUTE PROCEDURE sharding.trf_virtual_fk_before_insert_or_update('%4$s', '{%2$s, %5$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before delete or update triggers on referenced table
        -- Check the ON UPDATE clause of the foreign key
        CASE p_update_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after update trigger on the parent referenced table to cascade the update to the referencing fields
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %14$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_cascade('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[14]);

            -- Create the after update trigger on the child referenced table to cascade the update to the referencing fields
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %14$I
                 AFTER UPDATE OF %3$s ON %5$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_cascade('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[5], aux_array[14]);

          WHEN 'n' THEN -- SET NULL
            -- Create the after update trigger on the parent referenced table to set the referencing fields to null
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %15$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_null('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[15]);

            -- Create the after update trigger on the child referenced table to set the referencing fields to null
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %15$I
                 AFTER UPDATE OF %3$s ON %5$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_null('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[5], aux_array[15]);

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after update trigger on the parent referenced table to set the referencing fields to their default values
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %16$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_default('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[16]);

            -- Create the after update trigger on the child referenced table to set the referencing fields to their default values
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %16$I
                 AFTER UPDATE OF %3$s ON %5$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_default('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[5], aux_array[16]);

          WHEN 'r' THEN -- RESTRICT
            -- Create the before update trigger on the parent referenced table to prevent changing the key fields if they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %17$I
                 BEFORE UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[17]);

            -- Create the before update trigger on the child referenced table to prevent changing the key fields if they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %17$I
                 BEFORE UPDATE OF %3$s ON %5$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[5], aux_array[17]);

          ELSE -- If NULL, default to NO ACTION
            -- Create the before update trigger on the parent referenced table to prevent changing the key fields if they're being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT triggers
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %18$I
                 BEFORE UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[18]);

            -- Create the before update trigger on the child referenced table to prevent changing the key fields if they're being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT triggers
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %18$I
                 BEFORE UPDATE OF %3$s ON %5$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[5], aux_array[18]);
        END CASE;

        -- Check the ON DELETE clause of the foreign key
        CASE p_delete_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after delete trigger on the referenced table to delete the rows referencing the deleted row
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %19$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_cascade('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[19]);

            -- Create the after delete trigger on the referenced table to delete the rows referencing the deleted row
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %19$I
                 AFTER DELETE ON %5$s
                 FOR EACH ROW
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_cascade('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[5], aux_array[19]);

          WHEN 'n' THEN -- SET NULL
            -- Create the after delete trigger on the parent referenced table set the referencing fields to null
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %20$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_null('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[20]);

            -- Create the after delete trigger on the child referenced table set the referencing fields to null
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %20$I
                 AFTER DELETE ON %5$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_null('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[5], aux_array[20]);

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after delete trigger on the parent referenced table to set the referencing fields to their default values
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %21$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_default('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[21]);

            -- Create the after delete trigger on the child referenced table to set the referencing fields to their default values
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %21$I
                 AFTER DELETE ON %5$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_default('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[5], aux_array[21]);

          WHEN 'r' THEN -- RESTRICT
            -- Create the before delete trigger on the parent referenced table to prevent deleting the row if it's being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %22$I
                 BEFORE DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[22]);

            -- Create the before delete trigger on the child referenced table to prevent deleting the row if it's being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %22$I
                 BEFORE DELETE ON %5$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[5], aux_array[22]);

          ELSE -- If NULL, default to NO ACTION
            -- Create the before delete trigger on the parent referenced table to prevent deleting the row if it's being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT triggers
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %23$I
                 BEFORE DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[23]);

            -- Create the before delete trigger on the child referenced table to prevent deleting the row if it's being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT triggers
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %23$I
                 BEFORE DELETE ON %5$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[5], aux_array[23]);

        END CASE;

        RETURN queries;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.get_create_virtual_foreign_key_queries(
        IN p_referencing_table TEXT,
        IN p_referencing_columns TEXT[],
        IN p_referenced_table TEXT,
        IN p_referenced_columns TEXT[],
        IN p_template_fk_name TEXT DEFAULT NULL,
        IN p_update_condition "char" DEFAULT NULL,
        IN p_delete_condition "char" DEFAULT NULL
      )
      RETURNS TEXT[] AS $BODY$
      DECLARE
        aux_array TEXT[];
        queries TEXT[];
        referencing_schema TEXT;
      BEGIN
        -- RAISE NOTICE 'sharding.get_create_virtual_foreign_key_queries(''%'', ''%'', ''%'', ''%'', ''%'', ''%'', ''%'')',
        --   p_referencing_table,
        --   p_referencing_columns,
        --   p_referenced_table,
        --   p_referenced_columns,
        --   p_template_fk_name,
        --   p_update_condition,
        --   p_delete_condition;

        IF p_template_fk_name IS NULL THEN
          p_template_fk_name := format('%1$s_%2$s_%3$s',
            regexp_replace(array_to_string(p_referencing_columns, '_'), '(.).*?(_|$)', '\1\2', 'g'),
            regexp_replace(p_referenced_table, '^(?:.*?\.)?(.*?)$', '\1'),
            regexp_replace(array_to_string(p_referenced_columns, '_'), '(.).*?(_|$)', '\1\2', 'g')
          );
        END IF;

        referencing_schema := regexp_replace(p_referencing_table, '^(?:(.+?)\.)?.*?$', '\1');

        aux_array := ARRAY[
          array_to_string(p_referencing_columns, ', '),                                                                                                    -- 1
          p_referenced_table,                                                                                                                              -- 2
          array_to_string(p_referenced_columns, ', '),                                                                                                     -- 3
          '{' || array_to_string(p_referencing_columns, ', ') || '}',                                                                                      -- 4
          p_referenced_table,                                                                                                                              -- 5
          '{' || array_to_string(p_referenced_columns, ', ') || '}',                                                                                       -- 6
          p_referencing_table,                                                                                                                             -- 7
          regexp_replace(p_referencing_table, '^(?:.*?\.)?(.*?)$', '\1'),                                                                                  -- 8
          p_template_fk_name,                                                                                                                              -- 9
          array_to_string((SELECT array_agg('NEW.' || f || ' IS NOT NULL') FROM unnest(p_referencing_columns) as f), ' AND '),                             -- 10
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(p_referenced_columns) as f), ' OR '),             -- 11
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(p_referencing_columns) as f), ' OR '),            -- 12
          array_to_string((SELECT array_agg('OLD.' || f || ' IS NOT NULL') FROM unnest(p_referenced_columns) as f), ' AND '),                              -- 13
          substring(format('trg_v%2$sfkr_au_c_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),     -- 14
          substring(format('trg_v%2$sfkr_au_sn_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 15
          substring(format('trg_v%2$sfkr_au_sd_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 16
          substring(format('trg_v%2$sfkr_bu_r_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),     -- 17
          substring(format('trg_v%2$sfkr_au_na_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 18
          substring(format('trg_v%2$sfkr_ad_c_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),     -- 19
          substring(format('trg_v%2$sfkr_ad_sn_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 20
          substring(format('trg_v%2$sfkr_ad_sd_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 21
          substring(format('trg_v%2$sfkr_ad_r_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),     -- 22
          substring(format('trg_v%2$sfkr_ad_na_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 23
          substring(format('trg_v%2$sfk_bi_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),        -- 24
          substring(format('trg_v%2$sfk_bu_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),        -- 25
          CASE WHEN referencing_schema = 'public' THEN 'trf_virtual_public_fk_before_insert_or_update' ELSE 'trf_virtual_fk_before_insert_or_update' END,  -- 26
          CASE WHEN referencing_schema = 'public' THEN p_referencing_table ELSE regexp_replace(p_referencing_table, '^(?:.*?\.)?(.*?)$', '\1') END         -- 27
        ];

        -- Create before insert trigger
        queries := queries || format($$
          CREATE TRIGGER %24$I
            BEFORE INSERT ON %7$s
            FOR EACH ROW
              WHEN (%10$s)
            EXECUTE PROCEDURE sharding.%26$s('%4$s', '{%2$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before update trigger
        queries := queries || format($$
          CREATE TRIGGER %25$I
            BEFORE UPDATE OF %1$s ON %7$s
            FOR EACH ROW
              WHEN ((%12$s) AND %10$s)
            EXECUTE PROCEDURE sharding.%26$s('%4$s', '{%2$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before delete or update triggers on referenced table
        -- Check the ON UPDATE clause of the foreign key
        CASE p_update_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after update trigger on the referenced table to cascade the change to any referencing field
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %14$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_cascade('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[14]);

          WHEN 'n' THEN -- SET NULL
            -- Create the after update trigger on the referenced table to set any referencing fields to NULL
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %15$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_null('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[15]);

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after update trigger on the referenced table to set any referencing fields to their default value
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %16$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_default('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[16]);

          WHEN 'r' THEN -- RESTRICT
            -- Create the before update trigger on the referenced table to prevent changing the key fields if they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %17$I
                 BEFORE UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[17]);

          ELSE -- If NULL, default to NO ACTION
            -- Create the before update trigger on the referenced table to prevent changing the key fields if they're being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT triggers
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %18$I
                 BEFORE UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[18]);
        END CASE;

        -- Check the ON DELETE clause of the foreign key
        CASE p_delete_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after update trigger on the referenced table to cascade the deletion to any referencing record
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %19$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_cascade('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[19]);

          WHEN 'n' THEN -- SET NULL
            -- Create the after delete trigger on the referenced table to set any referencing fields to NULL
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %20$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_null('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[20]);

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after delete trigger on the referenced table to set any referencing fields to their default values
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %21$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_default('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[21]);

          WHEN 'r' THEN -- RESTRICT
            -- Create the before delete trigger on the referenced table to prevent deleting the record if the key fields are being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %22$I
                 BEFORE DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[22]);

          ELSE -- If NULL, default to NO ACTION
            -- Create the before delete trigger on the referenced table to prevent deleting the record if the key fields are being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT triggers
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %23$I
                 BEFORE DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ), aux_array[2], aux_array[23]);
        END CASE;

        RETURN queries;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.generate_create_company_shard_function(
        IN p_use_original_sequence BOOLEAN DEFAULT TRUE
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        auxiliary_table_information JSONB;

        all_objects_data JSONB;
        object_data JSON;
        qualified_object_name TEXT;
        schema_name TEXT;
        object_name TEXT;
        aux TEXT;
        aux_array TEXT[];
        col_default_value TEXT;

        json_object JSON;

        queries TEXT[];
        query TEXT;
        before_query TEXT;
        after_queries TEXT[];
        p_destination_schema_name TEXT;
        shard_company_id TEXT;
        original_search_path TEXT;
      BEGIN
        SHOW search_path INTO original_search_path;
        SET search_path TO '';

        p_destination_schema_name := '%1$I';
        shard_company_id := '%2$L';

        -- Let's grant that we will use original sequences (temporary hack)
        p_use_original_sequence := TRUE;

        auxiliary_table_information = sharding.get_auxiliary_table_information();

        queries := queries || format($$
          INSERT INTO sharding.sharding_statistics (sharding_key, structure_sharding_started_at) VALUES (%1$s, clock_timestamp())
        $$, shard_company_id);

        -------------------------------------------------------------------------------------------------------------
        -- Invoke the sharding.get_queries_to_run_before_sharding_company_structure function if set by the project --
        -------------------------------------------------------------------------------------------------------------

        IF common.function_exists('sharding.get_queries_to_run_before_sharding_company_structure') THEN
          queries := queries || (SELECT sharding.get_queries_to_run_before_sharding_company_structure(auxiliary_table_information));
        END IF;


        -- Get the necessary data to create the new tables, indexes, stored procedures and triggers
        WITH table_columns AS (
          SELECT
            t.tablename AS object_name,
            format('%1$I.%2$I', t.schemaname, t.tablename) AS qualified_object_name,
            (t.schemaname || '.' || t.tablename)::regclass::oid AS table_oid,
            json_agg(json_build_object(
              'name', a.attname,
              'type', pg_catalog.format_type(a.atttypid, a.atttypmod),
              'default_value', (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
              'is_not_null', a.attnotnull
            ) ORDER BY a.attnum)::JSONB AS columns
          FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_tables t ON c.oid = (t.schemaname || '.' || t.tablename)::regclass::oid
          WHERE a.attnum > 0
            AND NOT a.attisdropped
            AND n.nspname = 'public'
          GROUP BY t.schemaname, t.tablename
        ),
        table_indexes AS (
          SELECT
            format('%1$I.%2$I', t.schemaname, t.tablename) AS qualified_object_name,
            (t.schemaname || '.' || t.tablename)::regclass::oid AS table_oid,
            json_agg(json_build_object(
              'name', c2.relname,
              'is_primary', i.indisprimary,
              'is_unique', i.indisunique,
              'definition', pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
              'constraint_definition', pg_catalog.pg_get_constraintdef(con.oid, true)
            )::JSONB) AS indexes
          FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_index i ON c.oid = i.indrelid
            JOIN pg_catalog.pg_class c2 ON i.indexrelid = c2.oid
            LEFT JOIN pg_catalog.pg_constraint con ON (con.conrelid = i.indrelid AND con.conindid = i.indexrelid AND con.contype IN ('p','u','x'))
            JOIN pg_catalog.pg_tables t ON c.oid = (t.schemaname || '.' || t.tablename)::regclass::oid
          WHERE t.schemaname = 'public'
          GROUP BY t.schemaname, t.tablename
        ),
        table_foreign_keys AS (
          SELECT
            format('%1$I.%2$I', t.schemaname, t.tablename) AS qualified_object_name,
            (t.schemaname || '.' || t.tablename)::regclass::oid AS table_oid,
            json_agg(json_build_object(
              'name', c.conname,
              'update_action', c.confupdtype,
              'delete_action', c.confdeltype,
              'definition', pg_catalog.pg_get_constraintdef(c.oid, true)
            )::JSONB) AS foreign_keys
          FROM pg_catalog.pg_constraint c
            LEFT JOIN pg_catalog.pg_tables t ON c.conrelid = (t.schemaname || '.' || t.tablename)::regclass::oid
          WHERE c.contype = 'f'
            AND t.schemaname = 'public'
          GROUP BY t.schemaname, t.tablename
        ),
        table_triggers AS (
          SELECT
            format('%1$I.%2$I', ta.schemaname, ta.tablename) AS qualified_object_name,
            (ta.schemaname || '.' || ta.tablename)::regclass::oid AS table_oid,
            json_agg(json_build_object(
              'name', t.tgname,
              'definition', pg_catalog.pg_get_triggerdef(t.oid, true)
            )::JSONB) AS triggers
          FROM pg_catalog.pg_trigger t
            LEFT JOIN pg_catalog.pg_tables ta ON t.tgrelid = (ta.schemaname || '.' || ta.tablename)::regclass::oid
          WHERE ta.schemaname = 'public'
            AND (NOT t.tgisinternal OR (t.tgisinternal AND t.tgenabled = 'D'))
            AND t.tgname != 'trg_prevent_insert_or_update_on_sharded_companies' -- Don't copy the prevent trigger for sharded companies
            AND t.tgname !~* '^trg_vfk(?:i|p)?' -- Don't copy the virtual foreign key triggers
          GROUP BY ta.schemaname, ta.tablename
        )
        SELECT
          json_object_agg(c.qualified_object_name,
            json_build_object(
              'columns', c.columns,
              'indexes', i.indexes,
              'foreign_keys', fk.foreign_keys,
              'triggers', trg.triggers
            )
          )::JSONB INTO all_objects_data
        FROM table_columns c
          LEFT JOIN table_indexes i ON c.table_oid = i.table_oid
          LEFT JOIN table_foreign_keys fk ON c.table_oid = fk.table_oid
          LEFT JOIN table_triggers trg ON c.table_oid = trg.table_oid
        WHERE c.object_name::TEXT NOT IN (
          SELECT jsonb_array_elements_text FROM jsonb_array_elements_text(auxiliary_table_information->'unsharded_tables')
        );

        ----------------------
        -- Build the tables --
        ----------------------

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP
          -- Reset variables
          aux := NULL;
          before_query := NULL;
          after_queries := '{}';

          object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');
          -- RAISE DEBUG 'object_name: %', object_name;

          queries := queries || format('RAISE DEBUG ''-- [TABLES] TABLE: %1$I'';', object_name);

          query := format('CREATE TABLE %1$s.%2$I (', p_destination_schema_name, object_name);

          FOR json_object IN SELECT * FROM json_array_elements(object_data->'columns') LOOP
            -- RAISE DEBUG 'column: %', json_object;
            col_default_value := NULL;

            IF NOT p_use_original_sequence AND (json_object->>'default_value') IS NOT NULL AND json_object->>'default_value' ~ '^nextval\('
               -- we cannot use specific sequence on inherited tables (otherwise values will collide on parent table)
               AND NOT auxiliary_table_information->'inherited_tables' ? object_name THEN
              -- Need to create a new sequence for the primary key
              aux := regexp_replace(json_object->>'default_value', 'nextval\(''(?:.+\.)?(.*)''.*', '\1');

              col_default_value := format('nextval(''%1$s.%2$s''::regclass)', p_destination_schema_name, aux);

              before_query := format('CREATE SEQUENCE %1$s.%2$I;', p_destination_schema_name, aux);
              after_queries := after_queries
                            || format('ALTER SEQUENCE %1$s.%2$I OWNED BY %1$s.%3$I.%4$I;', p_destination_schema_name, aux, object_name, json_object->>'name')
                            || format('EXECUTE ''SELECT last_value FROM public.%1$I'' INTO seq_nextval;', aux)
                            || format('EXECUTE format(''ALTER SEQUENCE %%1$s.%1$I RESTART WITH %%2$s'', p_company_schema_name, seq_nextval);', aux);
            END IF;

            IF col_default_value IS NULL THEN
              col_default_value := json_object->>'default_value';
            END IF;

            query := query || format('%1$I %2$s%3$s%4$s%5$s, ',
              json_object->>'name',
              json_object->>'type',
              CASE WHEN (json_object->>'is_not_null')::BOOLEAN THEN ' NOT NULL' END,
              CASE WHEN col_default_value IS NOT NULL THEN format(' DEFAULT %1$s', col_default_value) END,
              CASE WHEN json_object->>'name' = 'company_id' THEN format(' CONSTRAINT "company_id must equal %1$s" CHECK (company_id = %1$s)', shard_company_id) END
            );

          END LOOP;

          query := LEFT(query, length(query) - 2);

          if auxiliary_table_information->'inherited_tables' ? object_name THEN
            query := query || format(') INHERITS (%1$s);', qualified_object_name);
          ELSE
            query := query || ');';
          END IF;

          IF before_query IS NOT NULL THEN
            queries := queries || before_query;
          END IF;

          queries := queries || query || after_queries;

          -- raise DEBUG 'query: %', query;
        END LOOP;

        -----------------------
        -- Build the indexes --
        -----------------------

        queries := queries || '{ -- Create indexes }'::TEXT[];

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP
          -- Reset variables
          aux := NULL;

          object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');
          -- RAISE DEBUG 'object_name: %', object_name;

          queries := queries || format('RAISE DEBUG ''-- [INDEXES] TABLE: %1$I'';', object_name);

          IF (object_data->>'indexes') IS NOT NULL THEN
            FOR json_object IN SELECT * FROM json_array_elements(object_data->'indexes') LOOP
              queries := queries || format('%1$s;', regexp_replace(json_object->>'definition', ' ON (?:.+\.)?', format(' ON %1$s.', p_destination_schema_name)));

              IF (json_object->>'is_primary')::BOOLEAN THEN
                queries := queries || format('ALTER TABLE %1$s.%2$I ADD CONSTRAINT %4$I PRIMARY KEY USING INDEX %3$I;', p_destination_schema_name, object_name, json_object->>'name', format('%1$s_pkey', object_name));
              END IF;
            END LOOP;
          END IF;
        END LOOP;

        ----------------------------
        -- Build the foreign keys --
        ----------------------------

        queries := queries || '{ -- Create foreign keys }'::TEXT[];

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP
          -- Reset variables
          aux := NULL;

          object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');
          schema_name := COALESCE(regexp_replace(qualified_object_name, '^(?:(.+)\.)?(?:.*)$', '\1'), 'public');
          -- RAISE DEBUG 'object_name: %', object_name;

          queries := queries || format('RAISE DEBUG ''-- [FOREIGN KEYS] TABLE: %1$I'';', object_name);

          IF (object_data->>'foreign_keys') IS NOT NULL THEN
            RAISE DEBUG '% foreign_keys: %', object_name, object_data->'foreign_keys';

            FOR json_object IN SELECT * FROM json_array_elements(object_data->'foreign_keys') LOOP

              -- Get the name of the referenced table
              aux := (regexp_matches(json_object->>'definition', 'REFERENCES (?:.*?\.)?(.*?)\('))[1];

              -- If the referenced table is in the unsharded tables list, we need to create some 'virtual' foreign keys via triggers
              IF auxiliary_table_information->'unsharded_tables' ? aux THEN
                -- aux_array[1] = local columns
                -- aux_array[2] = referenced table
                -- aux_array[3] = referenced columns
                aux_array := regexp_matches(json_object->>'definition', 'FOREIGN KEY \((.*?)\) REFERENCES ((?:.*?\.)?.*?)\((.*?)\)');

                queries := queries || sharding.get_create_virtual_foreign_key_queries(
                  format('%1$s.%2$I', p_destination_schema_name, object_name),
                  regexp_split_to_array(aux_array[1], '\s*,\s*')::TEXT[],
                  aux_array[2],
                  regexp_split_to_array(aux_array[3], '\s*,\s*')::TEXT[],
                  json_object->>'name',
                  (json_object->>'update_action')::"char",
                  (json_object->>'delete_action')::"char"
                );
              -- If the referenced table is in the inherited tables list, we need to create some 'virtual' foreign keys via triggers
              -- to both the parent and the child table
              ELSIF auxiliary_table_information->'inherited_tables' ? aux THEN
                -- aux_array[1] = local columns
                -- aux_array[2] = referenced table
                -- aux_array[3] = referenced columns
                aux_array := regexp_matches(json_object->>'definition', 'FOREIGN KEY \((.*?)\) REFERENCES (?:.*?\.)?(.*?)\((.*?)\)');

                -- RAISE DEBUG 'aux_array: %', aux_array;

                queries := queries || sharding.get_create_virtual_foreign_key_to_inherited_table_queries(
                  format('%1$s.%2$I', p_destination_schema_name, object_name),
                  format('%1$s.%2$I', schema_name, aux_array[2]),
                  format('%1$s.%2$I', p_destination_schema_name, aux_array[2]),
                  regexp_split_to_array(aux_array[1], '\s*,\s*')::TEXT[],
                  regexp_split_to_array(aux_array[3], '\s*,\s*')::TEXT[],
                  json_object->>'name',
                  (json_object->>'update_action')::"char",
                  (json_object->>'delete_action')::"char"
                );
              ELSE
                -- It's a foreign key for the same shard, so we can replicate it
                queries := queries || ARRAY[format('ALTER TABLE %1$s.%2$I ADD CONSTRAINT %3$I %4$s;',
                  p_destination_schema_name,
                  object_name,
                  json_object->>'name',
                  regexp_replace(json_object->>'definition', 'REFERENCES (?:.*?\.)?', format('REFERENCES %1$s.', p_destination_schema_name))
                )];
              END IF;
            END LOOP;
          END IF;
        END LOOP;

        ------------------------
        -- Build the triggers --
        ------------------------

        queries := queries || '{ -- Create triggers }'::TEXT[];

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP
          -- Reset variables
          aux := NULL;

          object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');
          -- RAISE DEBUG 'object_name: %', object_name;

          queries := queries || format('RAISE DEBUG ''-- [TRIGGERS] TABLE: %1$I'';', object_name);

          IF (object_data->>'triggers') IS NOT NULL THEN
            FOR json_object IN SELECT * FROM json_array_elements(object_data->'triggers') LOOP
              -- Just replace the name of the table. The executed procedure will NOT be replicated, but should handle the different schemas
              queries := queries || regexp_replace(
                json_object->>'definition',
                ' ON (?:\S+?\.)?',
                format(' ON %1$s.', p_destination_schema_name)
              );
            END LOOP;
          END IF;
        END LOOP;

        ---------------------
        -- Build the views --
        ---------------------

        SELECT json_object(array_agg(dependent_view), array_agg(depends_on))::JSONB
          INTO all_objects_data
        FROM (
          SELECT
            dependent_view.relname::TEXT AS dependent_view,
            array_agg(source_view.relname)::TEXT AS depends_on
          FROM pg_depend
            JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
            JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
            JOIN pg_class as source_view ON pg_depend.refobjid = source_view.oid
            JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
            JOIN pg_namespace source_ns ON source_ns.oid = source_view.relnamespace
          WHERE source_ns.nspname = 'public'
            AND dependent_ns.nspname = 'public'
            AND source_view.relname != dependent_view.relname
            AND source_view.relkind = 'v'
          GROUP by dependent_view.relname
        ) views_dependencies;

        FOR qualified_object_name, aux IN
          SELECT
            format('%1$I.%2$I', v.schemaname, v.viewname),
            pg_catalog.pg_get_viewdef(c.oid)
          FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_views v ON c.oid = (v.schemaname || '.' || v.viewname)::regclass::oid
          WHERE n.nspname = 'public'
            AND NOT all_objects_data ? v.viewname
        LOOP
          object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');

          aux := regexp_replace(aux, 'public\.', '', 'g');

          queries := queries || format('CREATE VIEW %1$s.%2$I AS %3$s;',
            p_destination_schema_name,
            object_name,
            aux
          );
        END LOOP;

        FOR qualified_object_name, aux IN
          SELECT
            format('%1$I.%2$I', v.schemaname, v.viewname),
            pg_catalog.pg_get_viewdef(c.oid)
          FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_views v ON c.oid = (v.schemaname || '.' || v.viewname)::regclass::oid
          WHERE n.nspname = 'public'
            AND all_objects_data ? v.viewname
        LOOP
          object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');

          aux := regexp_replace(aux, 'public\.', '', 'g');

          queries := queries || format('CREATE VIEW %1$s.%2$I AS %3$s;',
            p_destination_schema_name,
            object_name,
            aux
          );

        END LOOP;

        ------------------------------------------------------------------------------------------------------------
        -- Invoke the sharding.get_queries_to_run_after_sharding_company_structure function if set by the project --
        ------------------------------------------------------------------------------------------------------------

        IF common.function_exists('sharding.get_queries_to_run_after_sharding_company_structure') THEN
          queries := queries || (SELECT sharding.get_queries_to_run_after_sharding_company_structure(auxiliary_table_information));
        END IF;

        queries := queries || format($$
          UPDATE sharding.sharding_statistics
          SET structure_sharding_ended_at = clock_timestamp()
          WHERE sharding_key = %1$s;
        $$, shard_company_id);

        --------------------------------
        -- Create the actual function --
        --------------------------------

        query := format($$
          CREATE OR REPLACE FUNCTION sharding.create_company_shard(
            IN p_company_id           INTEGER,
            IN p_company_schema_name  TEXT
          )
          RETURNS BOOLEAN AS $FUNCTION_BODY$
          DECLARE
            query                   TEXT;
            seq_nextval             BIGINT;
            previous_search_path    TEXT;
            spath                   TEXT;
            rec                     RECORD;
            current_public_triggers TEXT[];
          BEGIN
            SHOW search_path INTO previous_search_path;
            EXECUTE 'SET search_path to ' || p_company_schema_name || ', public';
            SHOW search_path INTO spath;

            SELECT array_agg('public.' || c.relname || '::' || t.tgname)
            FROM pg_trigger t
              JOIN pg_class c ON t.tgrelid = c.oid
              JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE NOT t.tgisinternal
              AND n.nspname = 'public'
            INTO current_public_triggers;

            %1$s

            EXECUTE 'SET search_path to ' || previous_search_path;

            RETURN TRUE;
          END;
          $FUNCTION_BODY$ LANGUAGE 'plpgsql';
        $$,
          (
            SELECT string_agg(
              CASE WHEN unnest ~* '^(?:--|RAISE|EXECUTE|SHOW)'
              THEN format(E'\n      %1$s', unnest)
              ELSE format(E'EXECUTE format(%1$L, p_company_schema_name, p_company_id, current_public_triggers);', regexp_replace(unnest, '\s+', ' ', 'g'))
              -- Switch this with the previous one for debug
              -- ELSE format(E'query := format(%1$L, p_company_schema_name, p_company_id);\n      RAISE DEBUG ''query: %%'', query;\n      EXECUTE query;', regexp_replace(unnest, '\s+', ' ', 'g'))
              END, E'\n      '
            )
            FROM unnest(queries)
          )
        );

        RAISE DEBUG 'query: %', query;

        EXECUTE query;

        EXECUTE 'SET search_path TO ''' || original_search_path || '''';

        RETURN TRUE;
      -- EXCEPTION
      --   WHEN OTHERS THEN
      --     RETURN false;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute %Q[DROP FUNCTION IF EXISTS sharding.wrap_with_duplicate_check(TEXT);]
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.wrap_with_duplicate_check(
        IN p_query        TEXT,
        IN p_table_name   TEXT,
        IN p_trigger_name TEXT
      )
      RETURNS TEXT AS $BODY$
      DECLARE
      BEGIN
        RETURN format(
          $RETURN$
            DO $BLOCK$
              BEGIN
                IF NOT '%2$s::%3$s' = ANY(%4$s::TEXT[]) THEN
                  %1$s
                END IF;
              END;
            $BLOCK$
          $RETURN$,
          p_query, p_table_name, p_trigger_name, '%3$L'
        );
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL
  end

  def down
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.get_create_virtual_polymorphic_foreign_key_queries(
        IN p_referencing_table TEXT,
        IN p_referenced_table TEXT,
        IN p_column_mappings JSONB, -- { "referencing_col_a": "referenced_col_a", "referencing_col_b": "referenced_col_b", "referencing_col_c": null }
        IN p_template_fk_name TEXT DEFAULT NULL,
        IN p_update_condition "char" DEFAULT NULL, -- DEFAULTS TO NO ACTION
        IN p_delete_condition "char" DEFAULT NULL, -- DEFAULTS TO NO ACTION
        IN p_trigger_conditions JSONB DEFAULT NULL -- { "local_col_c": [ "value_a", "value_b" ] }
      )
      RETURNS TEXT[] AS $BODY$
      DECLARE
        aux_array TEXT[];
        queries TEXT[];

        all_local_columns TEXT[];
        referencing_columns TEXT[];
        referenced_columns TEXT[];
        trigger_condition_clause TEXT;
      BEGIN
        -- RAISE NOTICE 'sharding.get_create_virtual_polymorphic_foreign_key_queries(''%'', ''%'', ''%'', ''%'', ''%'', ''%'', ''%'')',
        -- p_referencing_table,
        -- p_referenced_table,
        -- p_column_mappings,
        -- p_template_fk_name,
        -- p_update_condition,
        -- p_delete_condition,
        -- p_trigger_conditions;

        -- Load the referencing columns from the JSON column mappings
        all_local_columns := (SELECT array_agg(k) FROM jsonb_object_keys(p_column_mappings) k);
        referencing_columns := (SELECT array_agg("key") FROM jsonb_each_text(p_column_mappings) WHERE "value" IS NOT NULL);
        referenced_columns := (SELECT array_agg("value") FROM jsonb_each_text(p_column_mappings) WHERE "value" IS NOT NULL);

        IF p_template_fk_name IS NULL THEN
          p_template_fk_name := format('%1$s_%2$s_%3$s_%4$s',
            regexp_replace(regexp_replace(p_referencing_table, '^(?:.*?\.)?(.*?)$', '\1'),'(.).*?(_|$)', '\1\2', 'g'),
            regexp_replace(array_to_string(referencing_columns, '_'), '(.).*?(_|$)', '\1\2', 'g'),
            regexp_replace(p_referenced_table, '^(?:.*?\.)?(.*?)$', '\1'),
            regexp_replace(array_to_string(referenced_columns, '_'), '(.).*?(_|$)', '\1\2', 'g')
          );
        END IF;

        trigger_condition_clause := array_to_string((
          SELECT array_agg('NEW.' || col_name || ' IN (''' || array_to_string(col_values, ''', ''') || ''')')
          FROM (
            SELECT col_name, array_agg(col_values) AS col_values
            FROM (SELECT "key" AS col_name, jsonb_array_elements_text("value"::JSONB) AS "col_values" FROM (SELECT * FROM jsonb_each_text(p_trigger_conditions)) x) y
            GROUP BY col_name
          ) z),
          ' AND '
        );

        aux_array := ARRAY[
          array_to_string(referencing_columns, ', '),                                                                                         -- 1
          p_referenced_table,                                                                                                                 -- 2
          array_to_string(referenced_columns, ', '),                                                                                          -- 3
          '{' || array_to_string(referencing_columns, ', ') || '}',                                                                           -- 4
          array_to_string(all_local_columns, ', '),                                                                                           -- 5
          '{' || array_to_string(referenced_columns, ', ') || '}',                                                                            -- 6
          p_referencing_table,                                                                                                                -- 7
          regexp_replace(p_referencing_table, '^(?:.*?\.)?(.*?)$', '\1'),                                                                     -- 8
          p_template_fk_name,                                                                                                                 -- 9
          array_to_string((SELECT array_agg('NEW.' || f || ' IS NOT NULL') FROM unnest(all_local_columns) as f), ' AND '),                    -- 10
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(referenced_columns) as f), ' OR '),  -- 11
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(all_local_columns) as f), ' OR '),   -- 12
          array_to_string((SELECT array_agg('OLD.' || f || ' IS NOT NULL') FROM unnest(referenced_columns) as f), ' AND '),                   -- 13
          trigger_condition_clause,                                                                                                           -- 14
          regexp_replace(trigger_condition_clause, 'NEW\.', '', 'g'),                                                                         -- 15
          p_trigger_conditions::TEXT,                                                                                                         -- 16
          substring(format('trg_vfkpr_au_c_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                         -- 17
          substring(format('trg_vfkpr_au_sn_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 18
          substring(format('trg_vfkpr_au_sd_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 19
          substring(format('trg_vfkpr_bu_r_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                         -- 20
          substring(format('trg_vfkpr_au_na_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 21
          substring(format('trg_vfkpr_ad_c_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                         -- 22
          substring(format('trg_vfkpr_ad_sn_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 23
          substring(format('trg_vfkpr_ad_sd_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 24
          substring(format('trg_vfkpr_ad_r_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                         -- 25
          substring(format('trg_vfkpr_ad_na_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 26
          substring(format('trg_vfkp_bi_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 27
          substring(format('trg_vfkp_bu_%1$s', p_template_fk_name) FROM 1 FOR 63)                                                             -- 28
        ];

        -- Create before insert trigger
        queries := queries || format($$
          CREATE TRIGGER %27$I
            BEFORE INSERT ON %7$s
            FOR EACH ROW
              WHEN (%14$s AND %10$s)
            EXECUTE PROCEDURE sharding.trf_virtual_fk_before_insert_or_update('%4$s', '{%2$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before update trigger
        queries := queries || format($$
          CREATE TRIGGER %28$I
            BEFORE UPDATE OF %5$s ON %7$s
            FOR EACH ROW
              WHEN ((%12$s) AND %14$s AND %10$s)
            EXECUTE PROCEDURE sharding.trf_virtual_fk_before_insert_or_update('%4$s', '{%2$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before delete or update triggers on referenced table
        -- Check the ON UPDATE clause of the foreign key
        CASE p_update_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after update trigger on the referenced table to cascade the change to any referencing field
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %17$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_cascade('%4$s', '%7$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ));

          WHEN 'n' THEN -- SET NULL
            -- Create the after update trigger on the referenced table to set any referencing records to NULL
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %18$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_null('%4$s', '%7$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ));

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after update trigger on the referenced table to set any referencing records to their default value
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %19$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_default('%4$s', '%7$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ));

          WHEN 'r' THEN -- RESTRICT
            -- Create the before update trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %20$I
                 BEFORE UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%7$s', '%6$s', %16$L);
              $$,
              VARIADIC aux_array
            ));

          ELSE -- If NULL, default to NO ACTION
            -- Create the after update trigger on the referenced table to prevent changing the key fields is they're being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT CONSTRAINT triggers that are deferred
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %21$I
                 BEFORE UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%7$s', '%6$s', %16$L);
              $$,
              VARIADIC aux_array
            ));
        END CASE;

        -- Check the ON DELETE clause of the foreign key
        CASE p_delete_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after delete trigger on the referenced table to cascade the deletion to referenced rows
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %22$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_cascade('%4$s', '%7$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ));

          WHEN 'n' THEN -- SET NULL
            -- Create the after delete trigger on the referenced table to set any referencing records to NULL
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %23$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_null('%4$s', '%7$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ));

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after delete trigger on the referenced table to set any referencing records to their default value
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %24$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_default('%4$s', '%7$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ));

          WHEN 'r' THEN -- RESTRICT
            -- Create the before delete trigger on the referenced table to prevent deleting the row if the key fields are being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %25$I
                 BEFORE DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%7$s', '%6$s', %16$L);
              $$,
              VARIADIC aux_array
            ));

          ELSE -- If NULL, default to NO ACTION
            -- Create the before delete trigger on the referenced table to prevent changing the key fields is they're being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT triggers
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %26$I
                 BEFORE DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%7$s', '%6$s', %16$L);
              $$,
              VARIADIC aux_array
            ));
        END CASE;

        RETURN queries;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.get_create_virtual_foreign_key_to_inherited_table_queries(
        IN p_referencing_table TEXT, -- p_destination_schema_name.p_template_table_name
        IN p_parent_referenced_table TEXT,
        IN p_child_referenced_table TEXT,
        IN p_referencing_columns TEXT[], -- aux_array[1]
        IN p_referenced_columns TEXT[], -- aux_array[3]
        IN p_template_fk_name TEXT DEFAULT NULL, -- catalog_info.conname
        IN p_update_condition "char" DEFAULT NULL,
        IN p_delete_condition "char" DEFAULT NULL
      )
      RETURNS TEXT[] AS $BODY$
      DECLARE
        aux_array TEXT[];
        queries TEXT[];
      BEGIN
        -- RAISE NOTICE 'sharding.get_create_virtual_foreign_key_to_inherited_table_queries(''%'', ''%'', ''%'', ''%'', ''%'', ''%'', ''%'', ''%'');',
        --   p_referencing_table,
        --   p_parent_referenced_table,
        --   p_child_referenced_table,
        --   p_referencing_columns,
        --   p_referenced_columns,
        --   p_template_fk_name,
        --   p_update_condition,
        --   p_delete_condition;

        IF p_template_fk_name IS NULL THEN
          p_template_fk_name := format('%1$s_%2$s_%3$s',
            regexp_replace(array_to_string(p_referencing_columns, '_'), '(.).*?(_|$)', '\1\2', 'g'),
            regexp_replace(p_referenced_table, '^(?:.*?\.)?(.*?)$', '\1'),
            regexp_replace(array_to_string(p_referenced_columns, '_'), '(.).*?(_|$)', '\1\2', 'g')
          );
        END IF;

        aux_array := ARRAY[
          array_to_string(p_referencing_columns, ', '),                                                                                           -- 1
          p_parent_referenced_table,                                                                                                              -- 2
          array_to_string(p_referenced_columns, ', '),                                                                                            -- 3
          '{' || array_to_string(p_referencing_columns, ', ') || '}',                                                                             -- 4
          p_child_referenced_table,                                                                                                               -- 5
          '{' || array_to_string(p_referenced_columns, ', ') || '}',                                                                              -- 6
          p_referencing_table,                                                                                                                    -- 7
          regexp_replace(p_referencing_table, '^(?:.*?\.)?(.*?)$', '\1'),                                                                         -- 8
          p_template_fk_name,                                                                                                                     -- 9
          array_to_string((SELECT array_agg('NEW.' || f || ' IS NOT NULL') FROM unnest(p_referencing_columns) as f), ' AND '),                    -- 10
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(p_referenced_columns) as f), ' OR '),    -- 11
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(p_referencing_columns) as f), ' OR '),   -- 12
          array_to_string((SELECT array_agg('OLD.' || f || ' IS NOT NULL') FROM unnest(p_referenced_columns) as f), ' AND '),                     -- 13
          substring(format('trg_vfkir_au_c_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                             -- 14
          substring(format('trg_vfkir_au_sn_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 15
          substring(format('trg_vfkir_au_sd_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 16
          substring(format('trg_vfkir_bu_r_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                             -- 17
          substring(format('trg_vfkir_au_na_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 18
          substring(format('trg_vfkir_ad_c_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                             -- 19
          substring(format('trg_vfkir_ad_sn_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 20
          substring(format('trg_vfkir_ad_sd_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 21
          substring(format('trg_vfkir_ad_r_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                             -- 22
          substring(format('trg_vfkir_ad_na_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 23
          substring(format('trg_vfki_bi_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                                -- 25
          substring(format('trg_vfki_bu_%1$s', p_template_fk_name) FROM 1 FOR 63)                                                                 -- 25
        ];

        -- Create before insert trigger
        queries := queries || format($$
          CREATE TRIGGER %24$I
            BEFORE INSERT ON %7$s
            FOR EACH ROW
              WHEN (%10$s)
            EXECUTE PROCEDURE sharding.trf_virtual_fk_before_insert_or_update('%4$s', '{%2$s, %5$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before update trigger
        queries := queries || format($$
          CREATE TRIGGER %25$I
            BEFORE UPDATE OF %1$s ON %7$s
            FOR EACH ROW
              WHEN ((%12$s) AND %10$s)
            EXECUTE PROCEDURE sharding.trf_virtual_fk_before_insert_or_update('%4$s', '{%2$s, %5$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before delete or update triggers on referenced table
        -- Check the ON UPDATE clause of the foreign key
        CASE p_update_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after update trigger on the parent referenced table to cascade the update to the referencing fields
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %14$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_cascade('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the after update trigger on the child referenced table to cascade the update to the referencing fields
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %14$I
                 AFTER UPDATE OF %3$s ON %5$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_cascade('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'n' THEN -- SET NULL
            -- Create the after update trigger on the parent referenced table to set the referencing fields to null
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %15$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_null('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the after update trigger on the child referenced table to set the referencing fields to null
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %15$I
                 AFTER UPDATE OF %3$s ON %5$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_null('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after update trigger on the parent referenced table to set the referencing fields to their default values
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %16$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_default('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the after update trigger on the child referenced table to set the referencing fields to their default values
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %16$I
                 AFTER UPDATE OF %3$s ON %5$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_default('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'r' THEN -- RESTRICT
            -- Create the before update trigger on the parent referenced table to prevent changing the key fields if they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %17$I
                 BEFORE UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the before update trigger on the child referenced table to prevent changing the key fields if they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %17$I
                 BEFORE UPDATE OF %3$s ON %5$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          ELSE -- If NULL, default to NO ACTION
            -- Create the before update trigger on the parent referenced table to prevent changing the key fields if they're being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT triggers
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %18$I
                 BEFORE UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the before update trigger on the child referenced table to prevent changing the key fields if they're being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT triggers
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %18$I
                 BEFORE UPDATE OF %3$s ON %5$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));
        END CASE;

        -- Check the ON DELETE clause of the foreign key
        CASE p_delete_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after delete trigger on the referenced table to delete the rows referencing the deleted row
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %19$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_cascade('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the after delete trigger on the referenced table to delete the rows referencing the deleted row
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %19$I
                 AFTER DELETE ON %5$s
                 FOR EACH ROW
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_cascade('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'n' THEN -- SET NULL
            -- Create the after delete trigger on the parent referenced table set the referencing fields to null
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %20$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_null('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the after delete trigger on the child referenced table set the referencing fields to null
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %20$I
                 AFTER DELETE ON %5$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_null('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after delete trigger on the parent referenced table to set the referencing fields to their default values
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %21$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_default('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the after delete trigger on the child referenced table to set the referencing fields to their default values
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %21$I
                 AFTER DELETE ON %5$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_default('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'r' THEN -- RESTRICT
            -- Create the before delete trigger on the parent referenced table to prevent deleting the row if it's being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %22$I
                 BEFORE DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the before delete trigger on the child referenced table to prevent deleting the row if it's being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %22$I
                 BEFORE DELETE ON %5$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          ELSE -- If NULL, default to NO ACTION
            -- Create the before delete trigger on the parent referenced table to prevent deleting the row if it's being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT triggers
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %23$I
                 BEFORE DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the before delete trigger on the child referenced table to prevent deleting the row if it's being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT triggers
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %23$I
                 BEFORE DELETE ON %5$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

        END CASE;

        RETURN queries;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.get_create_virtual_foreign_key_queries(
        IN p_referencing_table TEXT,
        IN p_referencing_columns TEXT[],
        IN p_referenced_table TEXT,
        IN p_referenced_columns TEXT[],
        IN p_template_fk_name TEXT DEFAULT NULL,
        IN p_update_condition "char" DEFAULT NULL,
        IN p_delete_condition "char" DEFAULT NULL
      )
      RETURNS TEXT[] AS $BODY$
      DECLARE
        aux_array TEXT[];
        queries TEXT[];
        referencing_schema TEXT;
      BEGIN
        -- RAISE NOTICE 'sharding.get_create_virtual_foreign_key_queries(''%'', ''%'', ''%'', ''%'', ''%'', ''%'', ''%'')',
        --   p_referencing_table,
        --   p_referencing_columns,
        --   p_referenced_table,
        --   p_referenced_columns,
        --   p_template_fk_name,
        --   p_update_condition,
        --   p_delete_condition;

        IF p_template_fk_name IS NULL THEN
          p_template_fk_name := format('%1$s_%2$s_%3$s',
            regexp_replace(array_to_string(p_referencing_columns, '_'), '(.).*?(_|$)', '\1\2', 'g'),
            regexp_replace(p_referenced_table, '^(?:.*?\.)?(.*?)$', '\1'),
            regexp_replace(array_to_string(p_referenced_columns, '_'), '(.).*?(_|$)', '\1\2', 'g')
          );
        END IF;

        referencing_schema := regexp_replace(p_referencing_table, '^(?:(.+?)\.)?.*?$', '\1');

        aux_array := ARRAY[
          array_to_string(p_referencing_columns, ', '),                                                                                                    -- 1
          p_referenced_table,                                                                                                                              -- 2
          array_to_string(p_referenced_columns, ', '),                                                                                                     -- 3
          '{' || array_to_string(p_referencing_columns, ', ') || '}',                                                                                      -- 4
          p_referenced_table,                                                                                                                              -- 5
          '{' || array_to_string(p_referenced_columns, ', ') || '}',                                                                                       -- 6
          p_referencing_table,                                                                                                                             -- 7
          regexp_replace(p_referencing_table, '^(?:.*?\.)?(.*?)$', '\1'),                                                                                  -- 8
          p_template_fk_name,                                                                                                                              -- 9
          array_to_string((SELECT array_agg('NEW.' || f || ' IS NOT NULL') FROM unnest(p_referencing_columns) as f), ' AND '),                             -- 10
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(p_referenced_columns) as f), ' OR '),             -- 11
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(p_referencing_columns) as f), ' OR '),            -- 12
          array_to_string((SELECT array_agg('OLD.' || f || ' IS NOT NULL') FROM unnest(p_referenced_columns) as f), ' AND '),                              -- 13
          substring(format('trg_v%2$sfkr_au_c_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),     -- 14
          substring(format('trg_v%2$sfkr_au_sn_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 15
          substring(format('trg_v%2$sfkr_au_sd_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 16
          substring(format('trg_v%2$sfkr_bu_r_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),     -- 17
          substring(format('trg_v%2$sfkr_au_na_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 18
          substring(format('trg_v%2$sfkr_ad_c_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),     -- 19
          substring(format('trg_v%2$sfkr_ad_sn_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 20
          substring(format('trg_v%2$sfkr_ad_sd_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 21
          substring(format('trg_v%2$sfkr_ad_r_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),     -- 22
          substring(format('trg_v%2$sfkr_ad_na_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 23
          substring(format('trg_v%2$sfk_bi_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),        -- 24
          substring(format('trg_v%2$sfk_bu_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),        -- 25
          CASE WHEN referencing_schema = 'public' THEN 'trf_virtual_public_fk_before_insert_or_update' ELSE 'trf_virtual_fk_before_insert_or_update' END,  -- 26
          CASE WHEN referencing_schema = 'public' THEN p_referencing_table ELSE regexp_replace(p_referencing_table, '^(?:.*?\.)?(.*?)$', '\1') END         -- 27
        ];

        -- Create before insert trigger
        queries := queries || format($$
          CREATE TRIGGER %24$I
            BEFORE INSERT ON %7$s
            FOR EACH ROW
              WHEN (%10$s)
            EXECUTE PROCEDURE sharding.%26$s('%4$s', '{%2$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before update trigger
        queries := queries || format($$
          CREATE TRIGGER %25$I
            BEFORE UPDATE OF %1$s ON %7$s
            FOR EACH ROW
              WHEN ((%12$s) AND %10$s)
            EXECUTE PROCEDURE sharding.%26$s('%4$s', '{%2$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before delete or update triggers on referenced table
        -- Check the ON UPDATE clause of the foreign key
        CASE p_update_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after update trigger on the referenced table to cascade the change to any referencing field
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %14$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_cascade('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'n' THEN -- SET NULL
            -- Create the after update trigger on the referenced table to set any referencing fields to NULL
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %15$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_null('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after update trigger on the referenced table to set any referencing fields to their default value
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %16$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_default('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'r' THEN -- RESTRICT
            -- Create the before update trigger on the referenced table to prevent changing the key fields if they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %17$I
                 BEFORE UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          ELSE -- If NULL, default to NO ACTION
            -- Create the before update trigger on the referenced table to prevent changing the key fields if they're being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT triggers
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %18$I
                 BEFORE UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));
        END CASE;

        -- Check the ON DELETE clause of the foreign key
        CASE p_delete_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after update trigger on the referenced table to cascade the deletion to any referencing record
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %19$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_cascade('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'n' THEN -- SET NULL
            -- Create the after delete trigger on the referenced table to set any referencing fields to NULL
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %20$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_null('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after delete trigger on the referenced table to set any referencing fields to their default values
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %21$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_default('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'r' THEN -- RESTRICT
            -- Create the before delete trigger on the referenced table to prevent deleting the record if the key fields are being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %22$I
                 BEFORE DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          ELSE -- If NULL, default to NO ACTION
            -- Create the before delete trigger on the referenced table to prevent deleting the record if the key fields are being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT triggers
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %23$I
                 BEFORE DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));
        END CASE;

        RETURN queries;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.generate_create_company_shard_function(
        IN p_use_original_sequence BOOLEAN DEFAULT TRUE
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        auxiliary_table_information JSONB;

        all_objects_data JSONB;
        object_data JSON;
        qualified_object_name TEXT;
        schema_name TEXT;
        object_name TEXT;
        aux TEXT;
        aux_array TEXT[];
        col_default_value TEXT;

        json_object JSON;

        queries TEXT[];
        query TEXT;
        before_query TEXT;
        after_queries TEXT[];
        p_destination_schema_name TEXT;
        shard_company_id TEXT;
        original_search_path TEXT;
      BEGIN
        SHOW search_path INTO original_search_path;
        SET search_path TO '';

        p_destination_schema_name := '%1$I';
        shard_company_id := '%2$L';

        -- Let's grant that we will use original sequences (temporary hack)
        p_use_original_sequence := TRUE;

        auxiliary_table_information = sharding.get_auxiliary_table_information();

        queries := queries || format($$
          INSERT INTO sharding.sharding_statistics (sharding_key, structure_sharding_started_at) VALUES (%1$s, clock_timestamp())
        $$, shard_company_id);

        -------------------------------------------------------------------------------------------------------------
        -- Invoke the sharding.get_queries_to_run_before_sharding_company_structure function if set by the project --
        -------------------------------------------------------------------------------------------------------------

        IF common.function_exists('sharding.get_queries_to_run_before_sharding_company_structure') THEN
          queries := queries || (SELECT sharding.get_queries_to_run_before_sharding_company_structure(auxiliary_table_information));
        END IF;


        -- Get the necessary data to create the new tables, indexes, stored procedures and triggers
        WITH table_columns AS (
          SELECT
            t.tablename AS object_name,
            format('%1$I.%2$I', t.schemaname, t.tablename) AS qualified_object_name,
            (t.schemaname || '.' || t.tablename)::regclass::oid AS table_oid,
            json_agg(json_build_object(
              'name', a.attname,
              'type', pg_catalog.format_type(a.atttypid, a.atttypmod),
              'default_value', (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
              'is_not_null', a.attnotnull
            ) ORDER BY a.attnum)::JSONB AS columns
          FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_tables t ON c.oid = (t.schemaname || '.' || t.tablename)::regclass::oid
          WHERE a.attnum > 0
            AND NOT a.attisdropped
            AND n.nspname = 'public'
          GROUP BY t.schemaname, t.tablename
        ),
        table_indexes AS (
          SELECT
            format('%1$I.%2$I', t.schemaname, t.tablename) AS qualified_object_name,
            (t.schemaname || '.' || t.tablename)::regclass::oid AS table_oid,
            json_agg(json_build_object(
              'name', c2.relname,
              'is_primary', i.indisprimary,
              'is_unique', i.indisunique,
              'definition', pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
              'constraint_definition', pg_catalog.pg_get_constraintdef(con.oid, true)
            )::JSONB) AS indexes
          FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_index i ON c.oid = i.indrelid
            JOIN pg_catalog.pg_class c2 ON i.indexrelid = c2.oid
            LEFT JOIN pg_catalog.pg_constraint con ON (con.conrelid = i.indrelid AND con.conindid = i.indexrelid AND con.contype IN ('p','u','x'))
            JOIN pg_catalog.pg_tables t ON c.oid = (t.schemaname || '.' || t.tablename)::regclass::oid
          WHERE t.schemaname = 'public'
          GROUP BY t.schemaname, t.tablename
        ),
        table_foreign_keys AS (
          SELECT
            format('%1$I.%2$I', t.schemaname, t.tablename) AS qualified_object_name,
            (t.schemaname || '.' || t.tablename)::regclass::oid AS table_oid,
            json_agg(json_build_object(
              'name', c.conname,
              'update_action', c.confupdtype,
              'delete_action', c.confdeltype,
              'definition', pg_catalog.pg_get_constraintdef(c.oid, true)
            )::JSONB) AS foreign_keys
          FROM pg_catalog.pg_constraint c
            LEFT JOIN pg_catalog.pg_tables t ON c.conrelid = (t.schemaname || '.' || t.tablename)::regclass::oid
          WHERE c.contype = 'f'
            AND t.schemaname = 'public'
          GROUP BY t.schemaname, t.tablename
        ),
        table_triggers AS (
          SELECT
            format('%1$I.%2$I', ta.schemaname, ta.tablename) AS qualified_object_name,
            (ta.schemaname || '.' || ta.tablename)::regclass::oid AS table_oid,
            json_agg(json_build_object(
              'name', t.tgname,
              'definition', pg_catalog.pg_get_triggerdef(t.oid, true)
            )::JSONB) AS triggers
          FROM pg_catalog.pg_trigger t
            LEFT JOIN pg_catalog.pg_tables ta ON t.tgrelid = (ta.schemaname || '.' || ta.tablename)::regclass::oid
          WHERE ta.schemaname = 'public'
            AND (NOT t.tgisinternal OR (t.tgisinternal AND t.tgenabled = 'D'))
            AND t.tgname != 'trg_prevent_insert_or_update_on_sharded_companies' -- Don't copy the prevent trigger for sharded companies
            AND t.tgname !~* '^trg_vfk(?:i|p)?' -- Don't copy the virtual foreign key triggers
          GROUP BY ta.schemaname, ta.tablename
        )
        SELECT
          json_object_agg(c.qualified_object_name,
            json_build_object(
              'columns', c.columns,
              'indexes', i.indexes,
              'foreign_keys', fk.foreign_keys,
              'triggers', trg.triggers
            )
          )::JSONB INTO all_objects_data
        FROM table_columns c
          LEFT JOIN table_indexes i ON c.table_oid = i.table_oid
          LEFT JOIN table_foreign_keys fk ON c.table_oid = fk.table_oid
          LEFT JOIN table_triggers trg ON c.table_oid = trg.table_oid
        WHERE c.object_name::TEXT NOT IN (
          SELECT jsonb_array_elements_text FROM jsonb_array_elements_text(auxiliary_table_information->'unsharded_tables')
        );

        ----------------------
        -- Build the tables --
        ----------------------

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP
          -- Reset variables
          aux := NULL;
          before_query := NULL;
          after_queries := '{}';

          object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');
          -- RAISE DEBUG 'object_name: %', object_name;

          queries := queries || format('RAISE DEBUG ''-- [TABLES] TABLE: %1$I'';', object_name);

          query := format('CREATE TABLE %1$s.%2$I (', p_destination_schema_name, object_name);

          FOR json_object IN SELECT * FROM json_array_elements(object_data->'columns') LOOP
            -- RAISE DEBUG 'column: %', json_object;
            col_default_value := NULL;

            IF NOT p_use_original_sequence AND (json_object->>'default_value') IS NOT NULL AND json_object->>'default_value' ~ '^nextval\('
               -- we cannot use specific sequence on inherited tables (otherwise values will collide on parent table)
               AND NOT auxiliary_table_information->'inherited_tables' ? object_name THEN
              -- Need to create a new sequence for the primary key
              aux := regexp_replace(json_object->>'default_value', 'nextval\(''(?:.+\.)?(.*)''.*', '\1');

              col_default_value := format('nextval(''%1$s.%2$s''::regclass)', p_destination_schema_name, aux);

              before_query := format('CREATE SEQUENCE %1$s.%2$I;', p_destination_schema_name, aux);
              after_queries := after_queries
                            || format('ALTER SEQUENCE %1$s.%2$I OWNED BY %1$s.%3$I.%4$I;', p_destination_schema_name, aux, object_name, json_object->>'name')
                            || format('EXECUTE ''SELECT last_value FROM public.%1$I'' INTO seq_nextval;', aux)
                            || format('EXECUTE format(''ALTER SEQUENCE %%1$s.%1$I RESTART WITH %%2$s'', p_company_schema_name, seq_nextval);', aux);
            END IF;

            IF col_default_value IS NULL THEN
              col_default_value := json_object->>'default_value';
            END IF;

            query := query || format('%1$I %2$s%3$s%4$s%5$s, ',
              json_object->>'name',
              json_object->>'type',
              CASE WHEN (json_object->>'is_not_null')::BOOLEAN THEN ' NOT NULL' END,
              CASE WHEN col_default_value IS NOT NULL THEN format(' DEFAULT %1$s', col_default_value) END,
              CASE WHEN json_object->>'name' = 'company_id' THEN format(' CONSTRAINT "company_id must equal %1$s" CHECK (company_id = %1$s)', shard_company_id) END
            );

          END LOOP;

          query := LEFT(query, length(query) - 2);

          if auxiliary_table_information->'inherited_tables' ? object_name THEN
            query := query || format(') INHERITS (%1$s);', qualified_object_name);
          ELSE
            query := query || ');';
          END IF;

          IF before_query IS NOT NULL THEN
            queries := queries || before_query;
          END IF;

          queries := queries || query || after_queries;

          -- raise DEBUG 'query: %', query;
        END LOOP;

        -----------------------
        -- Build the indexes --
        -----------------------

        queries := queries || '{ -- Create indexes }'::TEXT[];

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP
          -- Reset variables
          aux := NULL;

          object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');
          -- RAISE DEBUG 'object_name: %', object_name;

          queries := queries || format('RAISE DEBUG ''-- [INDEXES] TABLE: %1$I'';', object_name);

          IF (object_data->>'indexes') IS NOT NULL THEN
            FOR json_object IN SELECT * FROM json_array_elements(object_data->'indexes') LOOP
              queries := queries || format('%1$s;', regexp_replace(json_object->>'definition', ' ON (?:.+\.)?', format(' ON %1$s.', p_destination_schema_name)));

              IF (json_object->>'is_primary')::BOOLEAN THEN
                queries := queries || format('ALTER TABLE %1$s.%2$I ADD CONSTRAINT %4$I PRIMARY KEY USING INDEX %3$I;', p_destination_schema_name, object_name, json_object->>'name', format('%1$s_pkey', object_name));
              END IF;
            END LOOP;
          END IF;
        END LOOP;

        ----------------------------
        -- Build the foreign keys --
        ----------------------------

        queries := queries || '{ -- Create foreign keys }'::TEXT[];

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP
          -- Reset variables
          aux := NULL;

          object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');
          schema_name := COALESCE(regexp_replace(qualified_object_name, '^(?:(.+)\.)?(?:.*)$', '\1'), 'public');
          -- RAISE DEBUG 'object_name: %', object_name;

          queries := queries || format('RAISE DEBUG ''-- [FOREIGN KEYS] TABLE: %1$I'';', object_name);

          IF (object_data->>'foreign_keys') IS NOT NULL THEN
            RAISE DEBUG '% foreign_keys: %', object_name, object_data->'foreign_keys';

            FOR json_object IN SELECT * FROM json_array_elements(object_data->'foreign_keys') LOOP

              -- Get the name of the referenced table
              aux := (regexp_matches(json_object->>'definition', 'REFERENCES (?:.*?\.)?(.*?)\('))[1];

              -- If the referenced table is in the unsharded tables list, we need to create some 'virtual' foreign keys via triggers
              IF auxiliary_table_information->'unsharded_tables' ? aux THEN
                -- aux_array[1] = local columns
                -- aux_array[2] = referenced table
                -- aux_array[3] = referenced columns
                aux_array := regexp_matches(json_object->>'definition', 'FOREIGN KEY \((.*?)\) REFERENCES ((?:.*?\.)?.*?)\((.*?)\)');

                queries := queries || sharding.get_create_virtual_foreign_key_queries(
                  format('%1$s.%2$I', p_destination_schema_name, object_name),
                  regexp_split_to_array(aux_array[1], '\s*,\s*')::TEXT[],
                  aux_array[2],
                  regexp_split_to_array(aux_array[3], '\s*,\s*')::TEXT[],
                  json_object->>'name',
                  (json_object->>'update_action')::"char",
                  (json_object->>'delete_action')::"char"
                );
              -- If the referenced table is in the inherited tables list, we need to create some 'virtual' foreign keys via triggers
              -- to both the parent and the child table
              ELSIF auxiliary_table_information->'inherited_tables' ? aux THEN
                -- aux_array[1] = local columns
                -- aux_array[2] = referenced table
                -- aux_array[3] = referenced columns
                aux_array := regexp_matches(json_object->>'definition', 'FOREIGN KEY \((.*?)\) REFERENCES (?:.*?\.)?(.*?)\((.*?)\)');

                -- RAISE DEBUG 'aux_array: %', aux_array;

                queries := queries || sharding.get_create_virtual_foreign_key_to_inherited_table_queries(
                  format('%1$s.%2$I', p_destination_schema_name, object_name),
                  format('%1$s.%2$I', schema_name, aux_array[2]),
                  format('%1$s.%2$I', p_destination_schema_name, aux_array[2]),
                  regexp_split_to_array(aux_array[1], '\s*,\s*')::TEXT[],
                  regexp_split_to_array(aux_array[3], '\s*,\s*')::TEXT[],
                  json_object->>'name',
                  (json_object->>'update_action')::"char",
                  (json_object->>'delete_action')::"char"
                );
              ELSE
                -- It's a foreign key for the same shard, so we can replicate it
                queries := queries || ARRAY[format('ALTER TABLE %1$s.%2$I ADD CONSTRAINT %3$I %4$s;',
                  p_destination_schema_name,
                  object_name,
                  json_object->>'name',
                  regexp_replace(json_object->>'definition', 'REFERENCES (?:.*?\.)?', format('REFERENCES %1$s.', p_destination_schema_name))
                )];
              END IF;
            END LOOP;
          END IF;
        END LOOP;

        ------------------------
        -- Build the triggers --
        ------------------------

        queries := queries || '{ -- Create triggers }'::TEXT[];

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP
          -- Reset variables
          aux := NULL;

          object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');
          -- RAISE DEBUG 'object_name: %', object_name;

          queries := queries || format('RAISE DEBUG ''-- [TRIGGERS] TABLE: %1$I'';', object_name);

          IF (object_data->>'triggers') IS NOT NULL THEN
            FOR json_object IN SELECT * FROM json_array_elements(object_data->'triggers') LOOP
              -- Just replace the name of the table. The executed procedure will NOT be replicated, but should handle the different schemas
              queries := queries || regexp_replace(
                json_object->>'definition',
                ' ON (?:\S+?\.)?',
                format(' ON %1$s.', p_destination_schema_name)
              );
            END LOOP;
          END IF;
        END LOOP;

        ---------------------
        -- Build the views --
        ---------------------

        SELECT json_object(array_agg(dependent_view), array_agg(depends_on))::JSONB
          INTO all_objects_data
        FROM (
          SELECT
            dependent_view.relname::TEXT AS dependent_view,
            array_agg(source_view.relname)::TEXT AS depends_on
          FROM pg_depend
            JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
            JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
            JOIN pg_class as source_view ON pg_depend.refobjid = source_view.oid
            JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
            JOIN pg_namespace source_ns ON source_ns.oid = source_view.relnamespace
          WHERE source_ns.nspname = 'public'
            AND dependent_ns.nspname = 'public'
            AND source_view.relname != dependent_view.relname
            AND source_view.relkind = 'v'
          GROUP by dependent_view.relname
        ) views_dependencies;

        FOR qualified_object_name, aux IN
          SELECT
            format('%1$I.%2$I', v.schemaname, v.viewname),
            pg_catalog.pg_get_viewdef(c.oid)
          FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_views v ON c.oid = (v.schemaname || '.' || v.viewname)::regclass::oid
          WHERE n.nspname = 'public'
            AND NOT all_objects_data ? v.viewname
        LOOP
          object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');

          aux := regexp_replace(aux, 'public\.', '', 'g');

          queries := queries || format('CREATE VIEW %1$s.%2$I AS %3$s;',
            p_destination_schema_name,
            object_name,
            aux
          );
        END LOOP;

        FOR qualified_object_name, aux IN
          SELECT
            format('%1$I.%2$I', v.schemaname, v.viewname),
            pg_catalog.pg_get_viewdef(c.oid)
          FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_views v ON c.oid = (v.schemaname || '.' || v.viewname)::regclass::oid
          WHERE n.nspname = 'public'
            AND all_objects_data ? v.viewname
        LOOP
          object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');

          aux := regexp_replace(aux, 'public\.', '', 'g');

          queries := queries || format('CREATE VIEW %1$s.%2$I AS %3$s;',
            p_destination_schema_name,
            object_name,
            aux
          );

        END LOOP;

        ------------------------------------------------------------------------------------------------------------
        -- Invoke the sharding.get_queries_to_run_after_sharding_company_structure function if set by the project --
        ------------------------------------------------------------------------------------------------------------

        IF common.function_exists('sharding.get_queries_to_run_after_sharding_company_structure') THEN
          queries := queries || (SELECT sharding.get_queries_to_run_after_sharding_company_structure(auxiliary_table_information));
        END IF;

        queries := queries || format($$
          UPDATE sharding.sharding_statistics
          SET structure_sharding_ended_at = clock_timestamp()
          WHERE sharding_key = %1$s;
        $$, shard_company_id);

        --------------------------------
        -- Create the actual function --
        --------------------------------

        query := format($$
          CREATE OR REPLACE FUNCTION sharding.create_company_shard(
            IN p_company_id INTEGER,
            IN p_company_schema_name TEXT
          )
          RETURNS BOOLEAN AS $FUNCTION_BODY$
          DECLARE
            query TEXT;
            seq_nextval BIGINT;
            previous_search_path TEXT;
            spath TEXT;
            rec RECORD;
          BEGIN
            SHOW search_path INTO previous_search_path;
            EXECUTE 'SET search_path to ' || p_company_schema_name || ', public';
            SHOW search_path INTO spath;

            %1$s

            EXECUTE 'SET search_path to ' || previous_search_path;

            RETURN TRUE;
          END;
          $FUNCTION_BODY$ LANGUAGE 'plpgsql';
        $$,
          (
            SELECT string_agg(
              CASE WHEN unnest ~* '^(?:--|RAISE|EXECUTE|SHOW)'
              THEN format(E'\n      %1$s', unnest)
              ELSE format(E'EXECUTE format(%1$L, p_company_schema_name, p_company_id);', regexp_replace(unnest, '\s+', ' ', 'g'))
              -- Switch this with the previous one for debug
              -- ELSE format(E'query := format(%1$L, p_company_schema_name, p_company_id);\n      RAISE DEBUG ''query: %%'', query;\n      EXECUTE query;', regexp_replace(unnest, '\s+', ' ', 'g'))
              END, E'\n      '
            )
            FROM unnest(queries)
          )
        );

        RAISE DEBUG 'query: %', query;

        EXECUTE query;

        EXECUTE 'SET search_path TO ''' || original_search_path || '''';

        RETURN TRUE;
      -- EXCEPTION
      --   WHEN OTHERS THEN
      --     RETURN false;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute %Q[DROP FUNCTION IF EXISTS sharding.wrap_with_duplicate_check(TEXT, TEXT, TEXT);]
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.wrap_with_duplicate_check(
        IN p_query TEXT
      )
      RETURNS TEXT AS $BODY$
      DECLARE
      BEGIN
        RETURN format(
          $RETURN$
            DO $BLOCK$
              BEGIN
                %1$s
              EXCEPTION WHEN duplicate_object THEN
              END;
            $BLOCK$
          $RETURN$,
          p_query
        );
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL
  end
end