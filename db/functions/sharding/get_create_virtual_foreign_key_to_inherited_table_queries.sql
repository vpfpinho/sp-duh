DROP FUNCTION IF EXISTS sharding.get_create_virtual_foreign_key_to_inherited_table_queries(TEXT, TEXT, TEXT, TEXT[], TEXT[], TEXT, "char", "char");

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
      -- Create the after update trigger on the parent referenced table to prevent changing the key fields if they're being referenced
      -- NO ACTION foreign keys are implemented as RESTRICT CONSTRAINT triggers that are deferred
      queries := queries || sharding.wrap_with_duplicate_check(format(
        $$CREATE CONSTRAINT TRIGGER %18$I
           AFTER UPDATE OF %3$s ON %2$s
           DEFERRABLE INITIALLY DEFERRED
           FOR EACH ROW
              WHEN (%11$s)
           EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%8$s', '%6$s');
        $$,
        VARIADIC aux_array
      ));

      -- Create the after update trigger on the child referenced table to prevent changing the key fields if they're being referenced
      -- NO ACTION foreign keys are implemented as RESTRICT CONSTRAINT triggers that are deferred
      queries := queries || sharding.wrap_with_duplicate_check(format(
        $$CREATE CONSTRAINT TRIGGER %18$I
           AFTER UPDATE OF %3$s ON %5$s
           DEFERRABLE INITIALLY DEFERRED
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
      -- Create the after delete trigger on the parent referenced table to prevent deleting the rows if it's being referenced
      queries := queries || sharding.wrap_with_duplicate_check(format(
        $$CREATE TRIGGER %22$I
           AFTER DELETE ON %2$s
           FOR EACH ROW
              WHEN (%13$s)
           EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%8$s', '%6$s');
        $$,
        VARIADIC aux_array
      ));

      -- Create the after delete trigger on the child referenced table to prevent deleting the rows if it's being referenced
      queries := queries || sharding.wrap_with_duplicate_check(format(
        $$CREATE TRIGGER %22$I
           AFTER DELETE ON %5$s
           FOR EACH ROW
              WHEN (%13$s)
           EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%8$s', '%6$s');
        $$,
        VARIADIC aux_array
      ));

    ELSE -- If NULL, default to NO ACTION
      -- Create the after delete trigger on the parent referenced table to prevent deleting the rows if it's being referenced
      -- NO ACTION foreign keys are implemented as RESTRICT CONSTRAINT triggers that are deferred
      queries := queries || sharding.wrap_with_duplicate_check(format(
        $$CREATE CONSTRAINT TRIGGER %23$I
           AFTER DELETE ON %2$s
           DEFERRABLE INITIALLY DEFERRED
           FOR EACH ROW
              WHEN (%13$s)
           EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%8$s', '%6$s');
        $$,
        VARIADIC aux_array
      ));

      -- Create the after delete trigger on the child referenced table to prevent deleting the rows if it's being referenced
      -- NO ACTION foreign keys are implemented as RESTRICT CONSTRAINT triggers that are deferred
      queries := queries || sharding.wrap_with_duplicate_check(format(
        $$CREATE CONSTRAINT TRIGGER %23$I
           AFTER DELETE ON %5$s
           DEFERRABLE INITIALLY DEFERRED
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