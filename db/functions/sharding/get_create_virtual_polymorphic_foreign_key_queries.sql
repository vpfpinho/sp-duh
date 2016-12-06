DROP FUNCTION IF EXISTS sharding.get_create_virtual_polymorphic_foreign_key_queries(TEXT, TEXT, JSONB, TEXT, "char", "char", JSONB);

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
      -- Create the after update trigger on the referenced table to prevent changing the key fields is they're being referenced
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
      -- Create the after update trigger on the referenced table to prevent changing the key fields is they're being referenced
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
      -- Create the after update trigger on the referenced table to prevent changing the key fields is they're being referenced
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
        $$CREATE CONSTRAINT TRIGGER %21$I
           AFTER UPDATE OF %3$s ON %2$s
           DEFERRABLE INITIALLY DEFERRED
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
      -- Create the after update trigger on the referenced table to prevent changing the key fields is they're being referenced
      queries := queries || sharding.wrap_with_duplicate_check(format(
        $$CREATE TRIGGER %22$I
           AFTER DELETE ON %2$s
           FOR EACH ROW
           EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_cascade('%4$s', '%7$s', '%6$s', %15$L);
        $$,
        VARIADIC aux_array
      ));

    WHEN 'n' THEN -- SET NULL
      -- Create the after delete trigger on the referenced table to prevent changing the key fields is they're being referenced
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
      -- Create the after delete trigger on the referenced table to prevent changing the key fields is they're being referenced
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
      -- Create the after delete trigger on the referenced table to prevent changing the key fields is they're being referenced
      queries := queries || sharding.wrap_with_duplicate_check(format(
        $$CREATE TRIGGER %25$I
           AFTER DELETE ON %2$s
           FOR EACH ROW
              WHEN (%13$s)
           EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%7$s', '%6$s', %16$L);
        $$,
        VARIADIC aux_array
      ));

    ELSE -- If NULL, default to NO ACTION
      -- Create the after delete trigger on the referenced table to prevent changing the key fields is they're being referenced
      -- NO ACTION foreign keys are implemented as RESTRICT CONSTRAINT triggers that are deferred
      queries := queries || sharding.wrap_with_duplicate_check(format(
        $$CREATE CONSTRAINT TRIGGER %26$I
           AFTER DELETE ON %2$s
           DEFERRABLE INITIALLY DEFERRED
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