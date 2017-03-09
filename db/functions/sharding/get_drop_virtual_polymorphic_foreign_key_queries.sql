-- DROP FUNCTION IF EXISTS sharding.get_drop_virtual_polymorphic_foreign_key_queries(TEXT, TEXT, JSONB, TEXT);

CREATE OR REPLACE FUNCTION sharding.get_drop_virtual_polymorphic_foreign_key_queries(
  IN p_referencing_table TEXT,
  IN p_referenced_table TEXT,
  IN p_column_mappings JSONB, -- { "referencing_col_a": "referenced_col_a", "referencing_col_b": "referenced_col_b", "referencing_col_c": null }
  IN p_template_fk_name TEXT DEFAULT NULL
)
RETURNS TEXT[] AS $BODY$
DECLARE
  aux_array TEXT[];
  queries TEXT[];

  all_local_columns TEXT[];
  referencing_columns TEXT[];
  referenced_columns TEXT[];
BEGIN
  RAISE NOTICE 'sharding.get_drop_virtual_polymorphic_foreign_key_queries(''%'', ''%'', ''%'', ''%'')',
  p_referencing_table,
  p_referenced_table,
  p_column_mappings,
  p_template_fk_name;

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

  aux_array := ARRAY[
    p_referenced_table,                                                                                                                 -- 1
    p_referencing_table,                                                                                                                -- 2
    substring(format('trg_vfkpr_au_c_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                         -- 3
    substring(format('trg_vfkpr_au_sn_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 4
    substring(format('trg_vfkpr_au_sd_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 5
    substring(format('trg_vfkpr_bu_r_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                         -- 6
    substring(format('trg_vfkpr_au_na_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 7
    substring(format('trg_vfkpr_ad_c_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                         -- 8
    substring(format('trg_vfkpr_ad_sn_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 9
    substring(format('trg_vfkpr_ad_sd_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 10
    substring(format('trg_vfkpr_ad_r_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                         -- 11
    substring(format('trg_vfkpr_ad_na_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 12
    substring(format('trg_vfkp_bi_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 13
    substring(format('trg_vfkp_bu_%1$s', p_template_fk_name) FROM 1 FOR 63)                                                             -- 14
  ];

  -- Drop any existing after update and after delete triggers on the referenced table
  queries := queries || format('DROP TRIGGER IF EXISTS %3$I ON %1$s;', VARIADIC aux_array);
  queries := queries || format('DROP TRIGGER IF EXISTS %4$I ON %1$s;', VARIADIC aux_array);
  queries := queries || format('DROP TRIGGER IF EXISTS %5$I ON %1$s;', VARIADIC aux_array);
  queries := queries || format('DROP TRIGGER IF EXISTS %6$I ON %1$s;', VARIADIC aux_array);
  queries := queries || format('DROP TRIGGER IF EXISTS %7$I ON %1$s;', VARIADIC aux_array);
  queries := queries || format('DROP TRIGGER IF EXISTS %8$I ON %1$s;', VARIADIC aux_array);
  queries := queries || format('DROP TRIGGER IF EXISTS %9$I ON %1$s;', VARIADIC aux_array);
  queries := queries || format('DROP TRIGGER IF EXISTS %10$I ON %1$s;', VARIADIC aux_array);
  queries := queries || format('DROP TRIGGER IF EXISTS %11$I ON %1$s;', VARIADIC aux_array);
  queries := queries || format('DROP TRIGGER IF EXISTS %12$I ON %1$s;', VARIADIC aux_array);

  -- Drop the before insert and before update triggers on the referencing table
  queries := queries || format('DROP TRIGGER IF EXISTS %13$I ON %2$s;', VARIADIC aux_array);
  queries := queries || format('DROP TRIGGER IF EXISTS %14$I ON %2$s;', VARIADIC aux_array);

  RETURN queries;
END;
$BODY$ LANGUAGE plpgsql;