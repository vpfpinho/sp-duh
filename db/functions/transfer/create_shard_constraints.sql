DROP FUNCTION IF EXISTS transfer.create_shard_constraints(BIGINT, BIGINT, TEXT, TEXT, TEXT, TEXT, JSONB);
CREATE OR REPLACE FUNCTION transfer.create_shard_constraints(
  company_id              BIGINT,
  template_company_id     BIGINT,
  template_schema_name    TEXT,
  schema_name             TEXT,
  template_prefix         TEXT DEFAULT '',
  prefix                  TEXT DEFAULT '',
  all_objects_data        JSONB DEFAULT NULL
)
RETURNS BOOLEAN AS $BODY$
DECLARE
  object_data                         JSON;
  qualified_object_name               TEXT;
  object_name                         TEXT;
  json_object                         JSON;
  query                               TEXT;
  name                                TEXT;
  aux                                 TEXT;
  meta_schema                         text;
  source_info                         RECORD;
  main_schema_template                text;
  accounting_schema_template          text;
  fiscal_year_template                text;
  template_tax_registration_number    text;
BEGIN

  IF all_objects_data IS NULL THEN
    -- Get the necessary data to create the new constraints
    SELECT
      json_object_agg(i.qualified_object_name,
        json_build_object(
          'constraints', i.constraints
        )
      )::JSONB INTO all_objects_data
    FROM sharding.get_tables_info(template_schema_name, template_prefix) i;
  END IF;

  -- Get additional info
  EXECUTE
    FORMAT('SELECT * FROM transfer.get_meta_schema_name(%1$L)', company_id)
  INTO STRICT meta_schema;
  EXECUTE
    FORMAT('SELECT * FROM %1$s.info', meta_schema)
  INTO STRICT source_info;
  SELECT * FROM transfer.get_restore_templates(company_id, template_company_id)
  INTO main_schema_template, accounting_schema_template, fiscal_year_template, template_tax_registration_number;

  ---------------------------
  -- Build the constraints --
  ---------------------------

  FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP

    object_name := regexp_replace(qualified_object_name, '^(?:' || template_schema_name || '\.' || template_prefix || ')?(.*)$', '\1');

    RAISE DEBUG '-- [CONSTRAINTS] TABLE: %', object_name;

    IF (object_data->>'constraints') IS NOT NULL THEN

      FOR json_object IN SELECT * FROM json_array_elements(object_data->'constraints') LOOP

        aux := regexp_replace(json_object->>'definition', 'company_id\s*=\s*\d+', format('company_id = %1$s', company_id));
        aux := replace(aux, format('%1$s', template_tax_registration_number), format('%1$s', source_info.tax_registration_number));

        name := json_object->>'name';
        name := replace(name, format('''%1$s''', template_company_id), format('''%1$s''', company_id));
        IF template_prefix <> '' THEN
          name := regexp_replace(name, template_prefix, prefix);
        END IF;

        FOREACH query IN ARRAY ARRAY[format('ALTER TABLE %1$s.%5$s%2$I ADD CONSTRAINT %3$I %4$s;',
          schema_name,
          object_name,
          name,
          aux,
          prefix
        )]
        LOOP
          -- RAISE DEBUG '%', query;
          EXECUTE query;
        END LOOP;
      END LOOP;
    END IF;
  END LOOP;

  RETURN TRUE;
END;
$BODY$ LANGUAGE 'plpgsql';
