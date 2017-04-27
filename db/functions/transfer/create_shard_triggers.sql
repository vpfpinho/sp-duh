DROP FUNCTION IF EXISTS transfer.create_shard_triggers(TEXT, TEXT, TEXT, TEXT, JSONB);
CREATE OR REPLACE FUNCTION transfer.create_shard_triggers(
  template_schema_name    TEXT,
  schema_name             TEXT,
  template_prefix         TEXT DEFAULT '',
  prefix                  TEXT DEFAULT '',
  all_objects_data        JSONB DEFAULT NULL
)
RETURNS BOOLEAN AS $BODY$
DECLARE
  object_data             JSON;
  qualified_object_name   TEXT;
  object_name             TEXT;
  json_object             JSON;
  query                   TEXT;
BEGIN

  IF all_objects_data IS NULL THEN
    -- Get the necessary data to create the new triggers
    SELECT
      json_object_agg(i.qualified_object_name,
        json_build_object(
          'triggers', i.triggers
        )
      )::JSONB INTO all_objects_data
    FROM sharding.get_tables_info(template_schema_name, template_prefix) i;
  END IF;

  ------------------------
  -- Build the triggers --
  ------------------------

  FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP

    object_name := regexp_replace(qualified_object_name, '^(?:' || template_schema_name || '\.' || template_prefix || ')?(.*)$', '\1');

    RAISE DEBUG '-- [TRIGGERS] TABLE: %', object_name;

    IF (object_data->>'triggers') IS NOT NULL THEN
      FOR json_object IN SELECT * FROM json_array_elements(object_data->'triggers') LOOP
        query := regexp_replace(
          json_object->>'definition',
          ' ON (?:' || template_schema_name || '\.' || template_prefix || ')?',
          format(' ON %1$s.%2$s', schema_name, prefix)
        );
        query := replace(
          query,
          template_schema_name || '.' || template_prefix,
          format('%1$s.%2$s', schema_name, prefix)
        );
        EXECUTE query;
      END LOOP;
    END IF;
  END LOOP;

  RETURN TRUE;
END;
$BODY$ LANGUAGE 'plpgsql';
