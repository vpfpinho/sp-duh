DROP FUNCTION IF EXISTS transfer.create_shard_triggers(TEXT, TEXT, TEXT, TEXT);

CREATE OR REPLACE FUNCTION transfer.create_shard_triggers(
  template_schema_name    TEXT,
  schema_name             TEXT,
  template_prefix         TEXT DEFAULT '',
  prefix                  TEXT DEFAULT ''
)
RETURNS BOOLEAN AS $BODY$
DECLARE
  all_objects_data JSONB;
  object_data JSON;
  qualified_object_name TEXT;
  object_name TEXT;
  json_object JSON;
  query TEXT;
  original_search_path TEXT;
BEGIN

  SHOW search_path INTO original_search_path;
  SET search_path TO '';

  -- Get the necessary data to create the new triggers
  SELECT
    json_object_agg(i.qualified_object_name,
      json_build_object(
        'triggers', i.triggers
      )
    )::JSONB INTO all_objects_data
  FROM sharding.get_tables_info(template_schema_name, template_prefix) i;

  EXECUTE 'SET search_path to ' || schema_name || ', public';

  ------------------------
  -- Build the triggers --
  ------------------------

  FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP

    object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');

    RAISE DEBUG '-- [TRIGGERS] TABLE: %', object_name;

    IF (object_data->>'triggers') IS NOT NULL THEN
      FOR json_object IN SELECT * FROM json_array_elements(object_data->'triggers') LOOP
        query := regexp_replace(
          json_object->>'definition',
          ' ON (?:\S+?\.)?',
          format(' ON %1$s.', schema_name)
        );
        EXECUTE query;
      END LOOP;
    END IF;
  END LOOP;

  EXECUTE 'SET search_path TO ''' || original_search_path || '''';

  RETURN TRUE;
END;
$BODY$ LANGUAGE 'plpgsql';
