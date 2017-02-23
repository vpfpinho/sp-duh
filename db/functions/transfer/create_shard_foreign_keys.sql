DROP FUNCTION IF EXISTS transfer.create_shard_foreign_keys(TEXT, TEXT, TEXT, TEXT);

CREATE OR REPLACE FUNCTION transfer.create_shard_foreign_keys(
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

  -- Get the necessary data to create the new foreign keys
  SELECT
    json_object_agg(i.qualified_object_name,
      json_build_object(
        'foreign_keys', i.foreign_keys
      )
    )::JSONB INTO all_objects_data
  FROM sharding.get_tables_info(template_schema_name, template_prefix) i;

  EXECUTE 'SET search_path to ' || schema_name || ', public';

  ----------------------------
  -- Build the foreign keys --
  ----------------------------

  FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP

    object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');

    RAISE DEBUG '-- [FOREIGN KEYS] TABLE: %', object_name;

    IF (object_data->>'foreign_keys') IS NOT NULL THEN
      FOR json_object IN SELECT * FROM json_array_elements(object_data->'foreign_keys') LOOP
        -- It's a foreign key for the same shard, so we can replicate it
        FOREACH query IN ARRAY ARRAY[format('ALTER TABLE %1$s.%2$I ADD CONSTRAINT %3$I %4$s;',
          schema_name,
          object_name,
          json_object->>'name',
          regexp_replace(json_object->>'definition', 'REFERENCES (?:.*?\.)?', format('REFERENCES %1$s.', schema_name))
        )]
        LOOP
          -- RAISE DEBUG '%', query;
          EXECUTE query;
        END LOOP;
      END LOOP;
    END IF;
  END LOOP;

  EXECUTE 'SET search_path TO ''' || original_search_path || '''';

  RETURN TRUE;
END;
$BODY$ LANGUAGE 'plpgsql';
