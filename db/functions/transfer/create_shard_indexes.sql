DROP FUNCTION IF EXISTS transfer.create_shard_indexes(TEXT, TEXT, TEXT, TEXT, JSONB);

CREATE OR REPLACE FUNCTION transfer.create_shard_indexes(
  template_schema_name    TEXT,
  schema_name             TEXT,
  template_prefix         TEXT DEFAULT '',
  prefix                  TEXT DEFAULT '',
  all_objects_data        JSONB DEFAULT NULL
)
RETURNS BOOLEAN AS $BODY$
DECLARE
  object_data JSON;
  qualified_object_name TEXT;
  object_name TEXT;
  json_object JSON;
  query TEXT;
  original_search_path TEXT;
BEGIN

  SHOW search_path INTO original_search_path;

  IF all_objects_data IS NULL THEN
    SET search_path TO '';

    -- Get the necessary data to create the new indexes
    SELECT
      json_object_agg(i.qualified_object_name,
        json_build_object(
          'indexes', i.indexes
        )
      )::JSONB INTO all_objects_data
    FROM sharding.get_tables_info(template_schema_name, template_prefix) i;
  END IF;

  EXECUTE 'SET search_path to ' || schema_name || ', public';

  -----------------------
  -- Build the indexes --
  -----------------------

  FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP

    object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');

    RAISE DEBUG '-- [INDEXES] TABLE: %', object_name;

    IF (object_data->>'indexes') IS NOT NULL THEN
      FOR json_object IN SELECT * FROM json_array_elements(object_data->'indexes') LOOP
        query := format('%1$s;', regexp_replace(json_object->>'definition', ' ON (?:.+\.)?', format(' ON %1$s.', schema_name)));
        -- RAISE DEBUG '%', query;
        EXECUTE query;

        IF (json_object->>'is_primary')::BOOLEAN THEN
          query := format('ALTER TABLE %1$s.%2$I ADD CONSTRAINT %4$I PRIMARY KEY USING INDEX %3$I;', schema_name, object_name, json_object->>'name', format('%1$s_pkey', object_name));
          -- RAISE DEBUG '%', query;
          EXECUTE query;
        END IF;
      END LOOP;
    END IF;
  END LOOP;

  EXECUTE 'SET search_path TO ''' || original_search_path || '''';

  RETURN TRUE;
END;
$BODY$ LANGUAGE 'plpgsql';
