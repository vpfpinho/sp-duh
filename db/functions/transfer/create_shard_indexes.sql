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
  object_data             JSON;
  qualified_object_name   TEXT;
  object_name             TEXT;
  json_object             JSON;
  query                   TEXT;
  name                    TEXT;
BEGIN

  IF all_objects_data IS NULL THEN
    -- Get the necessary data to create the new indexes
    SELECT
      json_object_agg(i.qualified_object_name,
        json_build_object(
          'indexes', i.indexes
        )
      )::JSONB INTO all_objects_data
    FROM sharding.get_tables_info(template_schema_name, template_prefix) i;
  END IF;

  -----------------------
  -- Build the indexes --
  -----------------------

  FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP

    object_name := regexp_replace(qualified_object_name, '^(?:' || template_schema_name || '\.' || template_prefix || ')?(.*)$', '\1');

    RAISE DEBUG '-- [INDEXES] TABLE: %', object_name;

    IF (object_data->>'indexes') IS NOT NULL THEN
      FOR json_object IN SELECT * FROM json_array_elements(object_data->'indexes') LOOP

        query := regexp_replace(json_object->>'definition', ' ON ' || template_schema_name || '\.' || template_prefix, format(' ON %1$s.%2$s', schema_name, prefix));
        IF template_prefix <> '' THEN
          query := regexp_replace(query, template_prefix, prefix);
        END IF;

        -- RAISE DEBUG '%', query;
        EXECUTE query;

        IF (json_object->>'is_primary')::BOOLEAN THEN
          name := json_object->>'name';
          IF template_prefix <> '' THEN
            name := regexp_replace(name, template_prefix, prefix);
          END IF;
          query := format('ALTER TABLE %1$s.%5$s%2$I ADD CONSTRAINT %5$s%4$I PRIMARY KEY USING INDEX %3$I;', schema_name, object_name, name, format('%1$s_pkey', object_name), prefix);
          -- RAISE DEBUG '%', query;
          EXECUTE query;
        END IF;
      END LOOP;
    END IF;
  END LOOP;

  RETURN TRUE;
END;
$BODY$ LANGUAGE 'plpgsql';
