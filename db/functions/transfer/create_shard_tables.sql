DROP FUNCTION IF EXISTS transfer.create_shard_tables(TEXT, TEXT, TEXT, TEXT);

CREATE OR REPLACE FUNCTION transfer.create_shard_tables(
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
  col_default_value TEXT;
  json_object JSON;
  query TEXT;
  original_search_path TEXT;
BEGIN

  SHOW search_path INTO original_search_path;
  SET search_path TO '';

  -- Get the necessary data to create the new tables
  SELECT
    json_object_agg(i.qualified_object_name,
      json_build_object(
        'columns', i.columns
      )
    )::JSONB INTO all_objects_data
  FROM sharding.get_tables_info(template_schema_name, template_prefix) i;

  EXECUTE 'SET search_path to ' || schema_name || ', public';

  ----------------------
  -- Build the tables --
  ----------------------

  FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP

    object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');

    RAISE DEBUG '-- [TABLES] TABLE: %', object_name;

    query := format('CREATE TABLE %3$s%1$s.%2$I (', schema_name, object_name, prefix);

    FOR json_object IN SELECT * FROM json_array_elements(object_data->'columns') LOOP
      col_default_value := json_object->>'default_value';
      query := query || format('%1$I %2$s%3$s%4$s, ',
        json_object->>'name',
        json_object->>'type',
        CASE WHEN (json_object->>'is_not_null')::BOOLEAN THEN ' NOT NULL' END,
        CASE WHEN col_default_value IS NOT NULL THEN format(' DEFAULT %1$s', col_default_value) END
      );

    END LOOP;

    query := LEFT(query, length(query) - 2) || ');';
    -- RAISE DEBUG '%', query;

    EXECUTE query;

  END LOOP;

  EXECUTE 'SET search_path TO ''' || original_search_path || '''';

  RETURN TRUE;
END;
$BODY$ LANGUAGE 'plpgsql';
