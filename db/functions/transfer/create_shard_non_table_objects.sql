DROP FUNCTION IF EXISTS transfer.create_shard_non_table_objects(TEXT, TEXT, TEXT, TEXT);

CREATE OR REPLACE FUNCTION transfer.create_shard_non_table_objects(
  template_schema_name    TEXT,
  schema_name             TEXT,
  template_prefix         TEXT DEFAULT '',
  prefix                  TEXT DEFAULT ''
)
RETURNS BOOLEAN AS $BODY$
DECLARE
  all_objects_data JSONB;
  original_search_path TEXT;
BEGIN

  SHOW search_path INTO original_search_path;
  SET search_path TO '';

  -- Get the necessary data to create the new indexes
  SELECT
    json_object_agg(i.qualified_object_name,
      json_build_object(
        'indexes', i.indexes,
        'constraints', i.constraints,
        'foreign_keys', i.foreign_keys,
        'triggers', i.triggers
      )
    )::JSONB INTO all_objects_data
  FROM sharding.get_tables_info(template_schema_name, template_prefix) i;

  EXECUTE 'SET search_path to ' || schema_name || ', public';

  PERFORM transfer.create_shard_indexes(template_schema_name, schema_name, template_prefix, prefix, all_objects_data);
  PERFORM transfer.create_shard_constraints(template_schema_name, schema_name, template_prefix, prefix, all_objects_data);
  PERFORM transfer.create_shard_foreign_keys(template_schema_name, schema_name, template_prefix, prefix, all_objects_data);
  PERFORM transfer.create_shard_triggers(template_schema_name, schema_name, template_prefix, prefix, all_objects_data);

  EXECUTE 'SET search_path TO ''' || original_search_path || '''';

  RETURN TRUE;
END;
$BODY$ LANGUAGE 'plpgsql';
