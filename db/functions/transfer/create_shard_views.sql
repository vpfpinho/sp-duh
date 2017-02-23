DROP FUNCTION IF EXISTS transfer.create_shard_views(TEXT, TEXT, TEXT, TEXT);

CREATE OR REPLACE FUNCTION transfer.create_shard_views(
  template_schema_name    TEXT,
  schema_name             TEXT,
  template_prefix         TEXT DEFAULT '',
  prefix                  TEXT DEFAULT ''
)
RETURNS BOOLEAN AS $BODY$
DECLARE
  object_data TEXT;
  object_name TEXT;
  query TEXT;
  original_search_path TEXT;
BEGIN

  SHOW search_path INTO original_search_path;
  EXECUTE 'SET search_path to ' || schema_name || ', public';

  ---------------------
  -- Build the views --
  ---------------------

  FOR object_name, object_data IN
    -- Get the necessary data to create the new views
    SELECT
      i.object_name,
      i.definition
    FROM sharding.get_views_info(template_schema_name, template_prefix) i
    ORDER BY
      i.independent DESC
  LOOP

    RAISE DEBUG '-- [VIEWS] VIEW: %', object_name;

    query := format('CREATE VIEW %1$s.%2$I AS %3$s;',
      schema_name,
      object_name,
      object_data
    );
    -- RAISE DEBUG '%', query;
    EXECUTE query;
  END LOOP;

  EXECUTE 'SET search_path TO ''' || original_search_path || '''';

  RETURN TRUE;
END;
$BODY$ LANGUAGE 'plpgsql';
