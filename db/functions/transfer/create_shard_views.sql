DROP FUNCTION IF EXISTS transfer.create_shard_views(TEXT[], TEXT[], TEXT, TEXT, TEXT[]);

CREATE OR REPLACE FUNCTION transfer.create_shard_views(
  template_schema_names   TEXT[],
  schema_names            TEXT[],
  template_prefix         TEXT DEFAULT '',
  prefix                  TEXT DEFAULT '',
  excluded_prefixes       TEXT[] DEFAULT '{}'
)
RETURNS BOOLEAN AS $BODY$
DECLARE
  template_schema_name  TEXT;
  schema_name           TEXT;
  object_data           TEXT;
  object_name           TEXT;
  new_object_name       TEXT;
  query                 TEXT;
  original_search_path  TEXT;
  i                     INTEGER;
  excluded_prefix       TEXT;
BEGIN

  template_schema_name := template_schema_names[1];
  schema_name := schema_names[1];

  SHOW search_path INTO original_search_path;
  EXECUTE 'SET search_path to ' || schema_name || ', public';

  ---------------------
  -- Build the views --
  ---------------------

  -- Get the necessary data to create the new views
  query := FORMAT('
    SELECT
      i.object_name,
      i.definition
    FROM sharding.get_views_info(''%1$s'', ''%2$s'') i
    WHERE 1 = 1
  ', template_schema_name, template_prefix);
  FOREACH excluded_prefix IN ARRAY excluded_prefixes
  LOOP
    query := query || ' AND object_name NOT ILIKE ''' || excluded_prefix || '%''';
  END LOOP;
  query := query || '
    ORDER BY
      i.independent DESC
  ';

  FOR object_name, object_data IN EXECUTE query
  LOOP

    new_object_name := prefix || substring(object_name FROM length(template_prefix) + 1);
    RAISE DEBUG '-- [VIEWS] VIEW: % (-> %)', object_name, new_object_name;

    FOR i IN 1..cardinality(template_schema_names)
    LOOP
      object_data := regexp_replace(object_data, template_schema_names[i], schema_names[i], 'g');
    END LOOP;
    IF template_prefix <> '' THEN
      object_data := regexp_replace(object_data, template_prefix, prefix, 'g');
    END IF;

    query := format('CREATE VIEW %1$s.%2$I AS %3$s;',
      schema_name,
      new_object_name,
      object_data
    );
    -- RAISE DEBUG '%', query;
    EXECUTE query;
  END LOOP;

  EXECUTE 'SET search_path TO ''' || original_search_path || '''';

  RETURN TRUE;
END;
$BODY$ LANGUAGE 'plpgsql';
