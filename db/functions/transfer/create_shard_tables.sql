DROP FUNCTION IF EXISTS transfer.create_shard_tables(TEXT, TEXT, TEXT, TEXT, TEXT[], JSONB);

CREATE OR REPLACE FUNCTION transfer.create_shard_tables(
  template_schema_name    TEXT,
  schema_name             TEXT,
  template_prefix         TEXT DEFAULT '',
  prefix                  TEXT DEFAULT '',
  excluded_prefixes       TEXT[] DEFAULT '{}',
  all_objects_data        JSONB DEFAULT NULL
)
RETURNS BOOLEAN AS $BODY$
DECLARE
  object_data           JSON;
  qualified_object_name TEXT;
  object_name           TEXT;
  col_default_value     TEXT;
  json_object           JSON;
  query                 TEXT;
  aux                   TEXT;
  original_search_path  TEXT;
  before_query          TEXT;
  after_query           TEXT;
  before_queries        TEXT[];
  after_queries         TEXT[];
  excluded_prefix       TEXT;
BEGIN

  SHOW search_path INTO original_search_path;

  IF all_objects_data IS NULL THEN
    SET search_path TO '';

    -- Get the necessary data to create the new tables
    query := FORMAT('
      SELECT
        json_object_agg(i.qualified_object_name,
          json_build_object(
            ''columns'', i.columns
          )
        )::JSONB
      FROM sharding.get_tables_info(''%1$s'', ''%2$s'') i
      WHERE 1 = 1
    ', template_schema_name, template_prefix);
    FOREACH excluded_prefix IN ARRAY excluded_prefixes
    LOOP
      query := query || ' AND object_name NOT ILIKE ''' || excluded_prefix || '%''';
    END LOOP;
    EXECUTE query INTO all_objects_data;
  END IF;

  EXECUTE 'SET search_path to ' || schema_name || ', public';

  ----------------------
  -- Build the tables --
  ----------------------

  FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP

    -- Reset variables
    aux := NULL;
    before_queries := '{}';
    after_queries := '{}';

    object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');
    object_name := regexp_replace(object_name, template_prefix, prefix);

    RAISE DEBUG '-- [TABLES] TABLE: %', object_name;

    query := format('CREATE TABLE %1$s.%2$I (', schema_name, object_name);

    FOR json_object IN SELECT * FROM json_array_elements(object_data->'columns') LOOP

      -- Handle sequences
      col_default_value := NULL;
      IF (json_object->>'default_value') IS NOT NULL AND (json_object->>'default_value') ~ 'nextval\(' THEN
        IF (json_object->>'default_value') ~ (template_schema_name || '\.' || template_prefix) THEN
          -- It is a sequence internal to the shard
          -- Need to create a new sequence for the primary key
          aux := substring(json_object->>'default_value' FROM position('nextval(' IN json_object->>'default_value'));
          aux := regexp_replace(aux, 'nextval\(''' || template_schema_name || '\.' || template_prefix || '(?:.+\.)?(.*)''.*', '\1');

          col_default_value := regexp_replace(json_object->>'default_value', 'nextval\(''' || template_schema_name || '\.' || template_prefix, 'nextval(''' || schema_name || '.' || prefix);

          before_queries := before_queries
                        || format('CREATE SEQUENCE %1$s.%2$s%3$I;', schema_name, prefix, aux);
          after_queries := after_queries
                        || format('ALTER SEQUENCE %1$s.%5$s%2$I OWNED BY %1$s.%3$I.%4$I;', schema_name, aux, object_name, json_object->>'name', prefix);
                        -- No need to set the counter, it will be set during the pg_restore
                        -- || format('
                        --       DO $$
                        --       DECLARE
                        --         seq_nextval BIGINT;
                        --       BEGIN
                        --         SELECT last_value FROM %4$s.%5$s%3$I INTO seq_nextval;
                        --         EXECUTE FORMAT(''ALTER SEQUENCE %1$s.%2$s%3$I RESTART WITH %%1$s'', seq_nextval);
                        --       END$$;
                        --    ', schema_name, prefix, aux, template_schema_name, template_prefix);
        END IF;
      END IF;

      IF col_default_value IS NULL THEN
        col_default_value := json_object->>'default_value';
      END IF;

      query := query || format('%1$I %2$s%3$s%4$s, ',
        json_object->>'name',
        json_object->>'type',
        CASE WHEN (json_object->>'is_not_null')::BOOLEAN THEN ' NOT NULL' END,
        CASE WHEN col_default_value IS NOT NULL THEN format(' DEFAULT %1$s', col_default_value) END
      );

    END LOOP;

    FOREACH before_query IN ARRAY before_queries
    LOOP
      -- RAISE DEBUG '%', before_query;
      EXECUTE before_query;
    END LOOP;

    query := LEFT(query, length(query) - 2) || ');';
    -- RAISE DEBUG '%', query;

    EXECUTE query;

    FOREACH after_query IN ARRAY after_queries
    LOOP
      -- RAISE DEBUG '%', after_query;
      EXECUTE after_query;
    END LOOP;

  END LOOP;

  EXECUTE 'SET search_path TO ''' || original_search_path || '''';

  RETURN TRUE;
END;
$BODY$ LANGUAGE 'plpgsql';
