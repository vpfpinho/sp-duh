DROP FUNCTION IF EXISTS transfer.restore_before_before_execute(bigint);
CREATE OR REPLACE FUNCTION transfer.restore_before_before_execute(
  company_id                  bigint
) RETURNS text AS $BODY$
DECLARE
  meta_schema                 text;
  external_tables             JSONB;
  schema_name                 TEXT;
  tables                      JSON;
  all_objects_data            JSONB;
  query                       TEXT;
  foreign_tables              TEXT[];
BEGIN

  -- Create the meta schema and base tables
  EXECUTE
    FORMAT('SELECT * FROM transfer.create_meta_schema(%1$L)', company_id)
  INTO STRICT meta_schema;

  -- Create the external foreign records tables

  SELECT
    json_object_agg(ta.schema_name,
      json_build_object(
        'tables', ta.table_names
      )
    )::JSONB INTO external_tables
  FROM (
    SELECT
      t.schema_name,
      array_agg(t.table_name) AS table_names
    FROM
      transfer.get_foreign_tables_to_transfer() t
    GROUP BY
      t.schema_name
  ) ta;

  FOR schema_name, tables IN SELECT * FROM jsonb_each(external_tables) LOOP

    SELECT ARRAY(SELECT trim(t::TEXT, '"') FROM json_array_elements(tables->'tables') t) INTO foreign_tables;

    -- Get the foreign tables to build
    query := FORMAT('
      SELECT
        json_object_agg(i.qualified_object_name,
          json_build_object(
            ''columns'', i.columns
          )
        )::JSONB
      FROM sharding.get_tables_info(''%1$s'') i
      WHERE i.object_name = ANY(''%2$s'')
    ', schema_name, foreign_tables);
    EXECUTE query INTO all_objects_data;

    -- Build the foreign tables
    PERFORM transfer.create_shard_tables(schema_name, meta_schema, '', schema_name || '_', '{}', all_objects_data);

  END LOOP;

  RETURN meta_schema;

END;
$BODY$ LANGUAGE 'plpgsql';