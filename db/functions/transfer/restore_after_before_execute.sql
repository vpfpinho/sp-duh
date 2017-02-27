DROP FUNCTION IF EXISTS transfer.restore_after_before_execute(bigint);
CREATE OR REPLACE FUNCTION transfer.restore_after_before_execute(
  company_id                  bigint
) RETURNS TABLE (
  schema_name                 text
) AS $BODY$
DECLARE
  meta_schema                 text;
  source_info                 RECORD;
  destination_schema_version  text;
  foreign_table               RECORD;
  query                       text;
  schema                      text;
  columns_list                text;
BEGIN

  -- Validate the company's info

  EXECUTE
    FORMAT('SELECT * FROM transfer.get_meta_schema_name(%1$L)', company_id)
  INTO STRICT meta_schema;

  EXECUTE
    FORMAT('SELECT * FROM %1$s.info', meta_schema)
  INTO STRICT source_info;

  -- Assert that it is the same company

  IF source_info.company_id <> company_id THEN
    RAISE EXCEPTION 'The source company (id %, % %) is not the same as the destination company (id %).', source_info.company_id, source_info.tax_registration_number, source_info.company_name, company_id
      USING ERRCODE = 'BR003';
  END IF;

  -- Assert that the company doesn't exist in the destination database

  IF EXISTS(SELECT 1 FROM public.companies WHERE id = company_id) THEN
    RAISE EXCEPTION 'The source company (id %) already exists in the destination database.', source_info.company_id
      USING ERRCODE = 'BR004';
  END IF;

  -- Assert that the schema version of the source database is compatible with the destination database!

  EXECUTE
    'SELECT version FROM public.schema_migrations ORDER BY version DESC LIMIT 1'
  INTO STRICT destination_schema_version;

  IF source_info.schema_version > destination_schema_version THEN
    RAISE EXCEPTION 'The source schema version (%) is newer than the destination schema version (%).', source_info.schema_version, destination_schema_version
      USING ERRCODE = 'BR005';
  END IF;

  RAISE NOTICE 'Source timestamp %', source_info.backed_up_at;
  RAISE NOTICE 'Source schema version %', source_info.schema_version;

  ----------------------------------------------
  -- Restore the source FOREIGN RECORDS first --
  ----------------------------------------------

  FOR foreign_table IN SELECT * FROM transfer.get_foreign_tables_to_transfer() LOOP

    SELECT
      array_to_string(get_columns_list_for_table, ', ')
    FROM
      transfer.get_columns_list_for_table(meta_schema, foreign_table.schema_name || '_' || foreign_table.table_name)
    INTO
      columns_list;

    RAISE NOTICE 'Restoring foreign records in table %.%_%', meta_schema, foreign_table.schema_name, foreign_table.table_name;
    EXECUTE
      FORMAT('
        ALTER TABLE %2$s.%1$s DISABLE TRIGGER ALL
      ', foreign_table.table_name, foreign_table.schema_name);

    query := FORMAT('
                INSERT INTO %3$s.%2$s
                (%4$s)
                SELECT
                %4$s
                FROM %1$s.%3$s_%2$s
              ', meta_schema, foreign_table.table_name, foreign_table.schema_name, columns_list);
    -- RAISE DEBUG '%', query;
    EXECUTE query;

    EXECUTE
      FORMAT('
        ALTER TABLE %2$s.%1$s ENABLE TRIGGER ALL
      ', foreign_table.table_name, foreign_table.schema_name);

  END LOOP;

  -- Create the schemas being restored (otherwise the pg_restore command won't work)

  FOREACH schema IN ARRAY source_info.backed_up_schemas LOOP
    EXECUTE
      FORMAT('
        DROP SCHEMA IF EXISTS %1$s CASCADE;
        CREATE SCHEMA %1$s;
      ', schema);
  END LOOP;

  -- Return the companies' schemas to include in the main restore
  RETURN QUERY EXECUTE FORMAT('SELECT * FROM unnest(%1$L::text[])', source_info.backed_up_schemas);

END;
$BODY$ LANGUAGE 'plpgsql';