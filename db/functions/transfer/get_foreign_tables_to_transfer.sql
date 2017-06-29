DROP FUNCTION IF EXISTS transfer.get_foreign_tables_to_transfer(boolean);
CREATE OR REPLACE FUNCTION transfer.get_foreign_tables_to_transfer(
  transfer_user_templates   boolean DEFAULT false
) RETURNS TABLE (
  table_name    text,
  column_name   text,
  schema_name   text
) AS $BODY$
DECLARE
  query                       text;
  auxiliary_table_information JSONB;
BEGIN

  auxiliary_table_information = sharding.get_auxiliary_table_information();
  query = FORMAT('

    WITH unsharded_tables AS (
      SELECT jsonb_array_elements_text AS table_name
        FROM jsonb_array_elements_text(%1$L)
      WHERE
        jsonb_array_elements_text NOT IN (SELECT unnest(table_name) FROM transfer.get_ignored_auxiliary_tables())
    ),
    foreign_tables AS (
      SELECT
        t.table_name,
        c.column_name::text,
        ''public''::text AS schema_name,
        CASE
          WHEN
            t.table_name = ''users'' THEN ''z''::text
          ELSE
            CASE
              WHEN
                t.table_name ILIKE ''%%_override%%'' THEN ''0''::text || c.column_name::text
              ELSE
                c.column_name::text
              END
          END AS priority_key
        FROM unsharded_tables t
        JOIN information_schema.columns c
          ON c.table_schema = ''public'' AND c.table_name = t.table_name
        WHERE
          (
            c.column_name = ''company_id'' OR c.column_name = ''user_id''
            OR (
              t.table_name = ''companies'' AND c.column_name = ''id''
            )
          )
          AND t.table_name NOT LIKE ''vw_%%''
          AND t.table_name <> ''duplicated_documents''
    ),
    all_tables AS (
      SELECT ''companies''::text AS table_name, ''company_id''::text AS column_name, ''purchases''::text AS schema_name, ''00''::text AS priority_key
      UNION
      SELECT ''accounting_companies''::text AS table_name, ''company_id''::text AS column_name, ''accounting''::text AS schema_name, ''00''::text AS priority_key
      UNION
      SELECT table_name, column_name, schema_name, priority_key FROM foreign_tables
  ', auxiliary_table_information->'unsharded_tables');

  IF transfer_user_templates THEN
    query := query || '
      UNION
      SELECT ''user_templates''::text AS table_name, ''user_id''::text AS column_name, ''accounting''::text AS schema_name, ''00''::text AS priority_key
    ';
  END IF;

  query := query || '
    )

    SELECT table_name, column_name, schema_name
      FROM all_tables
      ORDER BY priority_key DESC, table_name;
  ';

  RETURN QUERY EXECUTE query;

END;
$BODY$ LANGUAGE 'plpgsql';