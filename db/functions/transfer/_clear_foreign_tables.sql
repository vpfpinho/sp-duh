DROP FUNCTION IF EXISTS transfer._clear_foreign_tables(bigint);
CREATE OR REPLACE FUNCTION transfer._clear_foreign_tables(
  company_id      bigint
) RETURNS VOID AS $BODY$
DECLARE
  meta_schema     text;
  company_users   text;
  query           text;
  foreign_table   RECORD;
  clear_query     text;
  schemas         text[];
  schema          text;
BEGIN

  EXECUTE
    FORMAT('SELECT * FROM transfer.get_meta_schema_name(%1$L)', company_id)
  INTO STRICT meta_schema;

  EXECUTE
    FORMAT('SELECT string_agg(user_id::text, '','') FROM transfer.get_company_users(%1$L)', company_id)
  INTO STRICT company_users;

  query = '
      WITH foreign_tables AS (
        SELECT
          t.table_name,
          t.column_name,
          t.schema_name,
          CASE
            WHEN
              t.table_name = ''users'' THEN ''z1''::text
            ELSE
              CASE
                WHEN
                  t.table_name = ''companies'' THEN ''z2''::text
                ELSE
                  CASE
                    WHEN
                      t.table_name ILIKE ''%%_override%%'' THEN ''0''::text || t.column_name
                    ELSE
                      t.column_name
                    END
                END
            END AS priority_key
          FROM transfer.get_foreign_tables_to_transfer() t
      )

      SELECT table_name, column_name, schema_name
        FROM foreign_tables
        ORDER BY priority_key ASC, table_name;

  ';

  FOR foreign_table IN EXECUTE(query) LOOP
    RAISE NOTICE 'Deleting foreign records from %.% using %', foreign_table.schema_name, foreign_table.table_name, foreign_table.column_name;
    clear_query = FORMAT('
                    ALTER TABLE %2$s.%1$s DISABLE TRIGGER ALL;
                    DELETE FROM %2$s.%1$s
                  ', foreign_table.table_name, foreign_table.schema_name);
    CASE foreign_table.column_name
      WHEN 'company_id' THEN
        CASE foreign_table.table_name
          WHEN 'users' THEN
            EXECUTE
              clear_query ||
              FORMAT('
                WHERE id IN (%1$s)
              ', company_users);
          ELSE
            EXECUTE
              clear_query ||
              FORMAT('
                WHERE company_id = %1$L
              ', company_id);
        END CASE;

      WHEN 'id' THEN
        EXECUTE
          clear_query ||
          FORMAT('
            WHERE id = %1$L
          ', company_id);

      WHEN 'user_id' THEN
        EXECUTE
          clear_query ||
          FORMAT('
            WHERE user_id IN (%1$s)
          ', company_users);

        ELSE
        -- Do nothing
    END CASE;
    EXECUTE FORMAT('ALTER TABLE %2$s.%1$s ENABLE TRIGGER ALL;', foreign_table.table_name, foreign_table.schema_name);
  END LOOP;

  EXECUTE
    FORMAT('SELECT backed_up_schemas FROM %1$s.info', meta_schema)
  INTO STRICT schemas;

  FOREACH schema IN ARRAY schemas LOOP
    RAISE NOTICE 'Dropping restored schema %', schema;
    EXECUTE FORMAT('DROP SCHEMA IF EXISTS %1$s CASCADE', schema);
  END LOOP;

END;
$BODY$ LANGUAGE 'plpgsql';