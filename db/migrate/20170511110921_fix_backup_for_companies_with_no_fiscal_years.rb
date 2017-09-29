class FixBackupForCompaniesWithNoFiscalYears < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.backup_before_execute(bigint);
      CREATE OR REPLACE FUNCTION transfer.backup_before_execute(
        company_id          bigint
      ) RETURNS TABLE (
        schema_name         text,
        schema_type         text
      ) AS $BODY$
      DECLARE
        meta_schema         text;
        foreign_table       RECORD;
        query               text;
        company_users       text;
        accounting_schema   text;
        all_table_prefixes  JSON;
      BEGIN

        -- Assert that the company exists and can be backed up!
        PERFORM transfer.validate_company(company_id);

        -- Create the meta schema

        EXECUTE
          FORMAT('SELECT * FROM transfer.create_meta_schema(%1$L)', company_id)
        INTO STRICT meta_schema;

        -- Populate the meta information about the company and the backup

        EXECUTE
          FORMAT('
            WITH schema_info AS (
              SELECT * FROM transfer.get_company_schemas_to_backup(%2$L)
            )
            INSERT INTO %1$s.info
              SELECT
                c.id AS company_id,
                c.tax_registration_number,
                c.company_name,
                (SELECT version FROM public.schema_migrations ORDER BY version DESC LIMIT 1) AS schema_version,
                now() AS backed_up_at,
                (SELECT array_agg(schema_name) FROM schema_info WHERE schema_type <> ''meta'') AS backed_up_schemas,
                (SELECT schema_name FROM schema_info WHERE schema_type = ''main'' LIMIT 1) AS main_schema,
                (SELECT array_agg(schema_name) FROM schema_info WHERE schema_type = ''accounting'') || ''{}'' AS accounting_schemas
                FROM public.companies c
                WHERE c.id = %2$L
          ', meta_schema, company_id);

        -- Get fiscal years information

        query := '';
        FOR accounting_schema IN
          SELECT s.schema_name FROM transfer.get_company_schemas_to_backup(company_id) s WHERE s.schema_type = 'accounting'
        LOOP
          IF query <> '' THEN
            query := query || '
              UNION
            ';
          END IF;
          query := query || FORMAT('
            SELECT ''%1$s''::text AS schema_name, table_prefix::text FROM %1$s.fiscal_years
          ', accounting_schema);
        END LOOP;

        IF query <> '' THEN
          query := '
            WITH fiscal_years AS (
          ' || query || '
            ),
            agg_fiscal_years AS (
              SELECT fy.schema_name, array_agg(fy.table_prefix) AS table_prefixes
              FROM fiscal_years fy
              GROUP BY fy.schema_name
            )
            SELECT
              json_object_agg(fy.schema_name,
                json_build_object(
                  ''prefixes'', fy.table_prefixes
                )
              )::JSON
            FROM agg_fiscal_years fy
          ';
          EXECUTE query INTO all_table_prefixes;

          -- The company may have no fiscal years!!!!
          IF all_table_prefixes IS NOT NULL THEN
            EXECUTE
              FORMAT('
                UPDATE %1$s.info
                SET fiscal_years = ''%2$s''
              ', meta_schema, all_table_prefixes);
          END IF;
        END IF;

        -- Backup all foreign records

        EXECUTE
          FORMAT('SELECT string_agg(user_id::text, '','') FROM transfer.get_company_users(%1$L)', company_id)
        INTO STRICT company_users;

        FOR foreign_table IN SELECT * FROM transfer.get_foreign_tables_to_transfer() LOOP
          RAISE NOTICE 'Backing up foreign records in table %.%', foreign_table.schema_name, foreign_table.table_name;
          query = FORMAT('
                    CREATE UNLOGGED TABLE %1$s.%3$s_%2$s
                    AS
                      SELECT *
                        FROM %3$s.%2$s ft
                  ', meta_schema, foreign_table.table_name, foreign_table.schema_name);
          CASE foreign_table.column_name
            WHEN 'company_id' THEN
              CASE foreign_table.table_name
                WHEN 'users' THEN
                  EXECUTE
                    query ||
                    FORMAT('
                      WHERE ft.id IN (%1$s)
                    ', company_users);
                ELSE
                  EXECUTE
                    query ||
                    FORMAT('
                      WHERE ft.company_id = %1$L
                    ', company_id);
              END CASE;

            WHEN 'id' THEN
              EXECUTE
                query ||
                FORMAT('
                  WHERE ft.id = %1$L
                ', company_id);

            WHEN 'user_id' THEN
              EXECUTE
                query ||
                FORMAT('
                  WHERE ft.user_id IN (%1$s)
                ', company_users);

              ELSE
              -- Do nothing
          END CASE;
        END LOOP;

        -- Return the companies' schemas to include in the backup
        RETURN QUERY EXECUTE FORMAT('SELECT * FROM transfer.get_company_schemas_to_backup(%1$L)', company_id);

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
  end

  def down
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.backup_before_execute(bigint);
      CREATE OR REPLACE FUNCTION transfer.backup_before_execute(
        company_id          bigint
      ) RETURNS TABLE (
        schema_name         text,
        schema_type         text
      ) AS $BODY$
      DECLARE
        meta_schema         text;
        foreign_table       RECORD;
        query               text;
        company_users       text;
        accounting_schema   text;
        all_table_prefixes  JSON;
      BEGIN

        -- Assert that the company exists and can be backed up!
        PERFORM transfer.validate_company(company_id);

        -- Create the meta schema

        EXECUTE
          FORMAT('SELECT * FROM transfer.create_meta_schema(%1$L)', company_id)
        INTO STRICT meta_schema;

        -- Populate the meta information about the company and the backup

        EXECUTE
          FORMAT('
            WITH schema_info AS (
              SELECT * FROM transfer.get_company_schemas_to_backup(%2$L)
            )
            INSERT INTO %1$s.info
              SELECT
                c.id AS company_id,
                c.tax_registration_number,
                c.company_name,
                (SELECT version FROM public.schema_migrations ORDER BY version DESC LIMIT 1) AS schema_version,
                now() AS backed_up_at,
                (SELECT array_agg(schema_name) FROM schema_info WHERE schema_type <> ''meta'') AS backed_up_schemas,
                (SELECT schema_name FROM schema_info WHERE schema_type = ''main'' LIMIT 1) AS main_schema,
                (SELECT array_agg(schema_name) FROM schema_info WHERE schema_type = ''accounting'') || ''{}'' AS accounting_schemas
                FROM public.companies c
                WHERE c.id = %2$L
          ', meta_schema, company_id);

        -- Get fiscal years information

        query := '';
        FOR accounting_schema IN
          SELECT s.schema_name FROM transfer.get_company_schemas_to_backup(company_id) s WHERE s.schema_type = 'accounting'
        LOOP
          IF query <> '' THEN
            query := query || '
              UNION
            ';
          END IF;
          query := query || FORMAT('
            SELECT ''%1$s''::text AS schema_name, table_prefix::text FROM %1$s.fiscal_years
          ', accounting_schema);
        END LOOP;

        IF query <> '' THEN
          query := '
            WITH fiscal_years AS (
          ' || query || '
            ),
            agg_fiscal_years AS (
              SELECT fy.schema_name, array_agg(fy.table_prefix) AS table_prefixes
              FROM fiscal_years fy
              GROUP BY fy.schema_name
            )
            SELECT
              json_object_agg(fy.schema_name,
                json_build_object(
                  ''prefixes'', fy.table_prefixes
                )
              )::JSON
            FROM agg_fiscal_years fy
          ';
          EXECUTE query INTO all_table_prefixes;

          EXECUTE
            FORMAT('
              UPDATE %1$s.info
              SET fiscal_years = ''%2$s''
            ', meta_schema, all_table_prefixes);
        END IF;

        -- Backup all foreign records

        EXECUTE
          FORMAT('SELECT string_agg(user_id::text, '','') FROM transfer.get_company_users(%1$L)', company_id)
        INTO STRICT company_users;

        FOR foreign_table IN SELECT * FROM transfer.get_foreign_tables_to_transfer() LOOP
          RAISE NOTICE 'Backing up foreign records in table %.%', foreign_table.schema_name, foreign_table.table_name;
          query = FORMAT('
                    CREATE UNLOGGED TABLE %1$s.%3$s_%2$s
                    AS
                      SELECT *
                        FROM %3$s.%2$s ft
                  ', meta_schema, foreign_table.table_name, foreign_table.schema_name);
          CASE foreign_table.column_name
            WHEN 'company_id' THEN
              CASE foreign_table.table_name
                WHEN 'users' THEN
                  EXECUTE
                    query ||
                    FORMAT('
                      WHERE ft.id IN (%1$s)
                    ', company_users);
                ELSE
                  EXECUTE
                    query ||
                    FORMAT('
                      WHERE ft.company_id = %1$L
                    ', company_id);
              END CASE;

            WHEN 'id' THEN
              EXECUTE
                query ||
                FORMAT('
                  WHERE ft.id = %1$L
                ', company_id);

            WHEN 'user_id' THEN
              EXECUTE
                query ||
                FORMAT('
                  WHERE ft.user_id IN (%1$s)
                ', company_users);

              ELSE
              -- Do nothing
          END CASE;
        END LOOP;

        -- Return the companies' schemas to include in the backup
        RETURN QUERY EXECUTE FORMAT('SELECT * FROM transfer.get_company_schemas_to_backup(%1$L)', company_id);

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
  end
end
