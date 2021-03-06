class UseTemplateCompanyFromCloudwareServices < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.get_restore_templates(bigint);
      CREATE OR REPLACE FUNCTION transfer.get_restore_templates(
        IN  p_company_id                bigint,
        IN  p_template_company_id       bigint,
        OUT main_schema_template        text,
        OUT accounting_schema_template  text,
        OUT fiscal_year_template        text
      )
      RETURNS record AS $BODY$
      BEGIN

        EXECUTE '
          SELECT
            c.schema_name::text,
            a.schema_name::text
          FROM
            public.companies c
          LEFT JOIN
            accounting.accounting_companies a ON a.company_id = c.id
          WHERE
            c.use_sharded_company = true AND
            c.id <> $1 AND
            c.id = $2
        '
        USING p_company_id, p_template_company_id
        INTO main_schema_template, accounting_schema_template;

        fiscal_year_template := NULL;
        IF accounting_schema_template IS NOT NULL THEN
          EXECUTE FORMAT('
            SELECT
              y.table_prefix::text
            FROM
              %1$s.fiscal_years y
            ORDER BY
              y.id
            LIMIT 1
          ', accounting_schema_template)
          INTO fiscal_year_template;
        END IF;

        RETURN;

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.restore_after_before_execute(bigint, boolean);
      CREATE OR REPLACE FUNCTION transfer.restore_after_before_execute(
        company_id                  bigint,
        template_company_id         bigint,
        validate_only               boolean DEFAULT false
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
        main_schema_template        text;
        accounting_schema_template  text;
        fiscal_year_template        text;
        accounting_schema           text;
        prefixes                    JSON;
        prefix                      text;
        excluded_prefixes           text[];
        has_fiscal_years            boolean;
        company_accountant_id       bigint;
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

        -- Assert that there are avaliable templates to build the company in the destination database

        SELECT * FROM transfer.get_restore_templates(company_id, template_company_id)
        INTO main_schema_template, accounting_schema_template, fiscal_year_template;

        has_fiscal_years := false;
        FOR accounting_schema, prefixes IN SELECT * FROM json_each(source_info.fiscal_years) LOOP
          FOR prefix IN SELECT * FROM json_array_elements(prefixes->'prefixes') LOOP
            has_fiscal_years := true;
          END LOOP;
        END LOOP;

        IF COALESCE(main_schema_template, '') = '' THEN
          RAISE EXCEPTION 'There are no sharded companies in the destination database that can serve as templates for the restore.'
            USING ERRCODE = 'BR006';
        END IF;
        IF cardinality(source_info.accounting_schemas) > 0 AND COALESCE(accounting_schema_template, '') = '' THEN
          RAISE EXCEPTION 'There are no accounting companies in the destination database that can serve as templates for the restore.'
            USING ERRCODE = 'BR007';
        END IF;
        IF has_fiscal_years = true AND COALESCE(fiscal_year_template, '') = '' THEN
          RAISE EXCEPTION 'There are no fiscal years in the destination database that can serve as templates for the restore.'
            USING ERRCODE = 'BR008';
        END IF;

        -- Show backup info

        RAISE NOTICE '------------------------';
        RAISE NOTICE 'Source information:';
        RAISE NOTICE '------------------------';
        RAISE NOTICE '   Company % - % (%)', source_info.tax_registration_number, source_info.company_name, source_info.company_id;
        RAISE NOTICE '   Schemas and prefixes:';
        RAISE NOTICE '      Company schema %', source_info.main_schema;
        RAISE NOTICE '      Accounting schemas %', source_info.accounting_schemas;
        RAISE NOTICE '      Fiscal years %', source_info.fiscal_years;
        RAISE NOTICE '   Schema version %', source_info.schema_version;
        RAISE NOTICE '   Backed up at %', source_info.backed_up_at;

        -- Show restore info

        RAISE NOTICE '------------------------';
        RAISE NOTICE 'Destination information:';
        RAISE NOTICE '------------------------';
        RAISE NOTICE '   Template schemas and prefixes:';
        RAISE NOTICE '      Company schema template %', main_schema_template;
        RAISE NOTICE '      Accounting schema template %', accounting_schema_template;
        RAISE NOTICE '      Fiscal year template %', fiscal_year_template;
        RAISE NOTICE '   Schema version %', destination_schema_version;
        RAISE NOTICE '------------------------';

        IF NOT validate_only THEN

          ----------------------------------------------
          -- Restore the source FOREIGN RECORDS first --
          ----------------------------------------------

          -- Who is the company accountant?
          EXECUTE FORMAT('SELECT c.accountant_id FROM %1$s.public_companies c WHERE c.id = %2$L', meta_schema, company_id)
          INTO STRICT company_accountant_id;
          -- Does the company accountant already exist in the destination database?
          IF EXISTS (SELECT u.id FROM public.users u WHERE u.id = company_accountant_id) THEN
            -- The company accountant already exists in the destination database. Do not try to insert it again.
            EXECUTE FORMAT('DELETE FROM %1$s.public_users u WHERE u.id = %2$L', meta_schema, company_accountant_id);
            RAISE DEBUG 'NOT RESTORING accountant %', company_accountant_id;
          END IF;

          FOR foreign_table IN SELECT * FROM transfer.get_foreign_tables_to_transfer() LOOP

            SELECT
              array_to_string(get_columns_list_for_table, ', ')
            FROM
              transfer.get_columns_list_for_table(meta_schema, foreign_table.schema_name || '_' || foreign_table.table_name)
            INTO
              columns_list;

            -- TO DO: replace trigger disabling with some other mechanism (trigger skipping in code?)
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

            -- TO DO: replace trigger disabling with some other mechanism (trigger skipping in code?)
            EXECUTE
              FORMAT('
                ALTER TABLE %2$s.%1$s ENABLE TRIGGER ALL
              ', foreign_table.table_name, foreign_table.schema_name);

          END LOOP;

          ----------------------------------------
          -- Create the SCHEMAS being restored  --
          ----------------------------------------

          FOREACH schema IN ARRAY source_info.backed_up_schemas LOOP
            RAISE NOTICE 'Creating schema %', schema;
            EXECUTE
              FORMAT('
                DROP SCHEMA IF EXISTS %1$s CASCADE;
                CREATE SCHEMA %1$s;
              ', schema);
          END LOOP;

          ---------------------------------------
          -- Create the TABLES being restored  --
          ---------------------------------------

          -- MAIN company schema tables
          RAISE NOTICE 'Creating tables in schema %', source_info.main_schema;
          PERFORM transfer.create_shard_tables(main_schema_template, source_info.main_schema);

          -- ACCOUNTING companies schema tables
          EXECUTE FORMAT('
            SELECT array_agg(table_prefix) from %1$s.fiscal_years
          ', accounting_schema_template)
          INTO STRICT excluded_prefixes;
          FOREACH accounting_schema IN ARRAY source_info.accounting_schemas LOOP
            RAISE NOTICE 'Creating (global) tables in schema %', accounting_schema;
            PERFORM transfer.create_shard_tables(accounting_schema_template, accounting_schema, '', '', excluded_prefixes);
          END LOOP;

          -- Accounting companies FISCAL YEARS schema tables
          FOR accounting_schema, prefixes IN SELECT * FROM json_each(source_info.fiscal_years) LOOP
            FOREACH prefix IN ARRAY ARRAY(SELECT trim(fy::text, '"') FROM json_array_elements(prefixes->'prefixes') fy) LOOP
              RAISE NOTICE 'Creating tables in schema % with prefix %', accounting_schema, prefix;
              PERFORM transfer.create_shard_tables(accounting_schema_template, accounting_schema, fiscal_year_template, prefix);
            END LOOP;
          END LOOP;

        END IF;

        -- Return the companies' schemas to include in the main restore
        RETURN QUERY EXECUTE FORMAT('SELECT * FROM unnest(%1$L::text[])', source_info.backed_up_schemas);

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
    execute <<-'SQL'
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
    SQL
  end

  def down
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.get_restore_templates(bigint, bigint);
      CREATE OR REPLACE FUNCTION transfer.get_restore_templates(
        p_company_id                bigint
      )
      RETURNS TABLE (
        main_schema_template        text,
        accounting_schema_template  text,
        fiscal_year_template        text
      ) AS $BODY$
      DECLARE
        query                       text;
        companies                   RECORD;
        main_schema_template        text;
        accounting_schema_template  text;
        fiscal_year_template        text;
      BEGIN

        main_schema_template := NULL;
        accounting_schema_template := NULL;

        FOR companies IN
          SELECT
            c.schema_name::text AS main_schema_template,
            a.schema_name::text AS accounting_schema_template
          FROM
            public.companies c
          LEFT JOIN
            accounting.accounting_companies a ON a.company_id = c.id
          WHERE
            c.use_sharded_company = true AND
            c.id <> p_company_id
          ORDER BY
            c.id,
            a.id
          LIMIT 10
        LOOP

          main_schema_template := companies.main_schema_template;
          accounting_schema_template := companies.accounting_schema_template;

          IF accounting_schema_template IS NOT NULL THEN
            EXECUTE FORMAT('
              SELECT
                y.table_prefix::text AS fiscal_year_template
              FROM
                %1$s.fiscal_years y
              ORDER BY
                y.id
              LIMIT 1
            ', accounting_schema_template)
            INTO fiscal_year_template;

            EXIT WHEN fiscal_year_template IS NOT NULL;
          END IF;

        END LOOP;

        query := FORMAT('
          SELECT
            ''%1$s''::text AS main_schema_template,
            ''%2$s''::text AS accounting_schema_template,
            ''%3$s''::text AS fiscal_year_template
        ', main_schema_template, accounting_schema_template, fiscal_year_template);

        RETURN QUERY EXECUTE query;

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.restore_after_before_execute(bigint, bigint, boolean);
      CREATE OR REPLACE FUNCTION transfer.restore_after_before_execute(
        company_id                  bigint,
        validate_only               boolean DEFAULT false
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
        main_schema_template        text;
        accounting_schema_template  text;
        fiscal_year_template        text;
        accounting_schema           text;
        prefixes                    JSON;
        prefix                      text;
        excluded_prefixes           text[];
        has_fiscal_years            boolean;
        company_accountant_id       bigint;
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

        -- Assert that there are avaliable templates to build the company in the destination database

        SELECT * FROM transfer.get_restore_templates(company_id)
        INTO main_schema_template, accounting_schema_template, fiscal_year_template;

        has_fiscal_years := false;
        FOR accounting_schema, prefixes IN SELECT * FROM json_each(source_info.fiscal_years) LOOP
          FOR prefix IN SELECT * FROM json_array_elements(prefixes->'prefixes') LOOP
            has_fiscal_years := true;
          END LOOP;
        END LOOP;

        IF COALESCE(main_schema_template, '') = '' THEN
          RAISE EXCEPTION 'There are no sharded companies in the destination database that can serve as templates for the restore.'
            USING ERRCODE = 'BR006';
        END IF;
        IF cardinality(source_info.accounting_schemas) > 0 AND COALESCE(accounting_schema_template, '') = '' THEN
          RAISE EXCEPTION 'There are no accounting companies in the destination database that can serve as templates for the restore.'
            USING ERRCODE = 'BR007';
        END IF;
        IF has_fiscal_years = true AND COALESCE(fiscal_year_template, '') = '' THEN
          RAISE EXCEPTION 'There are no fiscal years in the destination database that can serve as templates for the restore.'
            USING ERRCODE = 'BR008';
        END IF;

        -- Show backup info

        RAISE NOTICE '------------------------';
        RAISE NOTICE 'Source information:';
        RAISE NOTICE '------------------------';
        RAISE NOTICE '   Company % - % (%)', source_info.tax_registration_number, source_info.company_name, source_info.company_id;
        RAISE NOTICE '   Schemas and prefixes:';
        RAISE NOTICE '      Company schema %', source_info.main_schema;
        RAISE NOTICE '      Accounting schemas %', source_info.accounting_schemas;
        RAISE NOTICE '      Fiscal years %', source_info.fiscal_years;
        RAISE NOTICE '   Schema version %', source_info.schema_version;
        RAISE NOTICE '   Backed up at %', source_info.backed_up_at;

        -- Show restore info

        RAISE NOTICE '------------------------';
        RAISE NOTICE 'Destination information:';
        RAISE NOTICE '------------------------';
        RAISE NOTICE '   Template schemas and prefixes:';
        RAISE NOTICE '      Company schema template %', main_schema_template;
        RAISE NOTICE '      Accounting schema template %', accounting_schema_template;
        RAISE NOTICE '      Fiscal year template %', fiscal_year_template;
        RAISE NOTICE '   Schema version %', destination_schema_version;
        RAISE NOTICE '------------------------';

        IF NOT validate_only THEN

          ----------------------------------------------
          -- Restore the source FOREIGN RECORDS first --
          ----------------------------------------------

          -- Who is the company accountant?
          EXECUTE FORMAT('SELECT c.accountant_id FROM %1$s.public_companies c WHERE c.id = %2$L', meta_schema, company_id)
          INTO STRICT company_accountant_id;
          -- Does the company accountant already exist in the destination database?
          IF EXISTS (SELECT u.id FROM public.users u WHERE u.id = company_accountant_id) THEN
            -- The company accountant already exists in the destination database. Do not try to insert it again.
            EXECUTE FORMAT('DELETE FROM %1$s.public_users u WHERE u.id = %2$L', meta_schema, company_accountant_id);
            RAISE DEBUG 'NOT RESTORING accountant %', company_accountant_id;
          END IF;

          FOR foreign_table IN SELECT * FROM transfer.get_foreign_tables_to_transfer() LOOP

            SELECT
              array_to_string(get_columns_list_for_table, ', ')
            FROM
              transfer.get_columns_list_for_table(meta_schema, foreign_table.schema_name || '_' || foreign_table.table_name)
            INTO
              columns_list;

            -- TO DO: replace trigger disabling with some other mechanism (trigger skipping in code?)
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

            -- TO DO: replace trigger disabling with some other mechanism (trigger skipping in code?)
            EXECUTE
              FORMAT('
                ALTER TABLE %2$s.%1$s ENABLE TRIGGER ALL
              ', foreign_table.table_name, foreign_table.schema_name);

          END LOOP;

          ----------------------------------------
          -- Create the SCHEMAS being restored  --
          ----------------------------------------

          FOREACH schema IN ARRAY source_info.backed_up_schemas LOOP
            RAISE NOTICE 'Creating schema %', schema;
            EXECUTE
              FORMAT('
                DROP SCHEMA IF EXISTS %1$s CASCADE;
                CREATE SCHEMA %1$s;
              ', schema);
          END LOOP;

          ---------------------------------------
          -- Create the TABLES being restored  --
          ---------------------------------------

          -- MAIN company schema tables
          RAISE NOTICE 'Creating tables in schema %', source_info.main_schema;
          PERFORM transfer.create_shard_tables(main_schema_template, source_info.main_schema);

          -- ACCOUNTING companies schema tables
          EXECUTE FORMAT('
            SELECT array_agg(table_prefix) from %1$s.fiscal_years
          ', accounting_schema_template)
          INTO STRICT excluded_prefixes;
          FOREACH accounting_schema IN ARRAY source_info.accounting_schemas LOOP
            RAISE NOTICE 'Creating (global) tables in schema %', accounting_schema;
            PERFORM transfer.create_shard_tables(accounting_schema_template, accounting_schema, '', '', excluded_prefixes);
          END LOOP;

          -- Accounting companies FISCAL YEARS schema tables
          FOR accounting_schema, prefixes IN SELECT * FROM json_each(source_info.fiscal_years) LOOP
            FOREACH prefix IN ARRAY ARRAY(SELECT trim(fy::text, '"') FROM json_array_elements(prefixes->'prefixes') fy) LOOP
              RAISE NOTICE 'Creating tables in schema % with prefix %', accounting_schema, prefix;
              PERFORM transfer.create_shard_tables(accounting_schema_template, accounting_schema, fiscal_year_template, prefix);
            END LOOP;
          END LOOP;

        END IF;

        -- Return the companies' schemas to include in the main restore
        RETURN QUERY EXECUTE FORMAT('SELECT * FROM unnest(%1$L::text[])', source_info.backed_up_schemas);

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
    execute <<-'SQL'
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
    SQL
  end
end
