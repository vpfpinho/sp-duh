class CreateTransferFunctions < ActiveRecord::MigrationWithoutTransaction
  def up

    # In sharding.

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS sharding.get_tables_info(TEXT, TEXT);
      CREATE OR REPLACE FUNCTION sharding.get_tables_info(
        schema_name             TEXT DEFAULT 'public',
        prefix                  TEXT DEFAULT ''
      )
      RETURNS TABLE (
        object_name             TEXT,
        qualified_object_name   TEXT,
        columns                 JSONB,
        indexes                 JSONB,
        foreign_keys            JSONB,
        constraints             JSONB,
        triggers                JSONB
      ) AS $BODY$
      DECLARE
      BEGIN

        RETURN QUERY EXECUTE FORMAT('
          WITH table_columns AS (
            SELECT
              t.tablename::TEXT AS object_name,
              format(''%%1$I.%%2$I'', t.schemaname, t.tablename) AS qualified_object_name,
              (t.schemaname || ''.'' || t.tablename)::regclass::oid AS table_oid,
              json_agg(json_build_object(
                ''name'', a.attname,
                ''type'', pg_catalog.format_type(a.atttypid, a.atttypmod),
                ''default_value'', (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
                ''is_not_null'', a.attnotnull
              ) ORDER BY a.attnum)::JSONB AS columns
            FROM pg_catalog.pg_attribute a
              JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
              JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              JOIN pg_catalog.pg_tables t ON c.oid = (t.schemaname || ''.'' || t.tablename)::regclass::oid
            WHERE a.attnum > 0
              AND NOT a.attisdropped
              AND n.nspname = %1$L
              AND t.tablename ILIKE ''%2$s%%''
            GROUP BY t.schemaname, t.tablename
          ),
          table_indexes AS (
            SELECT
              format(''%%1$I.%%2$I'', t.schemaname, t.tablename) AS qualified_object_name,
              (t.schemaname || ''.'' || t.tablename)::regclass::oid AS table_oid,
              json_agg(json_build_object(
                ''name'', c2.relname,
                ''is_primary'', i.indisprimary,
                ''is_unique'', i.indisunique,
                ''definition'', pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
                ''constraint_definition'', pg_catalog.pg_get_constraintdef(con.oid, true)
              )::JSONB)::JSONB AS indexes
            FROM pg_catalog.pg_class c
              JOIN pg_catalog.pg_index i ON c.oid = i.indrelid
              JOIN pg_catalog.pg_class c2 ON i.indexrelid = c2.oid
              LEFT JOIN pg_catalog.pg_constraint con ON (con.conrelid = i.indrelid AND con.conindid = i.indexrelid AND con.contype IN (''p'',''u'',''x''))
              JOIN pg_catalog.pg_tables t ON c.oid = (t.schemaname || ''.'' || t.tablename)::regclass::oid
            WHERE t.schemaname = %1$L
              AND t.tablename ILIKE ''%2$s%%''
            GROUP BY t.schemaname, t.tablename
          ),
          table_foreign_keys AS (
            SELECT
              format(''%%1$I.%%2$I'', t.schemaname, t.tablename) AS qualified_object_name,
              (t.schemaname || ''.'' || t.tablename)::regclass::oid AS table_oid,
              json_agg(json_build_object(
                ''name'', c.conname,
                ''update_action'', c.confupdtype,
                ''delete_action'', c.confdeltype,
                ''definition'', pg_catalog.pg_get_constraintdef(c.oid, true)
              )::JSONB)::JSONB AS foreign_keys
            FROM pg_catalog.pg_constraint c
              LEFT JOIN pg_catalog.pg_tables t ON c.conrelid = (t.schemaname || ''.'' || t.tablename)::regclass::oid
            WHERE c.contype = ''f''
              AND t.schemaname = %1$L
              AND t.tablename ILIKE ''%2$s%%''
            GROUP BY t.schemaname, t.tablename
          ),
          table_constraints AS (
            SELECT
              format(''%%1$I.%%2$I'', t.schemaname, t.tablename) AS qualified_object_name,
              (t.schemaname || ''.'' || t.tablename)::regclass::oid AS table_oid,
              json_agg(json_build_object(
                ''name'', c.conname,
                ''definition'', pg_catalog.pg_get_constraintdef(c.oid, true)
              )::JSONB)::JSONB AS constraints
            FROM pg_catalog.pg_constraint c
              LEFT JOIN pg_catalog.pg_tables t ON c.conrelid = (t.schemaname || ''.'' || t.tablename)::regclass::oid
            WHERE c.contype = ''c''
              AND t.schemaname = %1$L
              AND t.tablename ILIKE ''%2$s%%''
            GROUP BY t.schemaname, t.tablename
          ),
          table_triggers AS (
            SELECT
              format(''%%1$I.%%2$I'', ta.schemaname, ta.tablename) AS qualified_object_name,
              (ta.schemaname || ''.'' || ta.tablename)::regclass::oid AS table_oid,
              json_agg(json_build_object(
                ''name'', t.tgname,
                ''definition'', pg_catalog.pg_get_triggerdef(t.oid, true)
              )::JSONB)::JSONB AS triggers
            FROM pg_catalog.pg_trigger t
              LEFT JOIN pg_catalog.pg_tables ta ON t.tgrelid = (ta.schemaname || ''.'' || ta.tablename)::regclass::oid
            WHERE ta.schemaname = %1$L
              AND ta.tablename ILIKE ''%2$s%%''
              AND (NOT t.tgisinternal OR (t.tgisinternal AND t.tgenabled = ''D''))
              AND t.tgname != ''trg_prevent_insert_or_update_on_sharded_companies'' -- Do not copy the prevent trigger for sharded companies
              -- AND t.tgname !~* ''^trg_vfk(?:i|p)?'' -- Do not copy the virtual foreign key triggers
            GROUP BY ta.schemaname, ta.tablename
          )
          SELECT
            c.object_name,
            c.qualified_object_name,
            c.columns,
            i.indexes,
            fk.foreign_keys,
            ct.constraints,
            trg.triggers
          FROM table_columns c
            LEFT JOIN table_indexes i ON c.table_oid = i.table_oid
            LEFT JOIN table_foreign_keys fk ON c.table_oid = fk.table_oid
            LEFT JOIN table_constraints ct ON c.table_oid = ct.table_oid
            LEFT JOIN table_triggers trg ON c.table_oid = trg.table_oid
        ', schema_name, prefix);

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS sharding.get_views_info(TEXT, TEXT);
      CREATE OR REPLACE FUNCTION sharding.get_views_info(
        schema_name             TEXT DEFAULT 'public',
        prefix                  TEXT DEFAULT ''
      )
      RETURNS TABLE (
        object_name             TEXT,
        qualified_object_name   TEXT,
        independent             BOOLEAN,
        definition              TEXT
      ) AS $BODY$
      DECLARE
        all_objects_data        JSONB;
      BEGIN

        EXECUTE FORMAT('
          SELECT json_object(array_agg(dependent_view), array_agg(depends_on))::JSONB
          FROM (
            SELECT
              dependent_view.relname::TEXT AS dependent_view,
              array_agg(source_view.relname)::TEXT AS depends_on
            FROM pg_depend
              JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
              JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
              JOIN pg_class as source_view ON pg_depend.refobjid = source_view.oid
              JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
              JOIN pg_namespace source_ns ON source_ns.oid = source_view.relnamespace
            WHERE source_ns.nspname = %1$L
              AND dependent_ns.nspname = %1$L
              AND source_view.relname != dependent_view.relname
              AND source_view.relname ILIKE ''%2$s%%''
              AND dependent_view.relname ILIKE ''%2$s%%''
              AND source_view.relkind = ''v''
            GROUP by dependent_view.relname
          ) views_dependencies;
        ', schema_name, prefix)
        INTO all_objects_data;

        RETURN QUERY EXECUTE FORMAT('
          SELECT
            v.viewname::TEXT AS object_name,
            format(''%%1$I.%%2$I'', v.schemaname, v.viewname) AS qualified_object_name,
            CASE WHEN NOT %3$L ? v.viewname THEN true ELSE false END AS independent,
            pg_catalog.pg_get_viewdef(c.oid) AS definition
          FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_views v ON c.oid = (v.schemaname || ''.'' || v.viewname)::regclass::oid
          WHERE n.nspname = %1$L
            AND v.viewname ILIKE ''%2$s%%''
        ', schema_name, prefix, all_objects_data);

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    # In transfer.

    execute <<-'SQL'
      CREATE SCHEMA transfer;
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.get_columns_list_for_table(text, text);
      CREATE OR REPLACE FUNCTION transfer.get_columns_list_for_table(
        schema_name   text,
        table_name    text
      ) RETURNS text[] AS $BODY$
      DECLARE
        columns_list  text[];
      BEGIN

        WITH table_columns AS (
          SELECT
            a.attname AS name
          FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_tables t ON c.oid = (t.schemaname || '.' || t.tablename)::regclass::oid
          WHERE a.attnum > 0
            AND NOT a.attisdropped
            AND n.nspname = schema_name
            AND t.tablename = table_name
        )
        SELECT array_agg(tc.name) FROM table_columns tc
        INTO columns_list;

        RETURN columns_list;

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.get_meta_schema_name(bigint);
      CREATE OR REPLACE FUNCTION transfer.get_meta_schema_name(
        company_id    bigint
      ) RETURNS text AS $BODY$
      DECLARE
      BEGIN

        -- Now is global (the same for all companies), but can be defined as one meta schema per company
        RETURN '_meta_';

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.get_restore_templates(bigint);
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
      DROP FUNCTION IF EXISTS transfer.create_meta_schema(bigint);
      CREATE OR REPLACE FUNCTION transfer.create_meta_schema(
        company_id    bigint
      ) RETURNS text AS $BODY$
      DECLARE
        meta_schema   text;
      BEGIN

        EXECUTE
          FORMAT('SELECT * FROM transfer.get_meta_schema_name(%1$L)', company_id)
        INTO STRICT meta_schema;

        EXECUTE FORMAT('

          DROP SCHEMA IF EXISTS %1$s CASCADE;
          CREATE SCHEMA %1$s;

          CREATE UNLOGGED TABLE %1$s.info (
            company_id                integer,
            tax_registration_number   character varying(255),
            company_name              character varying(255),
            schema_version            character varying(255),
            backed_up_at              timestamp with time zone,
            backed_up_schemas         text[],
            main_schema               text,
            accounting_schemas        text[],
            fiscal_years              JSON DEFAULT NULL
          );
        ', meta_schema);

        RETURN meta_schema;

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.get_company_users(bigint);
      CREATE OR REPLACE FUNCTION transfer.get_company_users(
        company_id    bigint
      ) RETURNS TABLE (
        user_id       bigint,
        full_name     text,
        type          text
      ) AS $BODY$
      DECLARE
        query         text;
      BEGIN

        query = FORMAT('

          SELECT u.id::bigint, u.full_name::text, u.type::text
            FROM public.users u
            WHERE
              u.company_id = %1$L
          UNION
          SELECT u.id::bigint, u.full_name::text, u.type::text
            FROM public.users u
            JOIN public.companies c
              ON u.id = c.accountant_id
            WHERE
              c.id = %1$L

        ', company_id);
        RETURN QUERY EXECUTE query;

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

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.get_company_schemas_to_backup(bigint, boolean);
      CREATE OR REPLACE FUNCTION transfer.get_company_schemas_to_backup(
        company_id                bigint,
        transfer_user_templates   boolean DEFAULT false
      ) RETURNS TABLE (
        schema_name   text,
        schema_type   text
      ) AS $BODY$
      DECLARE
        query         text;
      BEGIN

        query := FORMAT('
          WITH accounting_schemas AS (
            SELECT ac.schema_name
              FROM accounting.accounting_companies ac
              WHERE ac.company_id = %1$L
          )
        ', company_id);

        IF transfer_user_templates THEN
          query := query || FORMAT('
            ,
            user_schemas AS (
              SELECT ut.schema_name
                FROM accounting.user_templates ut
                WHERE ut.user_id IN (
                  SELECT user_id FROM transfer.get_company_users(%1$L)
                )
            )
          ', company_id);
        END IF;

        query := query || FORMAT('
          SELECT c.schema_name::text, ''main''::text AS schema_type
            FROM companies c
            WHERE c.id = %1$L AND COALESCE(c.schema_name, '''') <> ''''
          UNION
          SELECT c.schema_name::text, ''accounting''::text AS schema_type
            FROM accounting_schemas c
          UNION
          SELECT (SELECT * FROM transfer.get_meta_schema_name(%1$L)) AS schema_name, ''meta''::text AS schema_type
        ', company_id);

        IF transfer_user_templates THEN
          query := query || '
            UNION
            SELECT c.schema_name::text, ''user''::text AS schema_type
              FROM user_schemas c
          ';
        END IF;

        RETURN QUERY EXECUTE query;

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.validate_company(bigint);
      CREATE OR REPLACE FUNCTION transfer.validate_company(
        company_id      bigint
      ) RETURNS VOID AS $BODY$
      DECLARE
        is_sharded      boolean;
      BEGIN

        -- Assert that the company exists!

        IF NOT EXISTS(SELECT 1 FROM public.companies WHERE id = company_id) THEN
          RAISE EXCEPTION 'The company does not exist.'
            USING ERRCODE = 'BR001';
        END IF;

        -- Assert that the company can be backed up: only sharded companies can!

        EXECUTE
          FORMAT('SELECT use_sharded_company FROM public.companies WHERE id = %1$L', company_id)
        INTO STRICT is_sharded;

        IF NOT is_sharded THEN
          RAISE EXCEPTION 'Only sharded companies can be backed up and transferred.'
            USING ERRCODE = 'BR002';
        END IF;

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer._prepare_company_to_backup(bigint);
      CREATE OR REPLACE FUNCTION transfer._prepare_company_to_backup(
        company_id      bigint
      ) RETURNS VOID AS $BODY$
      DECLARE
        query           text;
        schema          text;
      BEGIN

        -- Assert that the company exists and can be backed up!
        PERFORM transfer.validate_company(company_id);

        SELECT schema_name FROM public.companies WHERE id = company_id
        INTO schema;

        -- Convert all public sequences to private sequences
        IF NOT EXISTS(SELECT 1 FROM to_regclass((schema || '.customers_id_seq')::cstring) s WHERE s IS NOT NULL) THEN
          RAISE NOTICE 'Converting public sequences to private (shard) sequences for company schema %', schema;
          PERFORM sharding.convert_sequences_to_schema_qualified(schema);
        ELSE
          RAISE NOTICE '[NOT DOING ANYTHING] Public sequences were already converted to private (shard) sequences for company schema %', schema;
        END IF;

        -- Drop tables that were sharded but are no longer

        IF EXISTS(SELECT 1 FROM to_regclass((schema || '.company_certificates')::cstring) s WHERE s IS NOT NULL) THEN
          RAISE NOTICE 'Dropping no longer sharded table ''%'' in company schema %', 'company_certificates', schema;
          EXECUTE 'DROP TABLE ' || schema || '.company_certificates';
        ELSE
          RAISE NOTICE '[NOT DOING ANYTHING] No longer sharded table ''%'' was already dropped from company schema %', 'company_certificates', schema;
        END IF;
        IF EXISTS(SELECT 1 FROM to_regclass((schema || '.user_message_statuses')::cstring) s WHERE s IS NOT NULL) THEN
          RAISE NOTICE 'Dropping no longer sharded table ''%'' in company schema %', 'user_message_statuses', schema;
          EXECUTE 'DROP TABLE ' || schema || '.user_message_statuses';
        ELSE
          RAISE NOTICE '[NOT DOING ANYTHING] No longer sharded table ''%'' was already dropped from company schema %', 'user_message_statuses', schema;
        END IF;

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer._drop_restored_company(bigint);
      CREATE OR REPLACE FUNCTION transfer._drop_restored_company(
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
    SQL

    execute <<-'SQL'
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
        before_query          TEXT;
        after_query           TEXT;
        after_queries         TEXT[];
        excluded_prefix       TEXT;
      BEGIN

        IF all_objects_data IS NULL THEN
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

        ----------------------
        -- Build the tables --
        ----------------------

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP

          -- Reset variables
          aux := NULL;
          -- before_queries := '{}';
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

                -- Check if the sequence already exists (may be used more than once)
                RAISE DEBUG '-- [SEQUENCES] TABLE: % TEST FOR SEQUENCE: %', object_name, prefix || aux;
                IF NOT EXISTS(SELECT 1 FROM to_regclass((schema_name || '.' || prefix || aux)::cstring) s WHERE s IS NOT NULL) THEN
                  RAISE DEBUG '-- [SEQUENCES] TABLE: % SEQUENCE: %', object_name, prefix || aux;

                  before_query := format('CREATE SEQUENCE %1$s.%2$s%3$I;', schema_name, prefix, aux);
                  -- RAISE DEBUG '%', before_query;
                  EXECUTE before_query;

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

          query := LEFT(query, length(query) - 2) || ');';
          -- RAISE DEBUG '%', query;

          EXECUTE query;

          FOREACH after_query IN ARRAY after_queries
          LOOP
            -- RAISE DEBUG '%', after_query;
            EXECUTE after_query;
          END LOOP;

        END LOOP;

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.create_shard_constraints(BIGINT, TEXT, TEXT, TEXT, TEXT, JSONB);
      CREATE OR REPLACE FUNCTION transfer.create_shard_constraints(
        company_id              BIGINT,
        template_schema_name    TEXT,
        schema_name             TEXT,
        template_prefix         TEXT DEFAULT '',
        prefix                  TEXT DEFAULT '',
        all_objects_data        JSONB DEFAULT NULL
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        object_data             JSON;
        qualified_object_name   TEXT;
        object_name             TEXT;
        json_object             JSON;
        query                   TEXT;
        name                    TEXT;
        aux                     TEXT;
      BEGIN

        IF all_objects_data IS NULL THEN
          -- Get the necessary data to create the new constraints
          SELECT
            json_object_agg(i.qualified_object_name,
              json_build_object(
                'constraints', i.constraints
              )
            )::JSONB INTO all_objects_data
          FROM sharding.get_tables_info(template_schema_name, template_prefix) i;
        END IF;

        ---------------------------
        -- Build the constraints --
        ---------------------------

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP

          object_name := regexp_replace(qualified_object_name, '^(?:' || template_schema_name || '\.' || template_prefix || ')?(.*)$', '\1');

          RAISE DEBUG '-- [CONSTRAINTS] TABLE: %', object_name;

          IF (object_data->>'constraints') IS NOT NULL THEN

            FOR json_object IN SELECT * FROM json_array_elements(object_data->'constraints') LOOP

              aux := regexp_replace(json_object->>'definition', 'company_id\s*=\s*\d+', format('company_id = %1$s', company_id));

              name := json_object->>'name';
              IF template_prefix <> '' THEN
                name := regexp_replace(name, template_prefix, prefix);
              END IF;

              FOREACH query IN ARRAY ARRAY[format('ALTER TABLE %1$s.%5$s%2$I ADD CONSTRAINT %3$I %4$s;',
                schema_name,
                object_name,
                name,
                aux,
                prefix
              )]
              LOOP
                -- RAISE DEBUG '%', query;
                EXECUTE query;
              END LOOP;
            END LOOP;
          END IF;
        END LOOP;

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.create_shard_indexes(TEXT, TEXT, TEXT, TEXT, JSONB);
      CREATE OR REPLACE FUNCTION transfer.create_shard_indexes(
        template_schema_name    TEXT,
        schema_name             TEXT,
        template_prefix         TEXT DEFAULT '',
        prefix                  TEXT DEFAULT '',
        all_objects_data        JSONB DEFAULT NULL
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        object_data             JSON;
        qualified_object_name   TEXT;
        object_name             TEXT;
        json_object             JSON;
        query                   TEXT;
        name                    TEXT;
      BEGIN

        IF all_objects_data IS NULL THEN
          -- Get the necessary data to create the new indexes
          SELECT
            json_object_agg(i.qualified_object_name,
              json_build_object(
                'indexes', i.indexes
              )
            )::JSONB INTO all_objects_data
          FROM sharding.get_tables_info(template_schema_name, template_prefix) i;
        END IF;

        -----------------------
        -- Build the indexes --
        -----------------------

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP

          object_name := regexp_replace(qualified_object_name, '^(?:' || template_schema_name || '\.' || template_prefix || ')?(.*)$', '\1');

          RAISE DEBUG '-- [INDEXES] TABLE: %', object_name;

          IF (object_data->>'indexes') IS NOT NULL THEN
            FOR json_object IN SELECT * FROM json_array_elements(object_data->'indexes') LOOP

              query := regexp_replace(json_object->>'definition', ' ON ' || template_schema_name || '\.' || template_prefix, format(' ON %1$s.%2$s', schema_name, prefix));
              IF template_prefix <> '' THEN
                query := regexp_replace(query, template_prefix, prefix);
              END IF;

              -- RAISE DEBUG '%', query;
              EXECUTE query;

              IF (json_object->>'is_primary')::BOOLEAN THEN
                name := json_object->>'name';
                IF template_prefix <> '' THEN
                  name := regexp_replace(name, template_prefix, prefix);
                END IF;
                query := format('ALTER TABLE %1$s.%5$s%2$I ADD CONSTRAINT %5$s%4$I PRIMARY KEY USING INDEX %3$I;', schema_name, object_name, name, format('%1$s_pkey', object_name), prefix);
                -- RAISE DEBUG '%', query;
                EXECUTE query;
              END IF;
            END LOOP;
          END IF;
        END LOOP;

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.create_shard_foreign_keys(TEXT, TEXT, TEXT, TEXT, JSONB);
      CREATE OR REPLACE FUNCTION transfer.create_shard_foreign_keys(
        template_schema_name    TEXT,
        schema_name             TEXT,
        template_prefix         TEXT DEFAULT '',
        prefix                  TEXT DEFAULT '',
        all_objects_data        JSONB DEFAULT NULL
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        object_data             JSON;
        qualified_object_name   TEXT;
        object_name             TEXT;
        json_object             JSON;
        query                   TEXT;
        name                    TEXT;
      BEGIN

        IF all_objects_data IS NULL THEN
          -- Get the necessary data to create the new foreign keys
          SELECT
            json_object_agg(i.qualified_object_name,
              json_build_object(
                'foreign_keys', i.foreign_keys
              )
            )::JSONB INTO all_objects_data
          FROM sharding.get_tables_info(template_schema_name, template_prefix) i;
        END IF;

        ----------------------------
        -- Build the foreign keys --
        ----------------------------

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP

          object_name := regexp_replace(qualified_object_name, '^(?:' || template_schema_name || '\.' || template_prefix || ')?(.*)$', '\1');

          RAISE DEBUG '-- [FOREIGN KEYS] TABLE: %', object_name;

          IF (object_data->>'foreign_keys') IS NOT NULL THEN
            FOR json_object IN SELECT * FROM json_array_elements(object_data->'foreign_keys') LOOP

              name := json_object->>'name';
              IF template_prefix <> '' THEN
                name := regexp_replace(name, template_prefix, prefix);
              END IF;

              -- It's a foreign key for the same shard, so we can replicate it
              FOREACH query IN ARRAY ARRAY[format('ALTER TABLE %1$s.%5$s%2$I ADD CONSTRAINT %3$I %4$s;',
                schema_name,
                object_name,
                name,
                regexp_replace(json_object->>'definition', 'REFERENCES (?:' || template_schema_name || '\.' || template_prefix || ')?', format('REFERENCES %1$s.%2$s', schema_name, prefix)),
                prefix
              )]
              LOOP
                -- RAISE DEBUG '%', query;
                EXECUTE query;
              END LOOP;
            END LOOP;
          END IF;
        END LOOP;

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.create_shard_triggers(TEXT, TEXT, TEXT, TEXT, JSONB);
      CREATE OR REPLACE FUNCTION transfer.create_shard_triggers(
        template_schema_name    TEXT,
        schema_name             TEXT,
        template_prefix         TEXT DEFAULT '',
        prefix                  TEXT DEFAULT '',
        all_objects_data        JSONB DEFAULT NULL
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        object_data             JSON;
        qualified_object_name   TEXT;
        object_name             TEXT;
        json_object             JSON;
        query                   TEXT;
      BEGIN

        IF all_objects_data IS NULL THEN
          -- Get the necessary data to create the new triggers
          SELECT
            json_object_agg(i.qualified_object_name,
              json_build_object(
                'triggers', i.triggers
              )
            )::JSONB INTO all_objects_data
          FROM sharding.get_tables_info(template_schema_name, template_prefix) i;
        END IF;

        ------------------------
        -- Build the triggers --
        ------------------------

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP

          object_name := regexp_replace(qualified_object_name, '^(?:' || template_schema_name || '\.' || template_prefix || ')?(.*)$', '\1');

          RAISE DEBUG '-- [TRIGGERS] TABLE: %', object_name;

          IF (object_data->>'triggers') IS NOT NULL THEN
            FOR json_object IN SELECT * FROM json_array_elements(object_data->'triggers') LOOP
              query := regexp_replace(
                json_object->>'definition',
                ' ON (?:' || template_schema_name || '\.' || template_prefix || ')?',
                format(' ON %1$s.%2$s', schema_name, prefix)
              );
              EXECUTE query;
            END LOOP;
          END IF;
        END LOOP;

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
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
        i                     INTEGER;
        excluded_prefix       TEXT;
      BEGIN

        template_schema_name := template_schema_names[1];
        schema_name := schema_names[1];

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

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.create_shard_non_table_objects(BIGINT, TEXT, TEXT, TEXT, TEXT, TEXT[]);
      CREATE OR REPLACE FUNCTION transfer.create_shard_non_table_objects(
        company_id              BIGINT,
        template_schema_name    TEXT,
        schema_name             TEXT,
        template_prefix         TEXT DEFAULT '',
        prefix                  TEXT DEFAULT '',
        excluded_prefixes       TEXT[] DEFAULT '{}'
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        all_objects_data        JSONB;
        query                   TEXT;
        excluded_prefix         TEXT;
      BEGIN

        -- Get the necessary data to create the new indexes
        query := FORMAT('
          SELECT
            json_object_agg(i.qualified_object_name,
              json_build_object(
                ''indexes'', i.indexes,
                ''constraints'', i.constraints,
                ''foreign_keys'', i.foreign_keys,
                ''triggers'', i.triggers
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

        PERFORM transfer.create_shard_indexes(template_schema_name, schema_name, template_prefix, prefix, all_objects_data);
        PERFORM transfer.create_shard_constraints(company_id, template_schema_name, schema_name, template_prefix, prefix, all_objects_data);
        PERFORM transfer.create_shard_foreign_keys(template_schema_name, schema_name, template_prefix, prefix, all_objects_data);
        PERFORM transfer.create_shard_triggers(template_schema_name, schema_name, template_prefix, prefix, all_objects_data);

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

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

        -- Return the companies schemas to include in the backup
        RETURN QUERY EXECUTE FORMAT('SELECT * FROM transfer.get_company_schemas_to_backup(%1$L)', company_id);

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
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
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.restore_after_before_execute(bigint, boolean);
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

        -- Return the companies schemas to include in the main restore
        RETURN QUERY EXECUTE FORMAT('SELECT * FROM unnest(%1$L::text[])', source_info.backed_up_schemas);

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.restore_after_execute(bigint);
      CREATE OR REPLACE FUNCTION transfer.restore_after_execute(
        company_id      bigint
      ) RETURNS VOID AS $BODY$
      DECLARE
        meta_schema                 text;
        source_info                 RECORD;
        query                       text;
        schema                      text;
        main_schema_template        text;
        accounting_schema_template  text;
        fiscal_year_template        text;
        accounting_schema           text;
        prefixes                    JSON;
        prefix                      text;
        excluded_prefixes           text[];
        schema_templates            text[];
        schemas                     text[];
      BEGIN

        -- Assert that the company was restored and is valid!
        PERFORM transfer.validate_company(company_id);

        EXECUTE
          FORMAT('SELECT * FROM transfer.get_meta_schema_name(%1$L)', company_id)
        INTO STRICT meta_schema;

        EXECUTE
          FORMAT('SELECT * FROM %1$s.info', meta_schema)
        INTO STRICT source_info;

        SELECT * FROM transfer.get_restore_templates(company_id)
        INTO main_schema_template, accounting_schema_template, fiscal_year_template;

        -----------------------------------------------------------------------------------------------
        -- Create the CONSTRAINTS, INDEXES, FOREIGN KEYS AND TRIGGERS for the tables being restored  --
        -----------------------------------------------------------------------------------------------

        -- MAIN company schema non-table objects
        RAISE NOTICE 'Creating non-table objects in schema %', source_info.main_schema;
        PERFORM transfer.create_shard_non_table_objects(company_id, main_schema_template, source_info.main_schema);

        -- ACCOUNTING companies schema non-table objects
        EXECUTE FORMAT('
          SELECT array_agg(table_prefix) from %1$s.fiscal_years
        ', accounting_schema_template)
        INTO STRICT excluded_prefixes;
        FOREACH accounting_schema IN ARRAY source_info.accounting_schemas LOOP
          RAISE NOTICE 'Creating (global) non-table objects in schema %', accounting_schema;
          PERFORM transfer.create_shard_non_table_objects(company_id, accounting_schema_template, accounting_schema, '', '', excluded_prefixes);
        END LOOP;

        -- Accounting companies FISCAL YEARS schema non-table objects
        FOR accounting_schema, prefixes IN SELECT * FROM json_each(source_info.fiscal_years) LOOP
          FOREACH prefix IN ARRAY ARRAY(SELECT trim(fy::text, '"') FROM json_array_elements(prefixes->'prefixes') fy) LOOP
            RAISE NOTICE 'Creating non-table objects in schema % with prefix %', accounting_schema, prefix;
            PERFORM transfer.create_shard_non_table_objects(company_id, accounting_schema_template, accounting_schema, fiscal_year_template, prefix);
          END LOOP;
        END LOOP;

        ------------------------
        -- Create the VIEWS   --
        ------------------------

        schema_templates := '{}';
        schema_templates := schema_templates || main_schema_template;
        schemas := '{}';
        schemas := schemas || source_info.main_schema;

        -- MAIN company schema views
        RAISE NOTICE 'Creating views in schema %', source_info.main_schema;
        PERFORM transfer.create_shard_views(schema_templates, schemas);

        -- ACCOUNTING companies schema views
        EXECUTE FORMAT('
          SELECT array_agg(table_prefix) from %1$s.fiscal_years
        ', accounting_schema_template)
        INTO STRICT excluded_prefixes;
        FOREACH accounting_schema IN ARRAY source_info.accounting_schemas LOOP
          schema_templates := '{}';
          schema_templates := schema_templates || accounting_schema_template;
          schema_templates := schema_templates || main_schema_template;
          schemas := '{}';
          schemas := schemas || accounting_schema;
          schemas := schemas || source_info.main_schema;
          RAISE NOTICE 'Creating (global) views in schema %', accounting_schema;
          PERFORM transfer.create_shard_views(schema_templates, schemas, '', '', excluded_prefixes);
        END LOOP;

        -- Accounting companies FISCAL YEARS schema views
        FOR accounting_schema, prefixes IN SELECT * FROM json_each(source_info.fiscal_years) LOOP
          schema_templates := '{}';
          schema_templates := schema_templates || accounting_schema_template;
          schema_templates := schema_templates || main_schema_template;
          schemas := '{}';
          schemas := schemas || accounting_schema;
          schemas := schemas || source_info.main_schema;
          FOREACH prefix IN ARRAY ARRAY(SELECT trim(fy::text, '"') FROM json_array_elements(prefixes->'prefixes') fy) LOOP
            RAISE NOTICE 'Creating views in schema % with prefix %', accounting_schema, prefix;
            PERFORM transfer.create_shard_views(schema_templates, schemas, fiscal_year_template, prefix);
          END LOOP;
        END LOOP;

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

  end

  def down

    execute <<-'SQL'
      DROP SCHEMA transfer CASCADE;
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS sharding.get_tables_info(TEXT, TEXT);
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS sharding.get_views_info(TEXT, TEXT);
    SQL

  end
end
