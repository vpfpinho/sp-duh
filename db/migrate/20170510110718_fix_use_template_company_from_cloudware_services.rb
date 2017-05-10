class FixUseTemplateCompanyFromCloudwareServices < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.create_shard_constraints(BIGINT, TEXT, TEXT, TEXT, TEXT, JSONB);
      CREATE OR REPLACE FUNCTION transfer.create_shard_constraints(
        company_id              BIGINT,
        template_company_id     BIGINT,
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
              name := replace(name, format('''%1$s''', template_company_id), format('''%1$s''', company_id));
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
      DROP FUNCTION IF EXISTS transfer.create_shard_non_table_objects(BIGINT, TEXT, TEXT, TEXT, TEXT, TEXT[]);
      CREATE OR REPLACE FUNCTION transfer.create_shard_non_table_objects(
        company_id              BIGINT,
        template_company_id     BIGINT,
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
        PERFORM transfer.create_shard_constraints(company_id, template_company_id, template_schema_name, schema_name, template_prefix, prefix, all_objects_data);
        PERFORM transfer.create_shard_foreign_keys(template_schema_name, schema_name, template_prefix, prefix, all_objects_data);
        PERFORM transfer.create_shard_triggers(template_schema_name, schema_name, template_prefix, prefix, all_objects_data);

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.restore_after_execute(bigint);
      CREATE OR REPLACE FUNCTION transfer.restore_after_execute(
        company_id            bigint,
        template_company_id   bigint
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

        SELECT * FROM transfer.get_restore_templates(company_id, template_company_id)
        INTO main_schema_template, accounting_schema_template, fiscal_year_template;

        -----------------------------------------------------------------------------------------------
        -- Create the CONSTRAINTS, INDEXES, FOREIGN KEYS AND TRIGGERS for the tables being restored  --
        -----------------------------------------------------------------------------------------------

        -- MAIN company schema non-table objects
        RAISE NOTICE 'Creating non-table objects in schema %', source_info.main_schema;
        PERFORM transfer.create_shard_non_table_objects(company_id, template_company_id, main_schema_template, source_info.main_schema);

        -- ACCOUNTING companies schema non-table objects
        EXECUTE FORMAT('
          SELECT array_agg(table_prefix) from %1$s.fiscal_years
        ', accounting_schema_template)
        INTO STRICT excluded_prefixes;
        FOREACH accounting_schema IN ARRAY source_info.accounting_schemas LOOP
          RAISE NOTICE 'Creating (global) non-table objects in schema %', accounting_schema;
          PERFORM transfer.create_shard_non_table_objects(company_id, template_company_id, accounting_schema_template, accounting_schema, '', '', excluded_prefixes);
        END LOOP;

        -- Accounting companies FISCAL YEARS schema non-table objects
        FOR accounting_schema, prefixes IN SELECT * FROM json_each(source_info.fiscal_years) LOOP
          FOREACH prefix IN ARRAY ARRAY(SELECT trim(fy::text, '"') FROM json_array_elements(prefixes->'prefixes') fy) LOOP
            RAISE NOTICE 'Creating non-table objects in schema % with prefix %', accounting_schema, prefix;
            PERFORM transfer.create_shard_non_table_objects(company_id, template_company_id, accounting_schema_template, accounting_schema, fiscal_year_template, prefix);
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
      DROP FUNCTION IF EXISTS transfer.restore_after_execute(bigint, bigint);
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
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.create_shard_constraints(BIGINT, BIGINT, TEXT, TEXT, TEXT, TEXT, JSONB);
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
        template_company_id     integer;
        object_data             JSON;
        qualified_object_name   TEXT;
        object_name             TEXT;
        json_object             JSON;
        query                   TEXT;
        name                    TEXT;
        aux                     TEXT;
      BEGIN

        template_company_id := split_part(template_schema_name, '_c', 2)::integer;

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
              name := replace(name, format('''%1$s''', template_company_id), format('''%1$s''', company_id));
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
      DROP FUNCTION IF EXISTS transfer.create_shard_non_table_objects(BIGINT, BIGINT, TEXT, TEXT, TEXT, TEXT, TEXT[]);
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
  end
end
