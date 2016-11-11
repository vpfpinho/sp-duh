class AddShardingStoredProcedures < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.check_record_existence(
        IN p_table_name TEXT,
        IN p_columns_and_values JSONB
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        record_exists BOOLEAN;
        clauses TEXT;
        clause_fields TEXT[];

        clause_format_expression TEXT;
        clause_columns_extract_expression TEXT;
        clause_columns_definition_expression TEXT;
      BEGIN
        record_exists := FALSE;

        -- raise notice 'sharding.check_record_existence(''%'', ''%'');', p_table_name, p_columns_and_values;

        clause_fields := (SELECT array_agg(jsonb_object_keys) FROM jsonb_object_keys(p_columns_and_values));

        SELECT
          array_to_string(array_agg(format('%1$s TEXT', unnest)), ', ')
        INTO
          clause_columns_definition_expression
        FROM unnest(clause_fields);

        SELECT
          format('''(%1$s)'', %2$s', array_to_string(array_agg('%' || i || '$L'), ', '), array_to_string(array_agg(field), ', ')),
          array_to_string(
            array_agg(format(
              '(SELECT jsonb_array_elements_text(%1$s::JSONB) AS %1$s FROM jsonb_to_record($1) AS data(%2$s)) data_%1$s',
              field,
              clause_columns_definition_expression
            )),
            E'\nCROSS JOIN '
          )
        INTO
          clause_format_expression,
          clause_columns_extract_expression
        FROM unnest(clause_fields) WITH ORDINALITY AS fields(field, i);

        EXECUTE format($$SELECT
          array_to_string((
            SELECT array_agg(format(%1$s))
            FROM %2$s
          ),
          ', ')
        $$,
          clause_format_expression,
          clause_columns_extract_expression,
          clause_columns_definition_expression,
          p_columns_and_values
        ) INTO clauses USING p_columns_and_values;

        EXECUTE format('SELECT EXISTS (SELECT 1 FROM %1$s WHERE (%2$s) IN (%3$s))', p_table_name, array_to_string(clause_fields, ', '), clauses) INTO record_exists;

        RETURN record_exists;

      EXCEPTION
        WHEN OTHERS THEN
          RETURN false;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.create_safety_triggers_for_sharded_companies()
      RETURNS VOID AS $BODY$
      DECLARE
        _table_name TEXT;
        query TEXT;
      BEGIN

        FOR _table_name IN (
          SELECT c.table_name
          FROM information_schema.columns c
            JOIN information_schema.tables t
              ON c.table_schema = t.table_schema
                AND c.table_name = t.table_name
                AND t.table_type = 'BASE TABLE'
          WHERE c.column_name = 'company_id'
            AND c.table_schema = 'public'
            AND NOT ( sharding.get_auxiliary_table_information()->'unsharded_tables' ? c.table_name )
        ) LOOP
          query := format('DROP TRIGGER IF EXISTS trg_prevent_insert_or_update_on_sharded_companies ON public.%1$I CASCADE', _table_name);
          RAISE NOTICE 'query: %', query;
          EXECUTE query;
          query := format($$
            CREATE TRIGGER trg_prevent_insert_or_update_on_sharded_companies
              BEFORE INSERT OR UPDATE ON public.%1$I
              FOR EACH ROW
              EXECUTE PROCEDURE sharding.trf_prevent_changes_on_sharded_tables_for_sharded_companies();
          $$, _table_name);
          RAISE NOTICE 'query: %', query;
          EXECUTE query;
        END LOOP;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.create_virtual_foreign_key(
        IN p_referencing_table TEXT, -- p_destination_schema_name.p_template_table_name
        IN p_referencing_columns TEXT[], -- aux_array[1]
        IN p_referenced_table TEXT, -- aux_array[2]
        IN p_referenced_columns TEXT[], -- aux_array[3]
        IN p_template_fk_name TEXT DEFAULT NULL, -- catalog_info.conname
        IN p_update_condition TEXT DEFAULT 'RESTRICT',
        IN p_delete_condition TEXT DEFAULT 'RESTRICT',
        IN p_check_single_table BOOLEAN DEFAULT false
      )
      RETURNS VOID AS $BODY$
      DECLARE
        query TEXT;
      BEGIN

        FOR query IN SELECT unnest(sharding.get_create_virtual_foreign_key_queries(
          p_referencing_table,
          p_referencing_columns,
          p_referenced_table,
          p_referenced_columns,
          p_template_fk_name,
          p_update_condition,
          p_delete_condition,
          p_check_single_table
        )) LOOP
          EXECUTE query;
        END LOOP;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.create_virtual_polymorphic_foreign_key(
        IN p_referencing_table TEXT,
        IN p_referenced_table TEXT,
        IN p_column_mappings JSONB, -- { "local_col_a": "remote_col_a", "local_col_b": "remote_col_b", "local_col_c": null }
        IN p_template_fk_name TEXT DEFAULT NULL,
        IN p_update_condition "char" DEFAULT NULL,
        IN p_delete_condition "char" DEFAULT NULL,
        IN p_trigger_conditions JSONB DEFAULT NULL -- { "local_col_c": [ "value_a", "value_b" ] }
      )
      RETURNS VOID AS $BODY$
      DECLARE
        query TEXT;
      BEGIN

        FOR query IN SELECT unnest(sharding.get_create_virtual_polymorphic_foreign_key_queries(
          p_referencing_table,
          p_referenced_table,
          p_column_mappings,
          p_template_fk_name,
          p_update_condition,
          p_delete_condition,
          p_trigger_conditions
        )) LOOP
          -- RAISE 'query: %', query;

          EXECUTE query;
        END LOOP;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.generate_create_company_shard_function(
        IN p_use_original_sequence BOOLEAN DEFAULT TRUE
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        auxiliary_table_information JSONB;

        all_objects_data JSONB;
        object_data JSON;
        qualified_object_name TEXT;
        schema_name TEXT;
        object_name TEXT;
        aux TEXT;
        aux_array TEXT[];
        col_default_value TEXT;

        json_object JSON;

        queries TEXT[];
        query TEXT;
        before_query TEXT;
        after_queries TEXT[];
        p_destination_schema_name TEXT;
        shard_company_id TEXT;
      BEGIN
        p_destination_schema_name := '%1$I';
        shard_company_id := '%2$L';

        -- Let's grant that we will use original sequences (temporary hack)
        p_use_original_sequence := TRUE;

        auxiliary_table_information = sharding.get_auxiliary_table_information();

        -- Get the necessary data to create the new tables, indexes, stored procedures and triggers
        WITH table_columns AS (
          SELECT
            t.tablename AS object_name,
            format('%1$I.%2$I', t.schemaname, t.tablename) AS qualified_object_name,
            (t.schemaname || '.' || t.tablename)::regclass::oid AS table_oid,
            json_agg(json_build_object(
              'name', a.attname,
              'type', pg_catalog.format_type(a.atttypid, a.atttypmod),
              'default_value', (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
              'is_not_null', a.attnotnull
            ) ORDER BY a.attnum)::JSONB AS columns
          FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_tables t ON c.oid = (t.schemaname || '.' || t.tablename)::regclass::oid
          WHERE a.attnum > 0
            AND NOT a.attisdropped
            AND n.nspname = 'public'
          GROUP BY t.schemaname, t.tablename
        ),
        table_indexes AS (
          SELECT
            format('%1$I.%2$I', t.schemaname, t.tablename) AS qualified_object_name,
            (t.schemaname || '.' || t.tablename)::regclass::oid AS table_oid,
            json_agg(json_build_object(
              'name', c2.relname,
              'is_primary', i.indisprimary,
              'is_unique', i.indisunique,
              'definition', pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
              'constraint_definition', pg_catalog.pg_get_constraintdef(con.oid, true)
            )::JSONB) AS indexes
          FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_index i ON c.oid = i.indrelid
            JOIN pg_catalog.pg_class c2 ON i.indexrelid = c2.oid
            LEFT JOIN pg_catalog.pg_constraint con ON (con.conrelid = i.indrelid AND con.conindid = i.indexrelid AND con.contype IN ('p','u','x'))
            JOIN pg_catalog.pg_tables t ON c.oid = (t.schemaname || '.' || t.tablename)::regclass::oid
          WHERE t.schemaname = 'public'
          GROUP BY t.schemaname, t.tablename
        ),
        table_foreign_keys AS (
          SELECT
            format('%1$I.%2$I', t.schemaname, t.tablename) AS qualified_object_name,
            (t.schemaname || '.' || t.tablename)::regclass::oid AS table_oid,
            json_agg(json_build_object(
              'name', c.conname,
              'update_action', c.confupdtype,
              'delete_action', c.confdeltype,
              'definition', pg_catalog.pg_get_constraintdef(c.oid, true)
            )::JSONB) AS foreign_keys
          FROM pg_catalog.pg_constraint c
            LEFT JOIN pg_catalog.pg_tables t ON c.conrelid = (t.schemaname || '.' || t.tablename)::regclass::oid
          WHERE c.contype = 'f'
            AND t.schemaname = 'public'
          GROUP BY t.schemaname, t.tablename
        ),
        table_triggers AS (
          SELECT
            format('%1$I.%2$I', ta.schemaname, ta.tablename) AS qualified_object_name,
            (ta.schemaname || '.' || ta.tablename)::regclass::oid AS table_oid,
            json_agg(json_build_object(
              'name', t.tgname,
              'definition', pg_catalog.pg_get_triggerdef(t.oid, true)
            )::JSONB) AS triggers
          FROM pg_catalog.pg_trigger t
            LEFT JOIN pg_catalog.pg_tables ta ON t.tgrelid = (ta.schemaname || '.' || ta.tablename)::regclass::oid
          WHERE ta.schemaname = 'public'
            AND (NOT t.tgisinternal OR (t.tgisinternal AND t.tgenabled = 'D'))
            AND t.tgname != 'trg_prevent_insert_or_update_on_sharded_companies' -- Don't copy the prevent trigger for sharded companies
            AND t.tgname !~* '^trg_vfk(?:i|p)?r' -- Don't copy the virtual foreign key reverse triggers
          GROUP BY ta.schemaname, ta.tablename
        )
        SELECT
          json_object_agg(c.qualified_object_name,
            json_build_object(
              'columns', c.columns,
              'indexes', i.indexes,
              'foreign_keys', fk.foreign_keys,
              'triggers', trg.triggers
            )
          )::JSONB INTO all_objects_data
        FROM table_columns c
          LEFT JOIN table_indexes i ON c.table_oid = i.table_oid
          LEFT JOIN table_foreign_keys fk ON c.table_oid = fk.table_oid
          LEFT JOIN table_triggers trg ON c.table_oid = trg.table_oid
        WHERE c.object_name::TEXT NOT IN (
          SELECT jsonb_array_elements_text FROM jsonb_array_elements_text(auxiliary_table_information->'unsharded_tables')
        );

        ----------------------
        -- Build the tables --
        ----------------------

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP
          -- Reset variables
          aux := NULL;
          before_query := NULL;
          after_queries := '{}';

          object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');
          -- RAISE NOTICE 'object_name: %', object_name;

          queries := queries || format('RAISE NOTICE ''-- [TABLES] TABLE: %1$I'';', object_name);

          query := format('CREATE TABLE %1$s.%2$I (', p_destination_schema_name, object_name);

          FOR json_object IN SELECT * FROM json_array_elements(object_data->'columns') LOOP
            -- RAISE NOTICE 'column: %', json_object;
            col_default_value := NULL;

            IF NOT p_use_original_sequence AND (json_object->>'default_value') IS NOT NULL AND json_object->>'default_value' ~ '^nextval\('
               -- we cannot use specific sequence on inherited tables (otherwise values will collide on parent table)
               AND NOT auxiliary_table_information->'inherited_tables' ? object_name THEN
              -- Need to create a new sequence for the primary key
              aux := regexp_replace(json_object->>'default_value', 'nextval\(''(?:.+\.)?(.*)''.*', '\1');

              col_default_value := format('nextval(''%1$s.%2$s''::regclass)', p_destination_schema_name, aux);

              before_query := format('CREATE SEQUENCE %1$s.%2$I;', p_destination_schema_name, aux);
              after_queries := after_queries
                            || format('ALTER SEQUENCE %1$s.%2$I OWNED BY %1$s.%3$I.%4$I;', p_destination_schema_name, aux, object_name, json_object->>'name')
                            || format('EXECUTE ''SELECT last_value FROM public.%1$I'' INTO seq_nextval;', aux)
                            || format('EXECUTE format(''ALTER SEQUENCE %%1$s.%1$I RESTART WITH %%2$s'', p_company_schema_name, seq_nextval);', aux);
            END IF;

            IF col_default_value IS NULL THEN
              col_default_value := json_object->>'default_value';
            END IF;

            query := query || format('%1$I %2$s%3$s%4$s%5$s, ',
              json_object->>'name',
              json_object->>'type',
              CASE WHEN (json_object->>'is_not_null')::BOOLEAN THEN ' NOT NULL' END,
              CASE WHEN col_default_value IS NOT NULL THEN format(' DEFAULT %1$s', col_default_value) END,
              CASE WHEN json_object->>'name' = 'company_id' THEN format(' CONSTRAINT "company_id must equal %1$s" CHECK (company_id = %1$s)', shard_company_id) END
            );

          END LOOP;

          query := LEFT(query, length(query) - 2);

          if auxiliary_table_information->'inherited_tables' ? object_name THEN
            query := query || format(') INHERITS (%1$s);', qualified_object_name);
          ELSE
            query := query || ');';
          END IF;

          IF before_query IS NOT NULL THEN
            queries := queries || before_query;
          END IF;

          queries := queries || query || after_queries;

          -- raise notice 'query: %', query;
        END LOOP;

        -----------------------
        -- Build the indexes --
        -----------------------

        queries := queries || '{ -- Create indexes }'::TEXT[];

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP
          -- Reset variables
          aux := NULL;

          object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');
          -- RAISE NOTICE 'object_name: %', object_name;

          queries := queries || format('RAISE NOTICE ''-- [INDEXES] TABLE: %1$I'';', object_name);

          IF (object_data->>'indexes') IS NOT NULL THEN
            FOR json_object IN SELECT * FROM json_array_elements(object_data->'indexes') LOOP
              queries := queries || format('%1$s;', regexp_replace(json_object->>'definition', ' ON (?:.+\.)?', format(' ON %1$s.', p_destination_schema_name)));

              IF (json_object->>'is_primary')::BOOLEAN THEN
                queries := queries || format('ALTER TABLE %1$s.%2$I ADD CONSTRAINT %4$I PRIMARY KEY USING INDEX %3$I;', p_destination_schema_name, object_name, json_object->>'name', format('%1$s_pkey', object_name));
              END IF;
            END LOOP;
          END IF;
        END LOOP;

        ----------------------------
        -- Build the foreign keys --
        ----------------------------

        queries := queries || '{ -- Create foreign keys }'::TEXT[];

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP
          -- Reset variables
          aux := NULL;

          object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');
          schema_name := COALESCE(regexp_replace(qualified_object_name, '^(?:(.+)\.)?(?:.*)$', '\1'), 'public');
          -- RAISE NOTICE 'object_name: %', object_name;

          queries := queries || format('RAISE NOTICE ''-- [FOREIGN KEYS] TABLE: %1$I'';', object_name);

          IF (object_data->>'foreign_keys') IS NOT NULL THEN
            RAISE NOTICE '% foreign_keys: %', object_name, object_data->'foreign_keys';

            FOR json_object IN SELECT * FROM json_array_elements(object_data->'foreign_keys') LOOP

              -- Get the name of the referenced table
              aux := (regexp_matches(json_object->>'definition', 'REFERENCES (?:.*?\.)?(.*?)\('))[1];

              -- If the referenced table is in the unsharded tables list, we need to create some 'virtual' foreign keys via triggers
              IF auxiliary_table_information->'unsharded_tables' ? aux THEN
                -- aux_array[1] = local columns
                -- aux_array[2] = referenced table
                -- aux_array[3] = referenced columns
                aux_array := regexp_matches(json_object->>'definition', 'FOREIGN KEY \((.*?)\) REFERENCES ((?:.*?\.)?.*?)\((.*?)\)');

                queries := queries || sharding.get_create_virtual_foreign_key_queries(
                  format('%1$s.%2$I', p_destination_schema_name, object_name),
                  regexp_split_to_array(aux_array[1], '\s*,\s*')::TEXT[],
                  aux_array[2],
                  regexp_split_to_array(aux_array[3], '\s*,\s*')::TEXT[],
                  json_object->>'name',
                  (json_object->>'update_action')::"char",
                  (json_object->>'delete_action')::"char"
                );
              -- If the referenced table is in the inherited tables list, we need to create some 'virtual' foreign keys via triggers
              -- to both the parent and the child table
              ELSIF auxiliary_table_information->'inherited_tables' ? aux THEN
                -- aux_array[1] = local columns
                -- aux_array[2] = referenced table
                -- aux_array[3] = referenced columns
                aux_array := regexp_matches(json_object->>'definition', 'FOREIGN KEY \((.*?)\) REFERENCES (?:.*?\.)?(.*?)\((.*?)\)');

                -- RAISE NOTICE 'aux_array: %', aux_array;

                queries := queries || sharding.get_create_virtual_foreign_key_to_inherited_table_queries(
                  format('%1$s.%2$I', p_destination_schema_name, object_name),
                  format('%1$s.%2$I', schema_name, aux_array[2]),
                  format('%1$s.%2$I', p_destination_schema_name, aux_array[2]),
                  regexp_split_to_array(aux_array[1], '\s*,\s*')::TEXT[],
                  regexp_split_to_array(aux_array[3], '\s*,\s*')::TEXT[],
                  json_object->>'name',
                  (json_object->>'update_action')::"char",
                  (json_object->>'delete_action')::"char"
                );
              ELSE
                -- It's a foreign key for the same shard, so we can replicate it
                queries := queries || ARRAY[format('ALTER TABLE %1$s.%2$I ADD CONSTRAINT %3$I %4$s;',
                  p_destination_schema_name,
                  object_name,
                  json_object->>'name',
                  regexp_replace(json_object->>'definition', 'REFERENCES (?:.*?\.)?', format('REFERENCES %1$s.', p_destination_schema_name))
                )];
              END IF;
            END LOOP;
          END IF;
        END LOOP;

        ------------------------
        -- Build the triggers --
        ------------------------

        queries := queries || '{ -- Create triggers }'::TEXT[];

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP
          -- Reset variables
          aux := NULL;

          object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');
          -- RAISE NOTICE 'object_name: %', object_name;

          queries := queries || format('RAISE NOTICE ''-- [TRIGGERS] TABLE: %1$I'';', object_name);

          IF (object_data->>'triggers') IS NOT NULL THEN
            FOR json_object IN SELECT * FROM json_array_elements(object_data->'triggers') LOOP
              -- Just replace the name of the table. The executed procedure will NOT be replicated, but should handle the different schemas
              queries := queries || regexp_replace(
                json_object->>'definition',
                ' ON (?:\S+?\.)?',
                format(' ON %1$s.', p_destination_schema_name)
              );
            END LOOP;
          END IF;
        END LOOP;

        ---------------------
        -- Build the views --
        ---------------------

        FOR qualified_object_name, aux IN
          SELECT
            format('%1$I.%2$I', v.schemaname, v.viewname),
            pg_catalog.pg_get_viewdef(c.oid)
          FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_views v ON c.oid = (v.schemaname || '.' || v.viewname)::regclass::oid
          WHERE n.nspname = 'public'
        LOOP
          object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');
          RAISE NOTICE 'qualified_object_name: %', qualified_object_name;

          queries := queries || format('CREATE VIEW %1$s.%2$I AS %3$s;',
            p_destination_schema_name,
            object_name,
            aux
          );

        END LOOP;

        --------------------------------------------
        -- Build virtual polymorphic foreign keys --
        --------------------------------------------

        -- paperclip_database_storage_attachments
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.paperclip_database_storage_attachments', 'public.users', '{ "attached_type": null, "attached_id": "id" }', NULL, 'r', 'r', '{ "attached_type": ["Manager", "Accountant", "User", "Employee"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.paperclip_database_storage_attachments', 'public.companies', '{ "attached_type": null, "attached_id": "id" }', NULL, 'r', 'r', '{ "attached_type": ["Company"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.paperclip_database_storage_attachments', '%1$I.archives', '{ "attached_type": null, "attached_id": "id" }', NULL, 'r', 'r', '{ "attached_type": ["Archive"] }'));

        -- company_certificates
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.company_certificates', 'public.companies', '{ "entity_type": null, "entity_id": "id" }', NULL, 'r', 'r', '{ "entity_type": ["Company"] }'));

        -- email_addresses
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.email_addresses', '%1$I.suppliers', '{ "email_addressable_type": null, "email_addressable_id": "id" }', NULL, 'r', 'r', '{ "email_addressable_type": ["Supplier"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.email_addresses', '%1$I.customers', '{ "email_addressable_type": null, "email_addressable_id": "id" }', NULL, 'r', 'r', '{ "email_addressable_type": ["Customer"] }'));

        -- contacts
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.contacts', '%1$I.suppliers', '{ "contactable_type": null, "contactable_id": "id" }', NULL, 'r', 'r', '{ "contactable_type": ["Supplier"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.contacts', '%1$I.customers', '{ "contactable_type": null, "contactable_id": "id" }', NULL, 'r', 'r', '{ "contactable_type": ["Customer"] }'));

        -- public_links
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.public_links', '%1$I.documents', '{ "entity_type": null, "entity_id": "id" }', NULL, 'r', 'r', '{ "entity_type": ["Document"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.public_links', '%1$I.receipts', '{ "entity_type": null, "entity_id": "id" }', NULL, 'r', 'r', '{ "entity_type": ["Receipt"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.public_links', '%1$I.customers', '{ "entity_type": null, "entity_id": "id" }', NULL, 'r', 'r', '{ "entity_type": ["DueDocument"] }'));

        -- addresses
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.addresses', 'public.companies', '{ "addressable_type": null, "addressable_id": "id" }', NULL, 'r', 'r', '{ "addressable_type": ["Company"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.addresses', '%1$I.suppliers', '{ "addressable_type": null, "addressable_id": "id" }', NULL, 'r', 'r', '{ "addressable_type": ["Supplier"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.addresses', '%1$I.customers', '{ "addressable_type": null, "addressable_id": "id" }', NULL, 'r', 'r', '{ "addressable_type": ["Customer"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.addresses', 'public.users', '{ "addressable_type": null, "addressable_id": "id" }', NULL, 'r', 'r', '{ "addressable_type": ["Manager", "Accountant", "User", "Employee"] }'));

        -- purchases_workflows
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.purchases_workflows', '%1$I.purchases_mission_maps', '{ "entity_type": null, "entity_id": "id" }', NULL, 'r', 'r', '{ "entity_type": ["Purchases::MissionMap"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.purchases_workflows', '%1$I.purchases_documents', '{ "entity_type": null, "entity_id": "id" }', NULL, 'r', 'r', '{ "entity_type": ["Purchases::Document"] }'));

        -- stocks_stockable_data
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.stocks_stockable_data', '%1$I.items', '{ "stockable_type": null, "stockable_id": "id" }', NULL, 'r', 'r', '{ "stockable_type": ["Item", "Product"] }'));

        -- stocks_stock_movements
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.stocks_stock_movements', '%1$I.documents', '{ "stock_affector_type": null, "stock_affector_id": "id" }', NULL, 'r', 'r', '{ "stock_affector_type": ["Document"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.stocks_stock_movements', '%1$I.document_lines', '{ "stock_affector_detail_type": null, "stock_affector_detail_id": "id" }', NULL, 'r', 'r', '{ "stock_affector_detail_type": ["DocumentLine"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.stocks_stock_movements', '%1$I.purchases_documents', '{ "stock_affector_type": null, "stock_affector_id": "id" }', NULL, 'r', 'r', '{ "stock_affector_type": ["Purchases::Document"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.stocks_stock_movements', '%1$I.purchases_document_lines', '{ "stock_affector_detail_type": null, "stock_affector_detail_id": "id" }', NULL, 'r', 'r', '{ "stock_affector_detail_type": ["Purchases::DocumentLine"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.stocks_stock_movements', '%1$I.stocks_documents', '{ "stock_affector_type": null, "stock_affector_id": "id" }', NULL, 'r', 'r', '{ "stock_affector_type": ["Stocks::Document"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.stocks_stock_movements', '%1$I.stocks_document_lines', '{ "stock_affector_detail_type": null, "stock_affector_detail_id": "id" }', NULL, 'r', 'r', '{ "stock_affector_detail_type": ["Stocks::DocumentLine"] }'));

        -- settlements
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.settlements', '%1$I.documents', '{ "associated_type": null, "associated_id": "id" }', NULL, 'r', 'r', '{ "associated_type": ["Document"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.settlements', '%1$I.document_lines', '{ "associated_type": null, "associated_id": "id" }', NULL, 'r', 'r', '{ "associated_type": ["DocumentLine"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.settlements', '%1$I.purchases_documents', '{ "associated_type": null, "associated_id": "id" }', NULL, 'r', 'r', '{ "associated_type": ["Purchases::Document"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.settlements', '%1$I.purchases_document_lines', '{ "associated_type": null, "associated_id": "id" }', NULL, 'r', 'r', '{ "associated_type": ["Purchases::DocumentLine"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.settlements', '%1$I.payment_lines', '{ "associated_type": null, "associated_id": "id" }', NULL, 'r', 'r', '{ "associated_type": ["PaymentLine"] }'));

        -- applied_taxes
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.applied_taxes', '%1$I.documents', '{ "taxable_type": null, "taxable_id": "id" }', NULL, 'r', 'r', '{ "taxable_type": ["Document"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.applied_taxes', '%1$I.receipts', '{ "taxable_type": null, "taxable_id": "id" }', NULL, 'r', 'r', '{ "taxable_type": ["Receipt"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.applied_taxes', '%1$I.receipt_lines', '{ "taxable_type": null, "taxable_id": "id" }', NULL, 'r', 'r', '{ "taxable_type": ["ReceiptLine"] }'));

        -- applied_vats
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.applied_vats', '%1$I.documents', '{ "reference_type": null, "reference_id": "id" }', NULL, 'r', 'r', '{ "reference_type": ["Document"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.applied_vats', '%1$I.document_lines', '{ "reference_type": null, "reference_id": "id" }', NULL, 'r', 'r', '{ "reference_type": ["DocumentLine"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.applied_vats', '%1$I.purchases_documents', '{ "reference_type": null, "reference_id": "id" }', NULL, 'r', 'r', '{ "reference_type": ["Purchases::Document"] }'));
        queries := queries || (SELECT sharding.get_create_virtual_polymorphic_foreign_key_queries('%1$I.applied_vats', '%1$I.purchases_document_lines', '{ "reference_type": null, "reference_id": "id" }', NULL, 'r', 'r', '{ "reference_type": ["Purchases::DocumentLine"] }'));

        --------------------------------
        -- Create the actual function --
        --------------------------------

        query := format($$
          CREATE OR REPLACE FUNCTION sharding.create_company_shard(
            IN p_company_id INTEGER,
            IN p_company_schema_name TEXT
          )
          RETURNS BOOLEAN AS $FUNCTION_BODY$
          DECLARE
            query TEXT;
            seq_nextval BIGINT;
          BEGIN

            %1$s

            RETURN TRUE;
          END;
          $FUNCTION_BODY$ LANGUAGE 'plpgsql';
        $$,
          (
            SELECT string_agg(
              CASE WHEN unnest ~* '^(?:--|RAISE|EXECUTE)'
              THEN format(E'\n      %1$s', unnest)
              ELSE format(E'EXECUTE format(%1$L, p_company_schema_name, p_company_id);', regexp_replace(unnest, '\s+', ' ', 'g'))
              -- Switch this with the previous one for debug
              -- ELSE format(E'query := format(%1$L, p_company_schema_name, p_company_id);\n      RAISE NOTICE ''query: %%'', query;\n      EXECUTE query;', regexp_replace(unnest, '\s+', ' ', 'g'))
              END, E'\n      '
            )
            FROM unnest(queries)
          )
        );

        RAISE NOTICE 'query: %', query;

        EXECUTE query;

        RETURN TRUE;
      -- EXCEPTION
      --   WHEN OTHERS THEN
      --     RETURN false;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.get_auxiliary_table_information(
        OUT auxiliary_table_information JSONB
      )
      RETURNS JSONB AS $BODY$
      BEGIN
        auxiliary_table_information = '{
          "unsharded_tables": [],
          "inherited_tables": []
        }'::JSONB;

        RETURN;
      END;
      $BODY$ LANGUAGE 'plpgsql' STABLE;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.get_column_default_value(
        IN p_table_name TEXT,
        IN p_column_name TEXT,
        IN p_table_schema TEXT DEFAULT 'public'
      )
      RETURNS TEXT AS $BODY$
      BEGIN
        RETURN (
          SELECT d.adsrc AS default_value
          FROM   pg_catalog.pg_attribute a
          LEFT   JOIN pg_catalog.pg_attrdef d ON (a.attrelid, a.attnum)
                                               = (d.adrelid,  d.adnum)
          WHERE  NOT a.attisdropped   -- no dropped (dead) columns
          AND    a.attnum > 0         -- no system columns
          AND    a.attrelid = (p_table_schema || '.' || p_table_name)::regclass
          AND    a.attname = p_column_name
        );
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.get_convert_foreign_keys_from_public_to_sharded_tables_queries(
        OUT queries TEXT[]
      )
      RETURNS TEXT[] AS $BODY$
      DECLARE
        aux TEXT;
        all_objects_data JSONB;
        qualified_object_name TEXT;
        object_data JSONB;
        foreign_key JSONB;
        referenced_table TEXT;
        aux_array TEXT[];
        update_action "char";
        delete_action "char";
      BEGIN
        SELECT
          json_object_agg(fk.qualified_object_name,
            fk.foreign_keys
          )::JSONB INTO all_objects_data
        FROM (
          SELECT
            format('%1$I.%2$I', t.schemaname, t.tablename) AS qualified_object_name,
            (t.schemaname || '.' || t.tablename)::regclass::oid AS table_oid,
            json_agg(json_build_object(
              'name', c.conname,
              'update_action', c.confupdtype,
              'delete_action', c.confdeltype,
              'definition', pg_catalog.pg_get_constraintdef(c.oid, true)
            )::JSONB) AS foreign_keys
          FROM pg_catalog.pg_constraint c
            LEFT JOIN pg_catalog.pg_tables t ON c.conrelid = (t.schemaname || '.' || t.tablename)::regclass::oid
          WHERE c.contype = 'f'
            AND t.schemaname = 'public'
            AND t.tablename IN (
                SELECT jsonb_array_elements_text FROM jsonb_array_elements_text(sharding.get_auxiliary_table_information()->'unsharded_tables')
            )
          GROUP BY t.schemaname, t.tablename
        ) fk;

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP
          -- RAISE NOTICE '%: %', qualified_object_name, object_data;

          FOR foreign_key IN SELECT * FROM jsonb_array_elements(object_data) LOOP
            aux_array := regexp_matches(foreign_key->>'definition', 'FOREIGN KEY \((.*?)\) REFERENCES (?:.*?\.)?(.*?)\((.*?)\)');

            IF NOT sharding.get_auxiliary_table_information()->'unsharded_tables' ? aux_array[2] THEN
              update_action := foreign_key->>'update_action'::"char";
              delete_action := foreign_key->>'delete_action'::"char";

              queries := queries || sharding.get_create_virtual_foreign_key_queries(
                qualified_object_name,
                ARRAY[aux_array[1]]::TEXT[],
                aux_array[2],
                ARRAY[aux_array[3]]::TEXT[],
                foreign_key->>'name',
                update_action,
                delete_action
              );


              queries := queries || format('ALTER TABLE %1$s DROP CONSTRAINT %2$I;', qualified_object_name, foreign_key->>'name');
              queries := queries || format('ALTER TABLE %1$s ADD CONSTRAINT %2$I %3$s;', qualified_object_name, foreign_key->>'name', foreign_key->>'definition');
            END IF;
          END LOOP;


        END LOOP;

        RETURN;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.get_create_virtual_foreign_key_queries(
        IN p_referencing_table TEXT,
        IN p_referencing_columns TEXT[],
        IN p_referenced_table TEXT,
        IN p_referenced_columns TEXT[],
        IN p_template_fk_name TEXT DEFAULT NULL,
        IN p_update_condition "char" DEFAULT NULL,
        IN p_delete_condition "char" DEFAULT NULL
      )
      RETURNS TEXT[] AS $BODY$
      DECLARE
        aux_array TEXT[];
        queries TEXT[];
        referencing_schema TEXT;
      BEGIN
        -- RAISE NOTICE 'sharding.get_create_virtual_foreign_key_queries(''%'', ''%'', ''%'', ''%'', ''%'', ''%'', ''%'')',
        --   p_referencing_table,
        --   p_referencing_columns,
        --   p_referenced_table,
        --   p_referenced_columns,
        --   p_template_fk_name,
        --   p_update_condition,
        --   p_delete_condition;

        IF p_template_fk_name IS NULL THEN
          p_template_fk_name := format('%1$s_%2$s_%3$s',
            regexp_replace(array_to_string(p_referencing_columns, '_'), '(.).*?(_|$)', '\1\2', 'g'),
            regexp_replace(p_referenced_table, '^(?:.*?\.)?(.*?)$', '\1'),
            regexp_replace(array_to_string(p_referenced_columns, '_'), '(.).*?(_|$)', '\1\2', 'g')
          );
        END IF;

        referencing_schema := regexp_replace(p_referencing_table, '^(?:(.+?)\.)?.*?$', '\1');

        aux_array := ARRAY[
          array_to_string(p_referencing_columns, ', '),                                                                                                    -- 1
          p_referenced_table,                                                                                                                              -- 2
          array_to_string(p_referenced_columns, ', '),                                                                                                     -- 3
          '{' || array_to_string(p_referencing_columns, ', ') || '}',                                                                                      -- 4
          p_referenced_table,                                                                                                                              -- 5
          '{' || array_to_string(p_referenced_columns, ', ') || '}',                                                                                       -- 6
          p_referencing_table,                                                                                                                             -- 7
          regexp_replace(p_referencing_table, '^(?:.*?\.)?(.*?)$', '\1'),                                                                                  -- 8
          p_template_fk_name,                                                                                                                              -- 9
          array_to_string((SELECT array_agg('NEW.' || f || ' IS NOT NULL') FROM unnest(p_referencing_columns) as f), ' AND '),                             -- 10
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(p_referenced_columns) as f), ' OR '),             -- 11
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(p_referencing_columns) as f), ' OR '),            -- 12
          array_to_string((SELECT array_agg('OLD.' || f || ' IS NOT NULL') FROM unnest(p_referenced_columns) as f), ' AND '),                              -- 13
          substring(format('trg_v%2$sfkr_au_c_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),     -- 14
          substring(format('trg_v%2$sfkr_au_sn_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 15
          substring(format('trg_v%2$sfkr_au_sd_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 16
          substring(format('trg_v%2$sfkr_bu_r_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),     -- 17
          substring(format('trg_v%2$sfkr_au_na_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 18
          substring(format('trg_v%2$sfkr_ad_c_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),     -- 19
          substring(format('trg_v%2$sfkr_ad_sn_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 20
          substring(format('trg_v%2$sfkr_ad_sd_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 21
          substring(format('trg_v%2$sfkr_ad_r_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),     -- 22
          substring(format('trg_v%2$sfkr_ad_na_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),    -- 23
          substring(format('trg_v%2$sfk_bi_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),        -- 24
          substring(format('trg_v%2$sfk_bu_%1$s', p_template_fk_name, CASE WHEN referencing_schema = 'public' THEN 'p' ELSE '' END) FROM 1 FOR 63),        -- 25
          CASE WHEN referencing_schema = 'public' THEN 'trf_virtual_public_fk_before_insert_or_update' ELSE 'trf_virtual_fk_before_insert_or_update' END,  -- 26
          CASE WHEN referencing_schema = 'public' THEN p_referencing_table ELSE regexp_replace(p_referencing_table, '^(?:.*?\.)?(.*?)$', '\1') END         -- 27
        ];

        -- Create before insert trigger
        queries := queries || format($$
          CREATE TRIGGER %24$I
            BEFORE INSERT ON %7$s
            FOR EACH ROW
              WHEN (%10$s)
            EXECUTE PROCEDURE sharding.%26$s('%4$s', '{%2$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before update trigger
        queries := queries || format($$
          CREATE TRIGGER %25$I
            BEFORE UPDATE OF %1$s ON %7$s
            FOR EACH ROW
              WHEN ((%12$s) AND %10$s)
            EXECUTE PROCEDURE sharding.%26$s('%4$s', '{%2$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before delete or update triggers on referenced table
        -- Check the ON UPDATE clause of the foreign key
        CASE p_update_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after update trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %14$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_cascade('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'n' THEN -- SET NULL
            -- Create the after update trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %15$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_null('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after update trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %16$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_default('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'r' THEN -- RESTRICT
            -- Create the before update trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %17$I
                 BEFORE UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          ELSE -- If NULL, default to NO ACTION
            -- Create the after update trigger on the referenced table to prevent changing the key fields is they're being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT CONSTRAINT sharding.wrap_with_duplicate_check(triggers that are deferred
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE CONSTRAINT TRIGGER %18$I
                 AFTER UPDATE OF %3$s ON %2$s
                 DEFERRABLE INITIALLY DEFERRED
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));
        END CASE;

        -- Check the ON DELETE clause of the foreign key
        CASE p_delete_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after update trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %19$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_cascade('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'n' THEN -- SET NULL
            -- Create the after delete trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %20$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_null('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after delete trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %21$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_default('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'r' THEN -- RESTRICT
            -- Create the after delete trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %22$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          ELSE -- If NULL, default to NO ACTION
            -- Create the after delete trigger on the referenced table to prevent changing the key fields is they're being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT CONSTRAINT triggers that are deferred
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE CONSTRAINT TRIGGER %23$I
                 AFTER DELETE ON %2$s
                 DEFERRABLE INITIALLY DEFERRED
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%27$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));
        END CASE;

        RETURN queries;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.get_create_virtual_foreign_key_to_inherited_table_queries(
        IN p_referencing_table TEXT, -- p_destination_schema_name.p_template_table_name
        IN p_parent_referenced_table TEXT,
        IN p_child_referenced_table TEXT,
        IN p_referencing_columns TEXT[], -- aux_array[1]
        IN p_referenced_columns TEXT[], -- aux_array[3]
        IN p_template_fk_name TEXT DEFAULT NULL, -- catalog_info.conname
        IN p_update_condition "char" DEFAULT NULL,
        IN p_delete_condition "char" DEFAULT NULL
      )
      RETURNS TEXT[] AS $BODY$
      DECLARE
        aux_array TEXT[];
        queries TEXT[];
      BEGIN
        -- RAISE NOTICE 'sharding.get_create_virtual_foreign_key_to_inherited_table_queries(''%'', ''%'', ''%'', ''%'', ''%'', ''%'', ''%'', ''%'');',
        --   p_referencing_table,
        --   p_parent_referenced_table,
        --   p_child_referenced_table,
        --   p_referencing_columns,
        --   p_referenced_columns,
        --   p_template_fk_name,
        --   p_update_condition,
        --   p_delete_condition;

        IF p_template_fk_name IS NULL THEN
          p_template_fk_name := format('%1$s_%2$s_%3$s',
            regexp_replace(array_to_string(p_referencing_columns, '_'), '(.).*?(_|$)', '\1\2', 'g'),
            regexp_replace(p_referenced_table, '^(?:.*?\.)?(.*?)$', '\1'),
            regexp_replace(array_to_string(p_referenced_columns, '_'), '(.).*?(_|$)', '\1\2', 'g')
          );
        END IF;

        aux_array := ARRAY[
          array_to_string(p_referencing_columns, ', '),                                                                                           -- 1
          p_parent_referenced_table,                                                                                                              -- 2
          array_to_string(p_referenced_columns, ', '),                                                                                            -- 3
          '{' || array_to_string(p_referencing_columns, ', ') || '}',                                                                             -- 4
          p_child_referenced_table,                                                                                                               -- 5
          '{' || array_to_string(p_referenced_columns, ', ') || '}',                                                                              -- 6
          p_referencing_table,                                                                                                                    -- 7
          regexp_replace(p_referencing_table, '^(?:.*?\.)?(.*?)$', '\1'),                                                                         -- 8
          p_template_fk_name,                                                                                                                     -- 9
          array_to_string((SELECT array_agg('NEW.' || f || ' IS NOT NULL') FROM unnest(p_referencing_columns) as f), ' AND '),                    -- 10
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(p_referenced_columns) as f), ' OR '),    -- 11
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(p_referencing_columns) as f), ' OR '),   -- 12
          array_to_string((SELECT array_agg('OLD.' || f || ' IS NOT NULL') FROM unnest(p_referenced_columns) as f), ' AND '),                     -- 13
          substring(format('trg_vfkir_au_c_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                             -- 14
          substring(format('trg_vfkir_au_sn_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 15
          substring(format('trg_vfkir_au_sd_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 16
          substring(format('trg_vfkir_bu_r_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                             -- 17
          substring(format('trg_vfkir_au_na_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 18
          substring(format('trg_vfkir_ad_c_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                             -- 19
          substring(format('trg_vfkir_ad_sn_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 20
          substring(format('trg_vfkir_ad_sd_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 21
          substring(format('trg_vfkir_ad_r_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                             -- 22
          substring(format('trg_vfkir_ad_na_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 23
          substring(format('trg_vfki_bi_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                                -- 25
          substring(format('trg_vfki_bu_%1$s', p_template_fk_name) FROM 1 FOR 63)                                                                 -- 25
        ];

        -- Create before insert trigger
        queries := queries || format($$
          CREATE TRIGGER %24$I
            BEFORE INSERT ON %7$s
            FOR EACH ROW
              WHEN (%10$s)
            EXECUTE PROCEDURE sharding.trf_virtual_fk_before_insert_or_update('%4$s', '{%2$s, %5$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before update trigger
        queries := queries || format($$
          CREATE TRIGGER %25$I
            BEFORE UPDATE OF %1$s ON %7$s
            FOR EACH ROW
              WHEN ((%12$s) AND %10$s)
            EXECUTE PROCEDURE sharding.trf_virtual_fk_before_insert_or_update('%4$s', '{%2$s, %5$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before delete or update triggers on referenced table
        -- Check the ON UPDATE clause of the foreign key
        CASE p_update_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after update trigger on the parent referenced table to cascade the update to the referencing fields
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %14$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_cascade('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the after update trigger on the child referenced table to cascade the update to the referencing fields
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %14$I
                 AFTER UPDATE OF %3$s ON %5$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_cascade('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'n' THEN -- SET NULL
            -- Create the after update trigger on the parent referenced table to set the referencing fields to null
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %15$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_null('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the after update trigger on the child referenced table to set the referencing fields to null
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %15$I
                 AFTER UPDATE OF %3$s ON %5$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_null('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after update trigger on the parent referenced table to set the referencing fields to their default values
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %16$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_default('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the after update trigger on the child referenced table to set the referencing fields to their default values
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %16$I
                 AFTER UPDATE OF %3$s ON %5$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_default('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'r' THEN -- RESTRICT
            -- Create the before update trigger on the parent referenced table to prevent changing the key fields if they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %17$I
                 BEFORE UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the before update trigger on the child referenced table to prevent changing the key fields if they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %17$I
                 BEFORE UPDATE OF %3$s ON %5$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          ELSE -- If NULL, default to NO ACTION
            -- Create the after update trigger on the parent referenced table to prevent changing the key fields if they're being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT CONSTRAINT triggers that are deferred
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE CONSTRAINT TRIGGER %18$I
                 AFTER UPDATE OF %3$s ON %2$s
                 DEFERRABLE INITIALLY DEFERRED
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the after update trigger on the child referenced table to prevent changing the key fields if they're being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT CONSTRAINT triggers that are deferred
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE CONSTRAINT TRIGGER %18$I
                 AFTER UPDATE OF %3$s ON %5$s
                 DEFERRABLE INITIALLY DEFERRED
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));
        END CASE;

        -- Check the ON DELETE clause of the foreign key
        CASE p_delete_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after delete trigger on the referenced table to delete the rows referencing the deleted row
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %19$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_cascade('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the after delete trigger on the referenced table to delete the rows referencing the deleted row
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %19$I
                 AFTER DELETE ON %5$s
                 FOR EACH ROW
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_cascade('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'n' THEN -- SET NULL
            -- Create the after delete trigger on the parent referenced table set the referencing fields to null
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %20$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_null('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the after delete trigger on the child referenced table set the referencing fields to null
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %20$I
                 AFTER DELETE ON %5$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_null('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after delete trigger on the parent referenced table to set the referencing fields to their default values
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %21$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_default('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the after delete trigger on the child referenced table to set the referencing fields to their default values
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %21$I
                 AFTER DELETE ON %5$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_default('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          WHEN 'r' THEN -- RESTRICT
            -- Create the after delete trigger on the parent referenced table to prevent deleting the rows if it's being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %22$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the after delete trigger on the child referenced table to prevent deleting the rows if it's being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %22$I
                 AFTER DELETE ON %5$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

          ELSE -- If NULL, default to NO ACTION
            -- Create the after delete trigger on the parent referenced table to prevent deleting the rows if it's being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT CONSTRAINT triggers that are deferred
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE CONSTRAINT TRIGGER %23$I
                 AFTER DELETE ON %2$s
                 DEFERRABLE INITIALLY DEFERRED
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

            -- Create the after delete trigger on the child referenced table to prevent deleting the rows if it's being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT CONSTRAINT triggers that are deferred
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE CONSTRAINT TRIGGER %23$I
                 AFTER DELETE ON %5$s
                 DEFERRABLE INITIALLY DEFERRED
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%8$s', '%6$s');
              $$,
              VARIADIC aux_array
            ));

        END CASE;

        RETURN queries;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.get_create_virtual_polymorphic_foreign_key_queries(
        IN p_referencing_table TEXT,
        IN p_referenced_table TEXT,
        IN p_column_mappings JSONB, -- { "referencing_col_a": "referenced_col_a", "referencing_col_b": "referenced_col_b", "referencing_col_c": null }
        IN p_template_fk_name TEXT DEFAULT NULL,
        IN p_update_condition "char" DEFAULT NULL, -- DEFAULTS TO NO ACTION
        IN p_delete_condition "char" DEFAULT NULL, -- DEFAULTS TO NO ACTION
        IN p_trigger_conditions JSONB DEFAULT NULL -- { "local_col_c": [ "value_a", "value_b" ] }
      )
      RETURNS TEXT[] AS $BODY$
      DECLARE
        aux_array TEXT[];
        queries TEXT[];

        all_local_columns TEXT[];
        referencing_columns TEXT[];
        referenced_columns TEXT[];
        trigger_condition_clause TEXT;
      BEGIN
        -- RAISE NOTICE 'sharding.get_create_virtual_polymorphic_foreign_key_queries(''%'', ''%'', ''%'', ''%'', ''%'', ''%'', ''%'')',
        -- p_referencing_table,
        -- p_referenced_table,
        -- p_column_mappings,
        -- p_template_fk_name,
        -- p_update_condition,
        -- p_delete_condition,
        -- p_trigger_conditions;

        -- Load the referencing columns from the JSON column mappings
        all_local_columns := (SELECT array_agg(k) FROM jsonb_object_keys(p_column_mappings) k);
        referencing_columns := (SELECT array_agg("key") FROM jsonb_each_text(p_column_mappings) WHERE "value" IS NOT NULL);
        referenced_columns := (SELECT array_agg("value") FROM jsonb_each_text(p_column_mappings) WHERE "value" IS NOT NULL);

        IF p_template_fk_name IS NULL THEN
          p_template_fk_name := format('%1$s_%2$s_%3$s_%4$s',
            regexp_replace(regexp_replace(p_referencing_table, '^(?:.*?\.)?(.*?)$', '\1'),'(.).*?(_|$)', '\1\2', 'g'),
            regexp_replace(array_to_string(referencing_columns, '_'), '(.).*?(_|$)', '\1\2', 'g'),
            regexp_replace(p_referenced_table, '^(?:.*?\.)?(.*?)$', '\1'),
            regexp_replace(array_to_string(referenced_columns, '_'), '(.).*?(_|$)', '\1\2', 'g')
          );
        END IF;

        trigger_condition_clause := array_to_string((
          SELECT array_agg('NEW.' || col_name || ' IN (''' || array_to_string(col_values, ''', ''') || ''')')
          FROM (
            SELECT col_name, array_agg(col_values) AS col_values
            FROM (SELECT "key" AS col_name, jsonb_array_elements_text("value"::JSONB) AS "col_values" FROM (SELECT * FROM jsonb_each_text(p_trigger_conditions)) x) y
            GROUP BY col_name
          ) z),
          ' AND '
        );

        aux_array := ARRAY[
          array_to_string(referencing_columns, ', '),                                                                                         -- 1
          p_referenced_table,                                                                                                                 -- 2
          array_to_string(referenced_columns, ', '),                                                                                          -- 3
          '{' || array_to_string(referencing_columns, ', ') || '}',                                                                           -- 4
          array_to_string(all_local_columns, ', '),                                                                                           -- 5
          '{' || array_to_string(referenced_columns, ', ') || '}',                                                                            -- 6
          p_referencing_table,                                                                                                                -- 7
          regexp_replace(p_referencing_table, '^(?:.*?\.)?(.*?)$', '\1'),                                                                     -- 8
          p_template_fk_name,                                                                                                                 -- 9
          array_to_string((SELECT array_agg('NEW.' || f || ' IS NOT NULL') FROM unnest(all_local_columns) as f), ' AND '),                    -- 10
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(referenced_columns) as f), ' OR '),  -- 11
          array_to_string((SELECT array_agg(format('NEW.%1$s IS DISTINCT FROM OLD.%1$s', f)) FROM unnest(all_local_columns) as f), ' OR '),   -- 12
          array_to_string((SELECT array_agg('OLD.' || f || ' IS NOT NULL') FROM unnest(referenced_columns) as f), ' AND '),                   -- 13
          trigger_condition_clause,                                                                                                           -- 14
          regexp_replace(trigger_condition_clause, 'NEW\.', '', 'g'),                                                                         -- 15
          p_trigger_conditions::TEXT,                                                                                                         -- 16
          substring(format('trg_vfkpr_au_c_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                         -- 17
          substring(format('trg_vfkpr_au_sn_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 18
          substring(format('trg_vfkpr_au_sd_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 19
          substring(format('trg_vfkpr_bu_r_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                         -- 20
          substring(format('trg_vfkpr_au_na_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 21
          substring(format('trg_vfkpr_ad_c_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                         -- 22
          substring(format('trg_vfkpr_ad_sn_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 23
          substring(format('trg_vfkpr_ad_sd_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 24
          substring(format('trg_vfkpr_ad_r_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                         -- 25
          substring(format('trg_vfkpr_ad_na_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                        -- 26
          substring(format('trg_vfkp_bi_%1$s', p_template_fk_name) FROM 1 FOR 63),                                                            -- 27
          substring(format('trg_vfkp_bu_%1$s', p_template_fk_name) FROM 1 FOR 63)                                                             -- 28
        ];

        -- Create before insert trigger
        queries := queries || format($$
          CREATE TRIGGER %27$I
            BEFORE INSERT ON %7$s
            FOR EACH ROW
              WHEN (%14$s AND %10$s)
            EXECUTE PROCEDURE sharding.trf_virtual_fk_before_insert_or_update('%4$s', '{%2$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before update trigger
        queries := queries || format($$
          CREATE TRIGGER %28$I
            BEFORE UPDATE OF %5$s ON %7$s
            FOR EACH ROW
              WHEN ((%12$s) AND %14$s AND %10$s)
            EXECUTE PROCEDURE sharding.trf_virtual_fk_before_insert_or_update('%4$s', '{%2$s}', '%6$s');
          $$,
          VARIADIC aux_array
        );

        -- Create before delete or update triggers on referenced table
        -- Check the ON UPDATE clause of the foreign key
        CASE p_update_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after update trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %17$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_cascade('%4$s', '%8$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ));

          WHEN 'n' THEN -- SET NULL
            -- Create the after update trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %18$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_null('%4$s', '%8$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ));

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after update trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %19$I
                 AFTER UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_set_default('%4$s', '%8$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ));

          WHEN 'r' THEN -- RESTRICT
            -- Create the before update trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %20$I
                 BEFORE UPDATE OF %3$s ON %2$s
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%8$s', '%6$s', %16$L);
              $$,
              VARIADIC aux_array
            ));

          ELSE -- If NULL, default to NO ACTION
            -- Create the after update trigger on the referenced table to prevent changing the key fields is they're being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT CONSTRAINT triggers that are deferred
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE CONSTRAINT TRIGGER %21$I
                 AFTER UPDATE OF %3$s ON %2$s
                 DEFERRABLE INITIALLY DEFERRED
                 FOR EACH ROW
                    WHEN (%11$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_update_restrict('%4$s', '%8$s', '%6$s', %16$L);
              $$,
              VARIADIC aux_array
            ));
        END CASE;

        -- Check the ON DELETE clause of the foreign key
        CASE p_delete_condition
          WHEN 'c' THEN -- CASCADE
            -- Create the after update trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %22$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_cascade('%4$s', '%8$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ));

          WHEN 'n' THEN -- SET NULL
            -- Create the after delete trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %23$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_null('%4$s', '%8$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ));

          WHEN 'd' THEN -- SET DEFAULT
            -- Create the after delete trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %24$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_set_default('%4$s', '%8$s', '%6$s', %15$L);
              $$,
              VARIADIC aux_array
            ));

          WHEN 'r' THEN -- RESTRICT
            -- Create the after delete trigger on the referenced table to prevent changing the key fields is they're being referenced
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE TRIGGER %25$I
                 AFTER DELETE ON %2$s
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%8$s', '%6$s', %16$L);
              $$,
              VARIADIC aux_array
            ));

          ELSE -- If NULL, default to NO ACTION
            -- Create the after delete trigger on the referenced table to prevent changing the key fields is they're being referenced
            -- NO ACTION foreign keys are implemented as RESTRICT CONSTRAINT triggers that are deferred
            queries := queries || sharding.wrap_with_duplicate_check(format(
              $$CREATE CONSTRAINT TRIGGER %26$I
                 AFTER DELETE ON %2$s
                 DEFERRABLE INITIALLY DEFERRED
                 FOR EACH ROW
                    WHEN (%13$s)
                 EXECUTE PROCEDURE sharding.trf_virtual_fk_reference_delete_restrict('%4$s', '%8$s', '%6$s', %16$L);
              $$,
              VARIADIC aux_array
            ));
        END CASE;

        RETURN queries;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.merge_jsonb_with_arrays_of_keys_and_values(
        IN p_jsonb JSONB,
        IN p_keys TEXT[],
        IN p_values TEXT[]
      )
      RETURNS JSONB AS $BODY$
      DECLARE
        query TEXT;
        result JSONB;
      BEGIN
        -- RAISE NOTICE 'sharding.merge_jsonb_with_arrays_of_keys_and_values(%, %, %)', p_jsonb, p_keys, p_values;

        query := $$SELECT format('{ %1$s }', array_to_string(part, ', '))
          FROM (
            SELECT array_agg(format('"%1$s": %2$s', "key", "value")) AS part
            FROM (
              SELECT * FROM jsonb_each_text('$$ || p_jsonb::TEXT || $$')
              UNION SELECT * FROM jsonb_each_text('$$
              || (
                SELECT format('{ %s }', array_to_string((SELECT array_agg(format('"%1$s": ["%2$s"]', field, val)) FROM (
                  SELECT * FROM unnest(p_keys, p_values)
                ) AS data(field, val)), ', ')))
              || $$')
            ) data
          ) x
        $$;

        EXECUTE query INTO result;

        RETURN result;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.shard_table_data(
        IN OUT insert_queries TEXT[],
        IN OUT delete_queries TEXT[],
        IN p_company_id INTEGER,
        IN p_table TEXT,
        IN p_schema_name TEXT,
        IN p_where_clause TEXT DEFAULT NULL
      )
      RETURNS record AS $BODY$
      DECLARE
        p_insert_queries ALIAS FOR insert_queries;
        p_delete_queries ALIAS FOR delete_queries;
        query TEXT;
      BEGIN
        -- RAISE DEBUG 'sharding.shard_table_data(''%'', ''%'', %, ''%'', ''%'', ''%'');', cardinality(p_insert_queries), cardinality(p_delete_queries), p_company_id, p_table, p_schema_name, p_where_clause;
        IF p_where_clause IS NULL THEN
          p_where_clause := 'company_id = %3$L';
        END IF;

        p_insert_queries := p_insert_queries || regexp_replace(format('INSERT INTO %1$I.%2$I (SELECT * FROM public.%2$I WHERE ' || p_where_clause || ')', p_schema_name, p_table, p_company_id), '\s+', ' ', 'gn');

        -- Store the sharded records into a separate table
        IF sharding.table_exists(format('sharded.%1$I', p_table)) THEN
          query := regexp_replace(format('INSERT INTO sharded.%2$I (SELECT * FROM public.%2$I WHERE ' || p_where_clause || ')', p_schema_name, p_table, p_company_id), '\s+', ' ', 'gn');
        ELSE
          query := regexp_replace(format('CREATE TABLE sharded.%2$I AS SELECT * FROM public.%2$I WHERE ' || p_where_clause, p_schema_name, p_table, p_company_id), '\s+', ' ', 'gn');
        END IF;

        p_insert_queries := p_insert_queries || query;

        -- And build the delete sharded records from the original table query (only, not from new inherited), to return from the function
        p_delete_queries := array_prepend(regexp_replace(format('DELETE FROM ONLY public.%2$I WHERE ' || p_where_clause, p_schema_name, p_table, p_company_id), '\s+', ' ', 'gn'),p_delete_queries);

        RETURN;
      -- EXCEPTION
      --   WHEN OTHERS THEN
      --     RAISE WARNING '%', SQLERRM;
      --     RETURN NULL;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.table_exists (
        IN p_relation_name TEXT
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        query TEXT;
        result BOOLEAN;
        schema_name TEXT;
        table_name TEXT;
      BEGIN
        -- RAISE NOTICE 'SELECT sharding.table_exists(''%'', ''%'');', p_relation_name, p_trigger_name;

        IF (SELECT EXISTS (SELECT 1 FROM regexp_matches(p_relation_name, '^.+\..+$'))) THEN
          SELECT (regexp_matches(p_relation_name, '^(.+?)\..+?'))[1] INTO schema_name;
          SELECT regexp_replace(p_relation_name, schema_name || '.', '') INTO table_name;
        ELSE
          schema_name := NULL;
          table_name := p_relation_name;
        END IF;

        query := format(
          $$
            SELECT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class c
              %1$s

            WHERE (c.relname %2$s) = (%4$L %3$s)
            );
          $$,
          CASE WHEN schema_name IS NOT NULL THEN 'LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace' END,
          CASE WHEN schema_name IS NOT NULL THEN ', n.nspname' END,
          CASE WHEN schema_name IS NOT NULL THEN format(', %1$L', schema_name) END,
          table_name
        );

        EXECUTE query INTO result;

        RETURN result;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_create_company_shard()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        old_search_path text;
      BEGIN
        RAISE NOTICE 'Sharding company [%] % - % - %', NEW.id, NEW.tax_registration_number, COALESCE(NEW.business_name, NEW.company_name, '<unnamed>'), NEW.use_sharded_company;

        IF NEW.use_sharded_company THEN

          IF NULLIF(NEW.schema_name,'') IS NULL THEN
            NEW.schema_name := format('pt%1$s_c%2$s', NEW.tax_registration_number, NEW.id);
          END IF;

          -- Create company schema if necessary
          RAISE DEBUG 'Creating new schema "%"', NEW.schema_name;
          EXECUTE ('CREATE SCHEMA IF NOT EXISTS "' || NEW.schema_name || '";');
          PERFORM common.create_table_schema_migrations(NEW.schema_name);

          -- Shard company
          PERFORM sharding.create_company_shard(NEW.id, NEW.schema_name);

          SHOW search_path INTO old_search_path;
          EXECUTE 'SET search_path to '||NEW.schema_name||', '||old_search_path||'';

          RAISE DEBUG 'Creating new schema "%" ... DONE!', NEW.schema_name;
        END IF;

        RETURN NEW;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_prevent_changes_on_sharded_tables_for_sharded_companies()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        _stack         text;
      BEGIN

        GET DIAGNOSTICS _stack = PG_CONTEXT;
        IF _stack ~ 'sharding\.trf_shard_existing_data()' THEN
          RETURN NEW;
        END IF;

        IF (SELECT use_sharded_company FROM public.companies WHERE id = NEW.company_id) THEN
          RAISE restrict_violation
            USING MESSAGE = format('Company %1$L has already been sharded, can''t INSERT or UPDATE records on unsharded tables' , NEW.company_id),
                  TABLE = TG_TABLE_NAME;
        END IF;

        RETURN NEW;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_prevent_unshard_of_sharded_companies()
      RETURNS TRIGGER AS $BODY$
      DECLARE
      BEGIN

        RAISE restrict_violation
          USING MESSAGE = format('Company %1$L has already been sharded, can''t be unsharded' , NEW.id),
                TABLE = TG_TABLE_NAME;

        RETURN OLD; -- not returning NEW
      END;
      $BODY$ LANGUAGE 'plpgsql';

      DROP TRIGGER IF EXISTS trg_prevent_unshard_of_sharded_companies ON public.companies;
      CREATE TRIGGER trg_prevent_unshard_of_sharded_companies
        AFTER UPDATE OF use_sharded_company ON public.companies
        FOR EACH ROW
          WHEN (OLD.use_sharded_company = TRUE AND OLD.use_sharded_company IS DISTINCT FROM NEW.use_sharded_company)
        EXECUTE PROCEDURE sharding.trf_prevent_unshard_of_sharded_companies();
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_before_insert_or_update()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        referencing_columns TEXT[];
        referencing_values TEXT[];
        referenced_tables TEXT[];
        referenced_table TEXT;
        referenced_columns TEXT[];
        record_existence_check_data JSONB;
      BEGIN
        referencing_columns := TG_ARGV[0];
        referenced_tables := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];
        -- RAISE DEBUG 'sharding.trf_virtual_fk_before_insert_or_update() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_before_insert_or_update() -        NEW: %', NEW;

        -- Extract the values from the NEW record into the referencing_values variable
        EXECUTE format(
          format('SELECT ARRAY[%s]::TEXT[]',
            (SELECT array_to_string((SELECT array_agg('($1).%I'::text) FROM (SELECT generate_series(1::integer, array_length(referencing_columns, 1)::integer)) bogus), ', '))
          ),
          VARIADIC referencing_columns
        ) USING NEW INTO referencing_values;

        FOR referenced_table IN SELECT * FROM unnest(referenced_tables) LOOP
          record_existence_check_data := (
            SELECT format('{ %s }',
              array_to_string((
                SELECT array_agg(format('"%1$s": ["%2$s"]', field, val))
                FROM (
                  SELECT * FROM unnest(referenced_columns, referencing_values)
                ) AS data(field, val)
              ), ', '))
          );

          -- Check for the existence of a record on the referenced_table with the referencing_values in the referenced_columns
          IF sharding.check_record_existence(referenced_table, record_existence_check_data) THEN
            -- If supplying more than one referenced table, the first one where the values are found validates the 'foreign key'
            -- RAISE NOTICE 'Tuple (%) exists on table %(%)', array_to_string(referencing_values, ', '), referenced_table, array_to_string(referenced_columns, ', ');
            -- RAISE DEBUG 'sharding.trf_virtual_fk_before_insert_or_update() - RETURN NEW: %', NEW;
            RETURN NEW;
          ELSE
          END IF;
        END LOOP;

        -- If we reach this point, the value was not found on any referenced table
        RAISE foreign_key_violation
          USING MESSAGE = format('Tuple (%1$s) was not found on %2$s(%3$s)', array_to_string(referencing_values, ', '), referenced_table, array_to_string(referenced_columns, ', ')),
                TABLE = referenced_table,
                COLUMN = referenced_columns
        ;

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_cascade()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        company_schema_name TEXT;
        referencing_columns TEXT[];
        referencing_table TEXT;
        referenced_columns TEXT[];
        referenced_values TEXT[];
        trigger_condition_clause TEXT;
        query TEXT;
      BEGIN
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_cascade() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_cascade() -        OLD: %', OLD;

        referencing_columns := TG_ARGV[0];
        referencing_table := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];
        trigger_condition_clause := TG_ARGV[3];

        -- Extract the values from the NEW record into the referenced_values variable
        EXECUTE format(
          format('SELECT ARRAY[%s]::TEXT[]',
            (SELECT array_to_string((SELECT array_agg('($1).%I'::TEXT) FROM (SELECT generate_series(1::integer, array_length(referenced_columns, 1)::integer)) bogus), ', '))
          ),
          VARIADIC referenced_columns
        ) USING OLD INTO referenced_values;

        -- Try to get the company schema from th referencing table (in case it's supplied as <schema>.<table>)
        IF (SELECT EXISTS (SELECT 1 FROM regexp_matches(referencing_table, '^.+\..+$'))) THEN
          SELECT (regexp_matches(referencing_table, '^(.+?)\..+?'))[1] INTO company_schema_name;
          SELECT regexp_replace(referencing_table, company_schema_name || '.', '') INTO referencing_table;
        ELSE
          BEGIN
            company_schema_name := sharding.get_schema_name_for_table(OLD.company_id, referencing_table);
          EXCEPTION
            WHEN OTHERS THEN
              company_schema_name := NULL;
          END;
        END IF;

        IF company_schema_name IS NOT NULL THEN
          -- This table has a company_id column, delete just from the associated schema
          query := format('DELETE FROM %1$I.%2$I WHERE %3$s',
            company_schema_name,
            referencing_table,
            array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          -- RAISE DEBUG 'query: %', query;
          EXECUTE query;
        ELSE
          -- The table doesn't have a company_id column, update all schemas
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company
          LOOP
            query := format('DELETE FROM %1$I.%2$I WHERE %3$s',
              company_schema_name,
              referencing_table,
              array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
            );

            IF trigger_condition_clause IS NOT NULL THEN
              query := query || ' AND ' || trigger_condition_clause;
            END IF;

            -- RAISE DEBUG 'query: %', query;
            EXECUTE query;
          END LOOP;
        END IF;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_cascade() - RETURN OLD: %', OLD;
        RETURN OLD;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_restrict()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        company_schema_name TEXT;
        culprit_schemas TEXT[];
        referencing_columns TEXT[];
        referencing_table TEXT;
        referenced_columns TEXT[];
        referenced_values TEXT[];
        trigger_condition JSONB;
      BEGIN
        -- RAISE NOTICE 'sharding.trf_virtual_fk_reference_delete_restrict() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE NOTICE 'sharding.trf_virtual_fk_reference_delete_restrict() -        OLD: %', OLD;

        referencing_columns := TG_ARGV[0];
        referencing_table := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];
        trigger_condition := TG_ARGV[3];

        IF trigger_condition IS NOT NULL THEN
          trigger_condition := sharding.merge_jsonb_with_arrays_of_keys_and_values(trigger_condition, referencing_columns, referenced_values);
        END IF;

        -- Try to get the company schema from th referencing table (in case it's supplied as <schema>.<table>)
        IF (SELECT EXISTS (SELECT 1 FROM regexp_matches(referencing_table, '^.+\..+$'))) THEN
          SELECT (regexp_matches(referencing_table, '^(.+?)\..+?'))[1] INTO company_schema_name;
          SELECT regexp_replace(referencing_table, company_schema_name || '.', '') INTO referencing_table;
        ELSE
          BEGIN
            company_schema_name := sharding.get_schema_name_for_table(OLD.company_id, referencing_table);
          EXCEPTION
            WHEN OTHERS THEN
              company_schema_name := NULL;
          END;
        END IF;

        IF company_schema_name IS NOT NULL THEN
          -- This table has a company_id column, check just the associated schema
            IF sharding.check_record_existence(format('%1$I.%2$I', company_schema_name, referencing_table), trigger_condition) THEN
              culprit_schemas := culprit_schemas || company_schema_name;
            END IF;
        ELSE
          -- The table doesn't have a company_id column, check all company schemas
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company
          LOOP
            IF sharding.check_record_existence(format('%1$I.%2$I', company_schema_name, referencing_table), trigger_condition) THEN
              culprit_schemas := culprit_schemas || company_schema_name;
            END IF;
          END LOOP;
        END IF;

        IF array_length(culprit_schemas, 1) > 0 THEN
          RAISE foreign_key_violation
            USING MESSAGE = format('Can''t delete record. Tuple (%1$s) exists in %2$s schema(s): %3$s', array_to_string(referenced_values, ', '), array_length(culprit_schemas, 1), array_to_string(culprit_schemas, ', ')),
                  SCHEMA = TG_TABLE_SCHEMA,
                  TABLE = TG_TABLE_NAME
          ;
        END IF;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_restrict() - RETURN OLD: %', OLD;
        RETURN OLD;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_set_default()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        company_schema_name TEXT;
        referencing_columns TEXT[];
        referencing_table TEXT;
        referenced_columns TEXT[];
        referenced_values TEXT[];
        trigger_condition_clause TEXT;
        query TEXT;
      BEGIN
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_default() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_default() -        OLD: %', OLD;

        referencing_columns := TG_ARGV[0];
        referencing_table := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];
        trigger_condition_clause := TG_ARGV[3];

        -- Extract the values from the NEW record into the referenced_values variable
        EXECUTE format(
          format('SELECT ARRAY[%s]::TEXT[]',
            (SELECT array_to_string((SELECT array_agg('($1).%I'::TEXT) FROM (SELECT generate_series(1::integer, array_length(referenced_columns, 1)::integer)) bogus), ', '))
          ),
          VARIADIC referenced_columns
        ) USING OLD INTO referenced_values;

        -- Try to get the company schema from th referencing table (in case it's supplied as <schema>.<table>)
        IF (SELECT EXISTS (SELECT 1 FROM regexp_matches(referencing_table, '^.+\..+$'))) THEN
          SELECT (regexp_matches(referencing_table, '^(.+?)\..+?'))[1] INTO company_schema_name;
          SELECT regexp_replace(referencing_table, company_schema_name || '.', '') INTO referencing_table;
        ELSE
          BEGIN
            company_schema_name := sharding.get_schema_name_for_table(OLD.company_id, referencing_table);
          EXCEPTION
            WHEN OTHERS THEN
              company_schema_name := NULL;
          END;
        END IF;

        IF company_schema_name IS NOT NULL THEN
          -- This table has a company_id column, update just the associated schema
          query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
            company_schema_name,
            referencing_table,
            array_to_string((select array_agg(format('%1$I = %2$s', columns, COALESCE(sharding.get_column_default_value(referencing_table, columns, company_schema_name), 'NULL'))) FROM unnest(referencing_columns) columns), ', '),
            array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          -- RAISE DEBUG 'query: %', query;
          EXECUTE query;
        ELSE
          -- The table doesn't have a company_id column, update all schemas
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company
          LOOP
            query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
              company_schema_name,
              referencing_table,
              array_to_string((select array_agg(format('%1$I = %2$s', columns, COALESCE(sharding.get_column_default_value(referencing_table, columns, company_schema_name), 'NULL'))) FROM unnest(referencing_columns) columns), ', '),
              array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
            );

            IF trigger_condition_clause IS NOT NULL THEN
              query := query || ' AND ' || trigger_condition_clause;
            END IF;

            -- RAISE DEBUG 'query: %', query;
            EXECUTE query;

          END LOOP;
        END IF;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_default() - RETURN OLD: %', OLD;
        RETURN OLD;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_delete_set_null()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        company_schema_name TEXT;
        referencing_columns TEXT[];
        referencing_table TEXT;
        referenced_columns TEXT[];
        referenced_values TEXT[];
        trigger_condition_clause TEXT;
        query TEXT;
      BEGIN
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_null() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_null() -        OLD: %', OLD;

        referencing_columns := TG_ARGV[0];
        referencing_table := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];
        trigger_condition_clause := TG_ARGV[3];

        -- Extract the values from the NEW record into the referenced_values variable
        EXECUTE format(
          format('SELECT ARRAY[%s]::TEXT[]',
            (SELECT array_to_string((SELECT array_agg('($1).%I'::TEXT) FROM (SELECT generate_series(1::integer, array_length(referenced_columns, 1)::integer)) bogus), ', '))
          ),
          VARIADIC referenced_columns
        ) USING OLD INTO referenced_values;

        -- Try to get the company schema from th referencing table (in case it's supplied as <schema>.<table>)
        IF (SELECT EXISTS (SELECT 1 FROM regexp_matches(referencing_table, '^.+\..+$'))) THEN
          SELECT (regexp_matches(referencing_table, '^(.+?)\..+?'))[1] INTO company_schema_name;
          SELECT regexp_replace(referencing_table, company_schema_name || '.', '') INTO referencing_table;
        ELSE
          BEGIN
            company_schema_name := sharding.get_schema_name_for_table(OLD.company_id, referencing_table);
          EXCEPTION
            WHEN OTHERS THEN
              company_schema_name := NULL;
          END;
        END IF;

        IF company_schema_name IS NOT NULL THEN
          -- This table has a company_id column, update just the associated schema
          query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
            company_schema_name,
            referencing_table,
            array_to_string((select array_agg(format('%1$I = NULL', columns)) FROM unnest(referencing_columns) columns), ', '),
            array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          -- RAISE DEBUG 'query: %', query;
          EXECUTE query;
        ELSE
          -- The table doesn't have a company_id column, update all schemas
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company
          LOOP
            query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
              company_schema_name,
              referencing_table,
              array_to_string((select array_agg(format('%1$I = NULL', columns)) FROM unnest(referencing_columns) columns), ', '),
              array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
            );

            IF trigger_condition_clause IS NOT NULL THEN
              query := query || ' AND ' || trigger_condition_clause;
            END IF;

            -- RAISE DEBUG 'query: %', query;
            EXECUTE query;
          END LOOP;
        END IF;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_delete_set_null() - RETURN OLD: %', OLD;
        RETURN OLD;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_update_cascade()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        company_schema_name TEXT;
        referencing_columns TEXT[];
        referencing_table TEXT;
        referenced_columns TEXT[];
        referenced_values TEXT[];
        new_values TEXT[];
        trigger_condition_clause TEXT;
        query TEXT;
      BEGIN
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() -        OLD: %', OLD;

        referencing_columns := TG_ARGV[0];
        referencing_table := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];
        trigger_condition_clause := TG_ARGV[3];

        -- Extract the values from the OLD record into the referenced_values variable
        EXECUTE format(
          format('SELECT ARRAY[%s]::TEXT[]',
            (SELECT array_to_string((SELECT array_agg('($1).%I'::TEXT) FROM (SELECT generate_series(1::integer, array_length(referenced_columns, 1)::integer)) bogus), ', '))
          ),
          VARIADIC referenced_columns
        ) USING OLD INTO referenced_values;

        -- Extract the values from the NEW record into the new_values variable
        EXECUTE format(
          format('SELECT ARRAY[%s]::TEXT[]',
            (SELECT array_to_string((SELECT array_agg('($1).%I'::TEXT) FROM (SELECT generate_series(1::integer, array_length(referenced_columns, 1)::integer)) bogus), ', '))
          ),
          VARIADIC referenced_columns
        ) USING NEW INTO new_values;

        -- Try to get the company schema from th referencing table (in case it's supplied as <schema>.<table>)
        IF (SELECT EXISTS (SELECT 1 FROM regexp_matches(referencing_table, '^.+\..+$'))) THEN
          SELECT (regexp_matches(referencing_table, '^(.+?)\..+?'))[1] INTO company_schema_name;
          SELECT regexp_replace(referencing_table, company_schema_name || '.', '') INTO referencing_table;
        ELSE
          BEGIN
            company_schema_name := sharding.get_schema_name_for_table(OLD.company_id, referencing_table);
          EXCEPTION
            WHEN OTHERS THEN
              company_schema_name := NULL;
          END;
        END IF;

        IF company_schema_name IS NOT NULL THEN
          -- This table has a company_id column, update just the associated schema
          query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
            company_schema_name,
            referencing_table,
            array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(new_values) as column_value) filters), ', '),
            array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          -- RAISE DEBUG 'query: %', query;
          EXECUTE query;
        ELSE
          -- The table doesn't have a company_id column, update all schemas
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company
          LOOP
            query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
              company_schema_name,
              referencing_table,
              array_to_string((select array_agg(format('%1$I = %2$s', columns, COALESCE(sharding.get_column_default_value(referencing_table, columns, company_schema_name), 'NULL'))) FROM unnest(referencing_columns) columns), ', '),
              array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
            );

              IF trigger_condition_clause IS NOT NULL THEN
                query := query || ' AND ' || trigger_condition_clause;
              END IF;

              -- RAISE DEBUG 'query: %', query;
              EXECUTE query;
          END LOOP;
        END IF;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() - RETURN NEW: %', NEW;
        RETURN NEW;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_update_restrict()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        company_schema_name TEXT;
        culprit_schemas TEXT[];
        referencing_columns TEXT[];
        referencing_table TEXT;
        referenced_columns TEXT[];
        referenced_values TEXT[];
        trigger_condition JSONB;
      BEGIN
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_restrict() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() -        OLD: %', OLD;

        referencing_columns := TG_ARGV[0];
        referencing_table := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];
        trigger_condition := TG_ARGV[3];

        -- Extract the values from the NEW record into the referenced_values variable
        EXECUTE format(
          format('SELECT ARRAY[%s]::TEXT[]',
            (SELECT array_to_string((SELECT array_agg('($1).%I'::TEXT) FROM (SELECT generate_series(1::integer, array_length(referenced_columns, 1)::integer)) bogus), ', '))
          ),
          VARIADIC referenced_columns
        ) USING OLD INTO referenced_values;

        trigger_condition := sharding.merge_jsonb_with_arrays_of_keys_and_values(trigger_condition, referencing_columns, referenced_values);

        -- Try to get the company schema from th referencing table (in case it's supplied as <schema>.<table>)
        IF (SELECT EXISTS (SELECT 1 FROM regexp_matches(referencing_table, '^.+\..+$'))) THEN
          SELECT (regexp_matches(referencing_table, '^(.+?)\..+?'))[1] INTO company_schema_name;
          SELECT regexp_replace(referencing_table, company_schema_name || '.', '') INTO referencing_table;
        ELSE
          BEGIN
            company_schema_name := sharding.get_schema_name_for_table(OLD.company_id, referencing_table);
          EXCEPTION
            WHEN OTHERS THEN
              company_schema_name := NULL;
          END;
        END IF;

        IF company_schema_name IS NOT NULL THEN
          -- This table has a company_id column, check just the associated schema
            IF sharding.check_record_existence(format('%1$I.%2$I', company_schema_name, referencing_table), trigger_condition) THEN
              culprit_schemas := culprit_schemas || company_schema_name;
            END IF;
        ELSE
          -- The table doesn't have a company_id column, check all company schemas
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company
          LOOP
            IF sharding.check_record_existence(format('%1$I.%2$I', company_schema_name, referencing_table), trigger_condition) THEN
              culprit_schemas := culprit_schemas || company_schema_name;
            END IF;
          END LOOP;
        END IF;

        IF array_length(culprit_schemas, 1) > 0 THEN
          RAISE foreign_key_violation
            USING MESSAGE = format('Can''t update record. Tuple (%1$s) exists in %2$s schema(s): %3$s', array_to_string(referenced_values, ', '), array_length(culprit_schemas, 1), array_to_string(culprit_schemas, ', ')),
                  SCHEMA = TG_TABLE_SCHEMA,
                  TABLE = TG_TABLE_NAME
          ;
        END IF;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() - RETURN NEW: %', NEW;
        RETURN NEW;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_update_set_default()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        company_schema_name TEXT;
        referencing_columns TEXT[];
        referencing_table TEXT;
        referenced_columns TEXT[];
        referenced_values TEXT[];
        trigger_condition_clause TEXT;
        query TEXT;
      BEGIN
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_set_default() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() -        OLD: %', OLD;

        referencing_columns := TG_ARGV[0];
        referencing_table := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];
        trigger_condition_clause := TG_ARGV[3];


        -- Extract the values from the NEW record into the referenced_values variable
        EXECUTE format(
          format('SELECT ARRAY[%s]::TEXT[]',
            (SELECT array_to_string((SELECT array_agg('($1).%I'::TEXT) FROM (SELECT generate_series(1::integer, array_length(referenced_columns, 1)::integer)) bogus), ', '))
          ),
          VARIADIC referenced_columns
        ) USING OLD INTO referenced_values;

        -- Try to get the company schema from th referencing table (in case it's supplied as <schema>.<table>)
        IF (SELECT EXISTS (SELECT 1 FROM regexp_matches(referencing_table, '^.+\..+$'))) THEN
          SELECT (regexp_matches(referencing_table, '^(.+?)\..+?'))[1] INTO company_schema_name;
          SELECT regexp_replace(referencing_table, company_schema_name || '.', '') INTO referencing_table;
        ELSE
          BEGIN
            company_schema_name := sharding.get_schema_name_for_table(OLD.company_id, referencing_table);
          EXCEPTION
            WHEN OTHERS THEN
              company_schema_name := NULL;
          END;
        END IF;

        IF company_schema_name IS NOT NULL THEN
          -- This table has a company_id column, update just the associated schema
          query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
            company_schema_name,
            referencing_table,
            array_to_string((select array_agg(format('%1$I = %2$s', columns, COALESCE(sharding.get_column_default_value(referencing_table, columns, company_schema_name), 'NULL'))) FROM unnest(referencing_columns) columns), ', '),
            array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          -- RAISE DEBUG 'query: %', query;
          EXECUTE query;
        ELSE
          -- The table doesn't have a company_id column, update all schemas
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company
          LOOP
            query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
              company_schema_name,
              referencing_table,
              array_to_string((select array_agg(format('%1$I = %2$s', columns, COALESCE(sharding.get_column_default_value(referencing_table, columns, company_schema_name), 'NULL'))) FROM unnest(referencing_columns) columns), ', '),
              array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
            );

            IF trigger_condition_clause IS NOT NULL THEN
              query := query || ' AND ' || trigger_condition_clause;
            END IF;

            -- RAISE DEBUG 'query: %', query;
            EXECUTE query;
          END LOOP;
        END IF;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() - RETURN NEW: %', NEW;
        RETURN NEW;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_fk_reference_update_set_null()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        company_schema_name TEXT;
        referencing_columns TEXT[];
        referencing_table TEXT;
        referenced_columns TEXT[];
        referenced_values TEXT[];
        trigger_condition_clause TEXT;
        query TEXT;
      BEGIN
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_set_null() TG_NAME:% TG_TABLE_SCHEMA:% TG_TABLE_NAME:% TG_NARGS:% TG_ARGV:%', TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NARGS, TG_ARGV;
        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() -        OLD: %', OLD;

        referencing_columns := TG_ARGV[0];
        referencing_table := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];
        trigger_condition_clause := TG_ARGV[3];

        -- Extract the values from the NEW record into the referenced_values variable
        EXECUTE format(
          format('SELECT ARRAY[%s]::TEXT[]',
            (SELECT array_to_string((SELECT array_agg('($1).%I'::TEXT) FROM (SELECT generate_series(1::integer, array_length(referenced_columns, 1)::integer)) bogus), ', '))
          ),
          VARIADIC referenced_columns
        ) USING OLD INTO referenced_values;

        -- Try to get the company schema from th referencing table (in case it's supplied as <schema>.<table>)
        IF (SELECT EXISTS (SELECT 1 FROM regexp_matches(referencing_table, '^.+\..+$'))) THEN
          SELECT (regexp_matches(referencing_table, '^(.+?)\..+?'))[1] INTO company_schema_name;
          SELECT regexp_replace(referencing_table, company_schema_name || '.', '') INTO referencing_table;
        ELSE
          BEGIN
            company_schema_name := sharding.get_schema_name_for_table(OLD.company_id, referencing_table);
          EXCEPTION
            WHEN OTHERS THEN
              company_schema_name := NULL;
          END;
        END IF;

        IF company_schema_name IS NOT NULL THEN
          -- This table has a company_id column, update just the associated schema
          query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
            company_schema_name,
            referencing_table,
            array_to_string((select array_agg(format('%1$I = NULL', columns)) FROM unnest(referencing_columns) columns), ', '),
            array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
          );

          IF trigger_condition_clause IS NOT NULL THEN
            query := query || ' AND ' || trigger_condition_clause;
          END IF;

          -- RAISE DEBUG 'query: %', query;
          EXECUTE query;
        ELSE
          -- The table doesn't have a company_id column, update all schemas
          FOR company_schema_name IN
            SELECT schema_name FROM public.companies WHERE use_sharded_company
          LOOP
            query := format('UPDATE %1$I.%2$I SET %3$s WHERE %4$s',
              company_schema_name,
              referencing_table,
              array_to_string((select array_agg(format('%1$I = NULL', columns)) FROM unnest(referencing_columns) columns), ', '),
              array_to_string((select array_agg(format('%1$I = %2$L', filters.column_name, filters.column_value)) from (SELECT unnest(referencing_columns) as column_name, unnest(referenced_values) as column_value) filters), ' AND ')
            );

            IF trigger_condition_clause IS NOT NULL THEN
              query := query || ' AND ' || trigger_condition_clause;
            END IF;

            -- RAISE DEBUG 'query: %', query;
            EXECUTE query;
          END LOOP;
        END IF;

        -- RAISE DEBUG 'sharding.trf_virtual_fk_reference_update_cascade() - RETURN NEW: %', NEW;
        RETURN NEW;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_virtual_public_fk_before_insert_or_update()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        referencing_columns TEXT[];
        referencing_values TEXT[];
        referenced_tables TEXT[];
        referenced_table TEXT;
        referenced_columns TEXT[];
        record_existence_check_data JSONB;
        company_schema_name TEXT;
      BEGIN
        referencing_columns := TG_ARGV[0];
        referenced_tables := TG_ARGV[1];
        referenced_columns := TG_ARGV[2];

        -- Extract the values from the NEW record into the referencing_values variable
        EXECUTE format(
          format('SELECT ARRAY[%s]::TEXT[]',
            (SELECT array_to_string((SELECT array_agg('($1).%I'::text) FROM (SELECT generate_series(1::integer, array_length(referencing_columns, 1)::integer)) bogus), ', '))
          ),
          VARIADIC referencing_columns
        ) USING NEW INTO referencing_values;

        FOR referenced_table IN SELECT * FROM unnest(referenced_tables) LOOP
          -- If we're working on the companies table, get the schema name directly
          IF TG_TABLE_NAME = 'companies' THEN
            IF NEW.use_sharded_company THEN
              company_schema_name := NEW.schema_name;
            ELSE
              company_schema_name := 'public';
            END IF;
          ELSE
            -- Otherwise get it from the company via the company_id column
            company_schema_name := sharding.get_schema_name_for_table(NEW.company_id, referencing_table);
          END IF;

          record_existence_check_data := (
            SELECT format('{ %s }',
              array_to_string((
                SELECT array_agg(format('"%1$s": ["%2$s"]', field, val))
                FROM (
                  SELECT * FROM unnest(referenced_columns, referencing_values)
                ) AS data(field, val)
              ), ', '))
          );

          -- Check for the existence of a record on the referenced_table with the referencing_values in the referenced_columns
          IF sharding.check_record_existence(format('%1$I.%2$I', company_schema_name, referenced_table), record_existence_check_data) THEN
            -- If supplying more than one referenced table, the first one where the values are found validates the 'foreign key'
            -- RAISE NOTICE 'Tuple (%) exists on table %(%)', array_to_string(referencing_values, ', '), referenced_table, array_to_string(referenced_columns, ', ');
            -- RAISE DEBUG 'sharding.trf_virtual_public_fk_before_insert_or_update() - RETURN NEW: %', NEW;
            RETURN NEW;
          ELSE
          END IF;
        END LOOP;

        -- If we reach this point, the value was not found on any referenced table
        RAISE foreign_key_violation
          USING MESSAGE = format('Tuple (%1$s) was not found on %2$s(%3$s)', array_to_string(referencing_values, ', '), referenced_table, array_to_string(referenced_columns, ', ')),
                TABLE = referenced_table,
                COLUMN = referenced_columns;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trigger_exists (
        IN p_relation_name TEXT,
        IN p_trigger_name TEXT
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        query TEXT;
        result BOOLEAN;
        schema_name TEXT;
        table_name TEXT;
      BEGIN
        -- RAISE NOTICE 'SELECT sharding.trigger_exists(''%'', ''%'');', p_relation_name, p_trigger_name;

        IF (SELECT EXISTS (SELECT 1 FROM regexp_matches(p_relation_name, '^.+\..+$'))) THEN
          SELECT (regexp_matches(p_relation_name, '^(.+?)\..+?'))[1] INTO schema_name;
          SELECT regexp_replace(p_relation_name, schema_name || '.', '') INTO table_name;
        ELSE
          schema_name := NULL;
          table_name := p_relation_name;
        END IF;

        query := format(
          $$
            SELECT EXISTS (
            SELECT 1
            FROM pg_trigger t
              JOIN pg_class c ON t.tgrelid = c.oid
              %1$s
            WHERE NOT t.tgisinternal
              AND c.relname = %3$L
              AND t.tgname = %4$L
              %2$s
            );
          $$,
          CASE WHEN schema_name IS NOT NULL THEN 'JOIN pg_namespace n ON c.relnamespace = n.oid' END,
          CASE WHEN schema_name IS NOT NULL THEN format('AND n.nspname = %1$L', schema_name) END,
          table_name,
          p_trigger_name
        );

        EXECUTE query INTO result;

        RETURN result;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.wrap_with_duplicate_check(
        IN p_query TEXT
      )
      RETURNS TEXT AS $BODY$
      DECLARE
      BEGIN
        RETURN format(
          $RETURN$
            DO $BLOCK$
              BEGIN
                %1$s
              EXCEPTION WHEN duplicate_object THEN
              END;
            $BLOCK$
          $RETURN$,
          p_query
        );
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL
  end

  def down
    execute %Q[DROP FUNCTION IF EXISTS sharding.wrap_with_duplicate_check(TEXT);]
    execute %Q[DROP FUNCTION IF EXISTS sharding.trigger_exists(TEXT, TEXT);]
    execute %Q[DROP FUNCTION IF EXISTS sharding.trf_virtual_public_fk_before_insert_or_update() CASCADE;]
    execute %Q[DROP FUNCTION IF EXISTS sharding.trf_virtual_fk_reference_update_set_null() CASCADE;]
    execute %Q[DROP FUNCTION IF EXISTS sharding.trf_virtual_fk_reference_update_set_default() CASCADE;]
    execute %Q[DROP FUNCTION IF EXISTS sharding.trf_virtual_fk_reference_update_restrict() CASCADE;]
    execute %Q[DROP FUNCTION IF EXISTS sharding.trf_virtual_fk_reference_update_cascade() CASCADE;]
    execute %Q[DROP FUNCTION IF EXISTS sharding.trf_virtual_fk_reference_delete_set_null() CASCADE;]
    execute %Q[DROP FUNCTION IF EXISTS sharding.trf_virtual_fk_reference_delete_set_default() CASCADE;]
    execute %Q[DROP FUNCTION IF EXISTS sharding.trf_virtual_fk_reference_delete_restrict() CASCADE;]
    execute %Q[DROP FUNCTION IF EXISTS sharding.trf_virtual_fk_reference_delete_cascade() CASCADE;]
    execute %Q[DROP FUNCTION IF EXISTS sharding.trf_virtual_fk_before_insert_or_update() CASCADE;]
    execute %Q[DROP FUNCTION IF EXISTS sharding.trf_prevent_unshard_of_sharded_companies() CASCADE;]
    execute %Q[DROP FUNCTION IF EXISTS sharding.trf_prevent_changes_on_sharded_tables_for_sharded_companies() CASCADE;]
    execute %Q[DROP FUNCTION IF EXISTS sharding.trf_create_company_shard() CASCADE;]
    execute %Q[DROP FUNCTION IF EXISTS sharding.table_exists(TEXT);]
    execute %Q[DROP FUNCTION IF EXISTS sharding.shard_table_data(TEXT[], TEXT[], INTEGER, TEXT, TEXT, TEXT);]
    execute %Q[DROP FUNCTION IF EXISTS sharding.merge_jsonb_with_arrays_of_keys_and_values(JSONB, TEXT[], TEXT[]);]
    execute %Q[DROP FUNCTION IF EXISTS sharding.get_create_virtual_polymorphic_foreign_key_queries(TEXT, TEXT, JSONB, TEXT, "char", "char", JSONB);]
    execute %Q[DROP FUNCTION IF EXISTS sharding.get_create_virtual_foreign_key_to_inherited_table_queries(TEXT, TEXT, TEXT, TEXT[], TEXT[], TEXT, "char", "char");]
    execute %Q[DROP FUNCTION IF EXISTS sharding.get_create_virtual_foreign_key_queries(TEXT, TEXT[], TEXT, TEXT[], TEXT, "char", "char");]
    execute %Q[DROP FUNCTION IF EXISTS sharding.get_convert_foreign_keys_from_public_to_sharded_tables_queries();]
    execute %Q[DROP FUNCTION IF EXISTS sharding.get_column_default_value(TEXT, TEXT, TEXT);]
    execute %Q[DROP FUNCTION IF EXISTS sharding.get_auxiliary_table_information();]
    execute %Q[DROP FUNCTION IF EXISTS sharding.generate_create_company_shard_function(BOOLEAN);]
    execute %Q[DROP FUNCTION IF EXISTS sharding.create_virtual_polymorphic_foreign_key(TEXT, TEXT, JSONB, TEXT, "char", "char", JSONB);]
    execute %Q[DROP FUNCTION IF EXISTS sharding.create_virtual_foreign_key(TEXT, TEXT[], TEXT, TEXT[], TEXT, TEXT, TEXT, BOOLEAN);]
    execute %Q[DROP FUNCTION IF EXISTS sharding.create_safety_triggers_for_sharded_companies();]
    execute %Q[DROP FUNCTION IF EXISTS sharding.check_record_existence(TEXT, JSONB);]
  end
end
