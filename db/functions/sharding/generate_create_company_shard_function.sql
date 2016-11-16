-- DROP FUNCTION IF EXISTS sharding.generate_create_company_shard_function(BOOLEAN);

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
  original_search_path TEXT;
BEGIN
  SHOW search_path INTO original_search_path;
  SET search_path TO '';

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
    -- RAISE DEBUG 'object_name: %', object_name;

    queries := queries || format('RAISE DEBUG ''-- [TABLES] TABLE: %1$I'';', object_name);

    query := format('CREATE TABLE %1$s.%2$I (', p_destination_schema_name, object_name);

    FOR json_object IN SELECT * FROM json_array_elements(object_data->'columns') LOOP
      -- RAISE DEBUG 'column: %', json_object;
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

    -- raise DEBUG 'query: %', query;
  END LOOP;

  -----------------------
  -- Build the indexes --
  -----------------------

  queries := queries || '{ -- Create indexes }'::TEXT[];

  FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP
    -- Reset variables
    aux := NULL;

    object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');
    -- RAISE DEBUG 'object_name: %', object_name;

    queries := queries || format('RAISE DEBUG ''-- [INDEXES] TABLE: %1$I'';', object_name);

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
    -- RAISE DEBUG 'object_name: %', object_name;

    queries := queries || format('RAISE DEBUG ''-- [FOREIGN KEYS] TABLE: %1$I'';', object_name);

    IF (object_data->>'foreign_keys') IS NOT NULL THEN
      RAISE DEBUG '% foreign_keys: %', object_name, object_data->'foreign_keys';

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

          -- RAISE DEBUG 'aux_array: %', aux_array;

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
    -- RAISE DEBUG 'object_name: %', object_name;

    queries := queries || format('RAISE DEBUG ''-- [TRIGGERS] TABLE: %1$I'';', object_name);

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

  SELECT json_object(array_agg(dependent_view), array_agg(depends_on))::JSONB
    INTO all_objects_data
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
    WHERE source_ns.nspname = 'public'
      AND dependent_ns.nspname = 'public'
      AND source_view.relname != dependent_view.relname
      AND source_view.relkind = 'v'
    GROUP by dependent_view.relname
  ) views_dependencies;

  FOR qualified_object_name, aux IN
    SELECT
      format('%1$I.%2$I', v.schemaname, v.viewname),
      pg_catalog.pg_get_viewdef(c.oid)
    FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_catalog.pg_views v ON c.oid = (v.schemaname || '.' || v.viewname)::regclass::oid
    WHERE n.nspname = 'public'
      AND NOT all_objects_data ? v.viewname
  LOOP
    object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');

    aux := regexp_replace(aux, 'public\.', '', 'g');

    queries := queries || format('CREATE VIEW %1$s.%2$I AS %3$s;',
      p_destination_schema_name,
      object_name,
      aux
    );
  END LOOP;

  FOR qualified_object_name, aux IN
    SELECT
      format('%1$I.%2$I', v.schemaname, v.viewname),
      pg_catalog.pg_get_viewdef(c.oid)
    FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_catalog.pg_views v ON c.oid = (v.schemaname || '.' || v.viewname)::regclass::oid
    WHERE n.nspname = 'public'
      AND all_objects_data ? v.viewname
  LOOP
    object_name := regexp_replace(qualified_object_name, '^(?:.+\.)?(.*)$', '\1');

    aux := regexp_replace(aux, 'public\.', '', 'g');

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
      previous_search_path TEXT;
      spath TEXT;
      rec RECORD;
    BEGIN
      SHOW search_path INTO previous_search_path;
      EXECUTE 'SET search_path to ' || p_company_schema_name || ', public';
      SHOW search_path INTO spath;

      %1$s

      EXECUTE 'SET search_path to ' || previous_search_path;

      RETURN TRUE;
    END;
    $FUNCTION_BODY$ LANGUAGE 'plpgsql';
  $$,
    (
      SELECT string_agg(
        CASE WHEN unnest ~* '^(?:--|RAISE|EXECUTE|SHOW)'
        THEN format(E'\n      %1$s', unnest)
        ELSE format(E'EXECUTE format(%1$L, p_company_schema_name, p_company_id);', regexp_replace(unnest, '\s+', ' ', 'g'))
        -- Switch this with the previous one for debug
        -- ELSE format(E'query := format(%1$L, p_company_schema_name, p_company_id);\n      RAISE DEBUG ''query: %%'', query;\n      EXECUTE query;', regexp_replace(unnest, '\s+', ' ', 'g'))
        END, E'\n      '
      )
      FROM unnest(queries)
    )
  );

  RAISE DEBUG 'query: %', query;

  EXECUTE query;

  EXECUTE 'SET search_path TO ''' || original_search_path || '''';

  RETURN TRUE;
-- EXCEPTION
--   WHEN OTHERS THEN
--     RETURN false;
END;
$BODY$ LANGUAGE 'plpgsql';
