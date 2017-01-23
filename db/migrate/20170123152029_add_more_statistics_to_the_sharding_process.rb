class AddMoreStatisticsToTheShardingProcess < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE FUNCTION common.array_search(needle ANYELEMENT, haystack ANYARRAY)
      RETURNS INT AS $$
          SELECT i
            FROM generate_subscripts($2, 1) AS i
           WHERE $2[i] = $1
        ORDER BY i
      $$ LANGUAGE sql STABLE;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_shard_existing_data()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        _insert_queries TEXT[][];
        _delete_queries TEXT[][];
        query TEXT[];
        _start_time TIMESTAMP WITH TIME ZONE;
        _end_time TIMESTAMP WITH TIME ZONE;
        _tables TEXT[];
        _record_count INTEGER;
        _record_counts INTEGER[];
        _duration INTERVAL;
        _durations INTERVAL[];
        _index INTEGER;
        _row record;
      BEGIN
        _insert_queries := '{}'::TEXT[][];
        _delete_queries := '{}'::TEXT[][];

        -- Add timing queries
        _insert_queries := format($${{ sharding_statistics, UPDATE sharding.sharding_statistics SET data_sharding_started_at = clock_timestamp() WHERE sharding_key = %1$s RETURNING -1; }}$$, NEW.id)::TEXT[][];
        _delete_queries := format($${{ sharding_statistics, UPDATE sharding.sharding_statistics SET data_sharding_ended_at = clock_timestamp() WHERE sharding_key = %1$s RETURNING -1; }}$$, NEW.id)::TEXT[][];

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'other_entities', NEW.schema_name::text);

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'tax_descriptors', NEW.schema_name::text);

        RAISE DEBUG 'XXX insert_queries: %', _insert_queries;

        -- We need to also copy the records that don't belong to any company
        _insert_queries := array_cat(
          _insert_queries,
          ARRAY[
            'tax_descriptors',
            regexp_replace(format('INSERT INTO %1$I.tax_descriptors (SELECT * FROM ONLY public.tax_descriptors WHERE company_id IS NULL) RETURNING -1', NEW.schema_name::text), '\s+', ' ', 'gn')
          ]
        );

        RAISE DEBUG 'XXX insert_queries: %', _insert_queries;

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'bank_accounts', NEW.schema_name::text, format($$
          company_id = %1$L
          OR (entity_type IN ('Customer') AND entity_id IN (SELECT id FROM %2$I.customers WHERE company_id = %1$L))
          OR (entity_type IN ('Supplier') AND entity_id IN (SELECT id FROM %2$I.suppliers WHERE company_id = %1$L))
          OR (entity_type IN ('User', 'Manager', 'Employee', 'Accountant') AND entity_id IN (SELECT id FROM public.users WHERE company_id = %1$L))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'document_communication_jobs', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'general_ledgers', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'document_series', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'customers', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'cash_accounts', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'receipts', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'template_options', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'messages', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'item_families', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'notifications', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'units_of_measure', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'background_jobs', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'settings', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'banks', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'items', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'archives', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'suppliers', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'sub_types', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'records', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'company_divisions', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'historical_licenses', NEW.schema_name::text);

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'addresses', NEW.schema_name::text, format($$
          (addressable_type IN ('Company') AND addressable_id = %1$L)
          OR (addressable_type IN ('Customer') AND addressable_id IN (SELECT id FROM %2$I.customers WHERE company_id = %1$L))
          OR (addressable_type IN ('Supplier') AND addressable_id IN (SELECT id FROM %2$I.suppliers WHERE company_id = %1$L))
          OR (addressable_type IN ('User', 'Manager', 'Employee', 'Accountant') AND addressable_id IN (SELECT id FROM public.users WHERE company_id = %1$L))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'documents', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_expense_categories', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payments', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_mission_maps', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_reports_infos', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_documents', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'stocks_suggestions', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'stocks_documents', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'journals', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'transactions', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'customer_general_ledger_customers', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'cost_dimensions', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'service_groups', NEW.schema_name::text);

        -- We need to also copy the records that don't belong to any company
        _insert_queries := array_cat(
          _insert_queries,
          ARRAY[
            'service_groups',
            regexp_replace(format('INSERT INTO %1$I.service_groups (SELECT * FROM ONLY public.service_groups WHERE company_id IS NULL) RETURNING -1', NEW.schema_name::text), '\s+', ' ', 'gn')
          ]
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'transactions_document_type_settings', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'cost_centers', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'non_attendances', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_deduction_entities', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_calculation_results', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_accounting_accounts', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_calculations', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_tax_reports', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_professional_categories', NEW.schema_name::text);

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'paperclip_database_storage_attachments', NEW.schema_name::text, format($$
          (attached_type = 'Company' AND attached_id = %1$L)
          OR (attached_type IN ('Manager', 'Accountant', 'User', 'Employee') AND attached_id IN (SELECT id FROM public.users WHERE company_id = %1$L))
          OR (attached_type IN ('Archive') AND attached_id IN (SELECT id FROM public.archives WHERE company_id = %1$L))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'document_communication_jobs_documents', NEW.schema_name::text, format($$
          (
            (document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L) AND document_type = 'Document')
            OR (document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L) AND document_type = 'Purchases::Document')
          )
          AND document_communication_job_id IN (SELECT id FROM %2$I.document_communication_jobs WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'customer_defaults', NEW.schema_name::text, format($$
          customer_id IN (SELECT id FROM %2$I.customers WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'support_messages', NEW.schema_name::text, format($$
          user_id IN (SELECT id FROM public.users WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'background_job_errors', NEW.schema_name::text, format($$
          background_job_id IN (SELECT id FROM %2$I.background_jobs WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'supplier_defaults', NEW.schema_name::text, format($$
          supplier_id IN (SELECT id FROM %2$I.suppliers WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'email_addresses', NEW.schema_name::text, format($$
          (email_addressable_type IN ('Supplier') AND email_addressable_id IN (SELECT id FROM %2$I.suppliers WHERE company_id = %1$L))
          OR (email_addressable_type IN ('Customer') AND email_addressable_id IN (SELECT id FROM %2$I.customers WHERE company_id = %1$L))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'contacts', NEW.schema_name::text, format($$
          (contactable_type IN ('Supplier') AND contactable_id IN (SELECT id FROM %2$I.suppliers WHERE company_id = %1$L))
          OR (contactable_type IN ('Customer') AND contactable_id IN (SELECT id FROM %2$I.customers WHERE company_id = %1$L))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT,
          NEW.id,
          'document_communication_logs',
          NEW.schema_name,
          format($$
            (
              (document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L) AND document_type = 'Document')
              OR (document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L) AND document_type = 'Purchases::Document')
            )
            AND (document_communication_job_id IS NULL AND result_code = 9999) OR document_communication_job_id IN (SELECT id FROM %2$I.document_communication_jobs WHERE company_id = %1$L)
          $$, NEW.id, NEW.schema_name)
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT,
          NEW.id,
          'public_links',
          NEW.schema_name,
          format($$
            (entity_type IN ('Document') AND entity_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L))
            OR (entity_type IN ('DueDocument') AND entity_id IN (SELECT id FROM %2$I.customers WHERE company_id = %1$L))
            OR (entity_type IN ('Receipt') AND entity_id IN (SELECT id FROM %2$I.receipts WHERE company_id = %1$L))
          $$, NEW.id, NEW.schema_name)
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT,
          NEW.id,
          'document_lines',
          NEW.schema_name,
          format($$
            document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L)
          $$, NEW.id, NEW.schema_name)
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'receipt_lines', NEW.schema_name::text, format($$
          receipt_id IN (SELECT id FROM %2$I.receipts WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT,
          NEW.id,
          'documents_receipts',
          NEW.schema_name,
          format($$
            receipt_id IN (SELECT id FROM %2$I.receipts WHERE company_id = %1$L)
            AND document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L)
          $$, NEW.id, NEW.schema_name)
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT,
          NEW.id,
          'related_documents',
          NEW.schema_name,
          format($$
            child_document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L)
            AND parent_document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L)
          $$, NEW.id, NEW.schema_name)
        );

        _insert_queries := array_cat(
          _insert_queries,
          ARRAY[
            'documents_extra_data',
            format('DELETE FROM %1$I.documents_extra_data RETURNING -1', NEW.schema_name)
          ]
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'documents_extra_data', NEW.schema_name::text, format($$
          document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_mission_map_lines', NEW.schema_name::text, format($$
          mission_map_id IN (SELECT id FROM %2$I.purchases_mission_maps WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_workflows', NEW.schema_name::text, format($$
          (entity_type = 'Purchases::MissionMap' and entity_id IN (SELECT id FROM %2$I.purchases_mission_maps WHERE company_id = %1$L))
          OR (entity_type = 'Purchases::Document' and entity_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT,
          NEW.id,
          'purchases_document_lines',
          NEW.schema_name,
          format($$
            document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L)
          $$, NEW.id, NEW.schema_name)
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payment_lines', NEW.schema_name::text, format($$
          payment_id IN (SELECT id FROM %2$I.payments WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_expenses_document_extra_data', NEW.schema_name::text, format($$
          expense_document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_related_documents', NEW.schema_name::text, format($$
          child_document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L)
          AND parent_document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchase_documents_payments', NEW.schema_name::text, format($$
          purchase_document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L)
          AND payment_id IN (SELECT id FROM %2$I.payments WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_mission_map_payments', NEW.schema_name::text, format($$
          mission_map_id IN (SELECT id FROM %2$I.purchases_mission_maps WHERE company_id = %1$L)
          AND payment_id IN (SELECT id FROM %2$I.payments WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'stocks_suggestion_lines', NEW.schema_name::text, format($$
          suggestion_id IN (SELECT id FROM %2$I.stocks_suggestions WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT,
          NEW.id,
          'stocks_document_lines',
          NEW.schema_name,
          format($$
            document_id IN (SELECT id FROM %2$I.stocks_documents WHERE company_id = %1$L)
          $$, NEW.id, NEW.schema_name)
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'stocks_stockable_data', NEW.schema_name::text, format($$
          stockable_type IN ('Item', 'Product') and stockable_id IN (SELECT id FROM %2$I.items WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT,
          NEW.id,
          'stocks_stock_movements',
          NEW.schema_name,
          format($$
            (stock_affector_type = 'Document' AND stock_affector_detail_type = 'DocumentLine' AND stock_affector_id IN (SELECT id FROM %2$I.documents) AND stock_affector_detail_id IN (SELECT id FROM %2$I.document_lines))
            OR (stock_affector_type = 'Purchases::Document' AND stock_affector_detail_type = 'Purchases::DocumentLine' AND stock_affector_id IN (SELECT id FROM %2$I.purchases_documents) AND stock_affector_detail_id IN (SELECT id FROM %2$I.purchases_document_lines))
            OR (stock_affector_type = 'Stocks::Document' AND stock_affector_detail_type = 'Stocks::DocumentLine' AND stock_affector_id IN (SELECT id FROM %2$I.stocks_documents) AND stock_affector_detail_id IN (SELECT id FROM %2$I.stocks_document_lines))
          $$, NEW.id, NEW.schema_name)
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'transaction_lines', NEW.schema_name::text, format($$
          transaction_id IN (SELECT id FROM %2$I.transactions)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'analytical_transaction_lines', NEW.schema_name::text, format($$
          transaction_line_id IN (SELECT id FROM %2$I.transaction_lines)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'cost_dimension_templates', NEW.schema_name::text, format($$
          cost_center_id IN (SELECT id FROM %2$I.cost_centers)
          OR cost_dimension_id IN (SELECT id FROM %2$I.cost_dimensions)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'non_attendance_days', NEW.schema_name::text, format($$
          non_attendance_id IN (SELECT id FROM %2$I.non_attendances)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_dismissals', NEW.schema_name::text, format($$
          user_id IN (SELECT id FROM public.users WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_calculation_result_lines', NEW.schema_name::text, format($$
          payroll_calculation_result_id IN (SELECT id FROM %2$I.payroll_calculation_results)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'address_holidays', NEW.schema_name::text, format($$
          address_id IN (SELECT id FROM %2$I.addresses)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_tax_report_workflow_histories', NEW.schema_name::text, format($$
          payroll_tax_report_id IN (SELECT id FROM %2$I.payroll_tax_reports)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_item_overrides_deduction_entities', NEW.schema_name::text, format($$
          payroll_item_override_id IN (SELECT id FROM public.payroll_item_overrides WHERE company_id = %1$L)
          AND payroll_deduction_entity_id IN (SELECT id FROM %2$I.payroll_deduction_entities)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_deduction_settings', NEW.schema_name::text, format($$
          payroll_deduction_entity_id IN (SELECT id FROM %2$I.payroll_deduction_entities)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'settlements', NEW.schema_name::text, format($$
          (associated_type = 'PaymentLine' AND associated_id IN (SELECT id FROM %2$I.payment_lines))
          OR (associated_type = 'DocumentLine' AND associated_id IN (SELECT id FROM %2$I.document_lines))
          OR (associated_type = 'Purchases::DocumentLine' AND associated_id IN (SELECT id FROM %2$I.purchases_document_lines))
          OR (associated_type = 'Document' AND associated_id IN (SELECT id FROM %2$I.documents))
          OR (associated_type = 'Purchases::Document' AND associated_id IN (SELECT id FROM %2$I.purchases_documents))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'applied_taxes', NEW.schema_name::text, format($$
          (taxable_type = 'ReceiptLine' AND taxable_id IN (SELECT id FROM %2$I.receipt_lines))
          OR (taxable_type = 'Document' AND taxable_id IN (SELECT id FROM %2$I.documents))
          OR (taxable_type = 'Receipt' AND taxable_id IN (SELECT id FROM %2$I.receipts))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'applied_vats', NEW.schema_name::text, format($$
          (reference_type = 'DocumentLine' AND reference_id IN (SELECT id FROM %2$I.document_lines))
          OR (reference_type = 'Purchases::DocumentLine' AND reference_id IN (SELECT id FROM %2$I.purchases_document_lines))
          OR (reference_type = 'Document' AND reference_id IN (SELECT id FROM %2$I.documents))
          OR (reference_type = 'Purchases::Document' AND reference_id IN (SELECT id FROM %2$I.purchases_documents))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_extended_attributes', NEW.schema_name::text, format($$
          (entity_type IN ('Company') AND entity_id = %1$L)
          OR (entity_type IN ('User', 'Manager', 'Employee', 'Accountant') AND entity_id IN (SELECT id FROM public.users WHERE company_id = %1$L))
        $$, NEW.id, NEW.schema_name));

        FOR query IN SELECT reduce_array_dimension FROM public.reduce_array_dimension(_insert_queries) LOOP
          -- raise DEBUG 'INSERT query: %', query[2];

          _start_time := clock_timestamp();
          EXECUTE query[2] INTO _record_count;
          _end_time := clock_timestamp();

          IF _record_count >= 0 THEN
            _duration := _end_time - _start_time;
            _index := common.array_search(query[1], _tables);

            IF _index IS NOT NULL THEN
              _durations[_index] := _durations[_index] + _duration;
              _record_counts[_index] := _record_counts[_index] + _record_count;
            ELSE
              _tables := _tables || query[1];
              _durations := _durations || _duration;
              _record_counts := _record_counts || _record_count;
            END IF;
          END IF;
        END LOOP;

        raise notice '% % %', _tables, _durations, _record_counts;

        FOR query IN SELECT reduce_array_dimension FROM public.reduce_array_dimension(_delete_queries) LOOP
          -- raise DEBUG 'DELETE query: %', query;
          _start_time := clock_timestamp();
          EXECUTE query[2];
          _end_time := clock_timestamp();

          IF _record_count >= 0 THEN
            _duration := _end_time - _start_time;
            _index := common.array_search(query[1], _tables);

            IF _index IS NOT NULL THEN
              _durations[_index] := _durations[_index] + _duration;
            END IF;
          END IF;
        END LOOP;

        -- Build the JSON with the durations and record counts per table, and update the sharding_statistics row
        WITH "data" AS (
          SELECT table_name, duration, record_count
          FROM unnest(_tables) WITH ORDINALITY AS a(table_name),
            unnest(_durations) WITH ORDINALITY AS b(duration),
            unnest(_record_counts) WITH ORDINALITY AS c(record_count)
          WHERE a.ORDINALITY = b.ORDINALITY
            AND a.ORDINALITY = c.ORDINALITY
        ),
        "json_data" AS (
          SELECT ('{' || string_agg(format('"%1$s": { "count": %2$s, "duration": "%3$s" }', table_name, record_count, duration), ', ') || '}')::JSONB AS "json"
          FROM "data"
        )
        UPDATE sharding.sharding_statistics
        SET per_table = "json"
        FROM "json_data"
        WHERE sharding_key = NEW.id;

        SELECT * INTO _row FROM sharding.sharding_statistics WHERE sharding_key = NEW.id;

        RAISE DEBUG 'recreate accounting views';
        PERFORM sharding.recreate_accounting_views(NEW.id);

        RAISE DEBUG 'recreate fixedassets views';
        PERFORM sharding.recreate_fixedassets_views(cm.company_id)
          FROM public.company_modules cm
         WHERE cm.company_id = NEW.id
           AND cm.name = 'fixedassets'
           AND cm.has_schema_structure = TRUE;

        RAISE DEBUG 'recreate payroll views';
        PERFORM sharding.recreate_payroll_views(cm.company_id)
          FROM public.company_modules cm
         WHERE cm.company_id = NEW.id
           AND cm.name = 'payroll'
           AND cm.has_schema_structure = TRUE;

        -- Update all the users associated with this company, to make sure the Redis cache is refreshed
        UPDATE public.users
        SET updated_at = LOCALTIMESTAMP
        WHERE company_id = NEW.id
          OR id = NEW.accountant_id
          OR id IN (SELECT accountant_id FROM accounting.accounting_companies WHERE company_id = NEW.id);

        RETURN NEW;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS sharding.shard_table_data(TEXT[], TEXT[], INTEGER, TEXT, TEXT, TEXT);
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.shard_table_data(
        IN OUT insert_queries TEXT,
        IN OUT delete_queries TEXT,
        IN p_company_id INTEGER,
        IN p_table TEXT,
        IN p_schema_name TEXT,
        IN p_where_clause TEXT DEFAULT NULL
      )
      RETURNS record AS $BODY$
      DECLARE
        p_insert_queries TEXT[][];
        p_delete_queries TEXT[][];
        query TEXT;
      BEGIN
        p_insert_queries := insert_queries::TEXT[][];
        p_delete_queries := delete_queries::TEXT[][];
        -- RAISE DEBUG 'sharding.shard_table_data(''%'', ''%'', %, ''%'', ''%'', ''%'');', cardinality(p_insert_queries), cardinality(p_delete_queries), p_company_id, p_table, p_schema_name, p_where_clause;
        IF p_where_clause IS NULL THEN
          p_where_clause := 'company_id = %3$L';
        END IF;

        query := regexp_replace(format(
          $${{ %2$I, "SELECT common.execute_and_log_count('INSERT INTO %1$I.%2$I (SELECT * FROM ONLY public.%2$I WHERE %4$s)', 'Inserted %% rows from table public.%2$s into %1$s.%2$s', 'DEBUG');" }}$$,
          p_schema_name,
          p_table,
          p_company_id,
          regexp_replace(format(p_where_clause, p_schema_name, p_table, p_company_id), '''', '''''', 'gn')
        ), '\s+', ' ', 'gn');

        p_insert_queries := format(
          '%1$s, %2$s',
          substr(p_insert_queries::TEXT, 1, length(p_insert_queries::TEXT) - 1),
          substr(query, 2)
        )::TEXT[][];

        -- Store the sharded records into a separate table
        IF sharding.table_exists(format('sharded.%1$I', p_table)) THEN
          query := regexp_replace(format('INSERT INTO sharded.%2$I (SELECT * FROM ONLY public.%2$I WHERE ' || p_where_clause || ') RETURNING -1', p_schema_name, p_table, p_company_id), '\s+', ' ', 'gn');
        ELSE
          query := regexp_replace(format('CREATE TABLE sharded.%2$I AS SELECT * FROM ONLY public.%2$I WHERE ' || p_where_clause, p_schema_name, p_table, p_company_id || ' RETURNING -1'), '\s+', ' ', 'gn');
        END IF;

        query := format(
          $${{ sharded.%1$I, "%2$s" }}$$,
          p_table,
          query
        );

        p_insert_queries := format(
          '%1$s, %2$s',
          substr(p_insert_queries::TEXT, 1, length(p_insert_queries::TEXT) - 1),
          substr(query, 2)
        )::TEXT[][];

        -- And build the delete sharded records from the original table query (only, not from new inherited), to return from the function

        -- Execute the query outputting the affected record count
        query := format(
          $${{ %2$I, "SELECT common.execute_and_log_count('DELETE FROM ONLY public.%2$I WHERE %4$s', 'Deleted %% rows from table public.%2$s for company %3$s', 'DEBUG');" }}$$,
          p_schema_name,
          p_table,
          p_company_id,
          regexp_replace(format(p_where_clause, p_schema_name, p_table, p_company_id), '''', '''''', 'gn')
        );

        p_delete_queries := format(
          '%1$s, %2$s',
          substr(query::TEXT, 1, length(query::TEXT) - 1),
          substr(p_delete_queries::TEXT, 2)
        )::TEXT[][];

        insert_queries := p_insert_queries::TEXT;
        delete_queries := p_delete_queries::TEXT;

        RETURN;
      -- EXCEPTION
      --   WHEN OTHERS THEN
      --     RAISE WARNING '%', SQLERRM;
      --     RETURN NULL;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL
  end

  def down
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_shard_existing_data()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        _insert_queries TEXT[][];
        _delete_queries TEXT[][];
        query TEXT[];
        _start_time TIMESTAMP WITH TIME ZONE;
        _end_time TIMESTAMP WITH TIME ZONE;
        _tables TEXT[];
        _record_count INTEGER;
        _record_counts INTEGER[];
        _duration INTERVAL;
        _durations INTERVAL[];
        _index INTEGER;
        _row record;
      BEGIN
        _insert_queries := '{}'::TEXT[][];
        _delete_queries := '{}'::TEXT[][];

        -- Add timing queries
        _insert_queries := format($${{ sharding_statistics, UPDATE sharding.sharding_statistics SET data_sharding_started_at = clock_timestamp() WHERE sharding_key = %1$s RETURNING -1; }}$$, NEW.id)::TEXT[][];
        _delete_queries := format($${{ sharding_statistics, UPDATE sharding.sharding_statistics SET data_sharding_ended_at = clock_timestamp() WHERE sharding_key = %1$s RETURNING -1; }}$$, NEW.id)::TEXT[][];

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'other_entities', NEW.schema_name::text);

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'tax_descriptors', NEW.schema_name::text);

        RAISE DEBUG 'XXX insert_queries: %', _insert_queries;

        -- We need to also copy the records that don't belong to any company
        _insert_queries := array_cat(
          _insert_queries,
          ARRAY[
            'tax_descriptors',
            regexp_replace(format('INSERT INTO %1$I.tax_descriptors (SELECT * FROM ONLY public.tax_descriptors WHERE company_id IS NULL) RETURNING -1', NEW.schema_name::text), '\s+', ' ', 'gn')
          ]
        );

        RAISE DEBUG 'XXX insert_queries: %', _insert_queries;

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'bank_accounts', NEW.schema_name::text, format($$
          company_id = %1$L
          OR (entity_type IN ('Customer') AND entity_id IN (SELECT id FROM %2$I.customers WHERE company_id = %1$L))
          OR (entity_type IN ('Supplier') AND entity_id IN (SELECT id FROM %2$I.suppliers WHERE company_id = %1$L))
          OR (entity_type IN ('User', 'Manager', 'Employee', 'Accountant') AND entity_id IN (SELECT id FROM public.users WHERE company_id = %1$L))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'document_communication_jobs', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'general_ledgers', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'document_series', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'customers', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'cash_accounts', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'receipts', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'template_options', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'messages', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'item_families', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'notifications', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'units_of_measure', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'background_jobs', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'settings', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'banks', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'items', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'archives', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'suppliers', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'sub_types', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'records', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'company_divisions', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'historical_licenses', NEW.schema_name::text);

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'addresses', NEW.schema_name::text, format($$
          (addressable_type IN ('Company') AND addressable_id = %1$L)
          OR (addressable_type IN ('Customer') AND addressable_id IN (SELECT id FROM %2$I.customers WHERE company_id = %1$L))
          OR (addressable_type IN ('Supplier') AND addressable_id IN (SELECT id FROM %2$I.suppliers WHERE company_id = %1$L))
          OR (addressable_type IN ('User', 'Manager', 'Employee', 'Accountant') AND addressable_id IN (SELECT id FROM public.users WHERE company_id = %1$L))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'documents', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_expense_categories', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payments', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_mission_maps', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_reports_infos', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_documents', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'stocks_suggestions', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'stocks_documents', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'journals', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'transactions', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'customer_general_ledger_customers', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'cost_dimensions', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'service_groups', NEW.schema_name::text);

        -- We need to also copy the records that don't belong to any company
        _insert_queries := array_cat(
          _insert_queries,
          ARRAY[
            'service_groups',
            regexp_replace(format('INSERT INTO %1$I.service_groups (SELECT * FROM ONLY public.service_groups WHERE company_id IS NULL) RETURNING -1', NEW.schema_name::text), '\s+', ' ', 'gn')
          ]
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'transactions_document_type_settings', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'cost_centers', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'non_attendances', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_deduction_entities', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_calculation_results', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_accounting_accounts', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_calculations', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_tax_reports', NEW.schema_name::text);
        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_professional_categories', NEW.schema_name::text);

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'paperclip_database_storage_attachments', NEW.schema_name::text, format($$
          (attached_type = 'Company' AND attached_id = %1$L)
          OR (attached_type IN ('Manager', 'Accountant', 'User', 'Employee') AND attached_id IN (SELECT id FROM public.users WHERE company_id = %1$L))
          OR (attached_type IN ('Archive') AND attached_id IN (SELECT id FROM public.archives WHERE company_id = %1$L))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'document_communication_jobs_documents', NEW.schema_name::text, format($$
          (
            (document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L) AND document_type = 'Document')
            OR (document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L) AND document_type = 'Purchases::Document')
          )
          AND document_communication_job_id IN (SELECT id FROM %2$I.document_communication_jobs WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'customer_defaults', NEW.schema_name::text, format($$
          customer_id IN (SELECT id FROM %2$I.customers WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'support_messages', NEW.schema_name::text, format($$
          user_id IN (SELECT id FROM public.users WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'background_job_errors', NEW.schema_name::text, format($$
          background_job_id IN (SELECT id FROM %2$I.background_jobs WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'supplier_defaults', NEW.schema_name::text, format($$
          supplier_id IN (SELECT id FROM %2$I.suppliers WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'email_addresses', NEW.schema_name::text, format($$
          (email_addressable_type IN ('Supplier') AND email_addressable_id IN (SELECT id FROM %2$I.suppliers WHERE company_id = %1$L))
          OR (email_addressable_type IN ('Customer') AND email_addressable_id IN (SELECT id FROM %2$I.customers WHERE company_id = %1$L))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'contacts', NEW.schema_name::text, format($$
          (contactable_type IN ('Supplier') AND contactable_id IN (SELECT id FROM %2$I.suppliers WHERE company_id = %1$L))
          OR (contactable_type IN ('Customer') AND contactable_id IN (SELECT id FROM %2$I.customers WHERE company_id = %1$L))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT,
          NEW.id,
          'document_communication_logs',
          NEW.schema_name,
          format($$
            (
              (document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L) AND document_type = 'Document')
              OR (document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L) AND document_type = 'Purchases::Document')
            )
            AND (document_communication_job_id IS NULL AND result_code = 9999) OR document_communication_job_id IN (SELECT id FROM %2$I.document_communication_jobs WHERE company_id = %1$L)
          $$, NEW.id, NEW.schema_name)
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT,
          NEW.id,
          'public_links',
          NEW.schema_name,
          format($$
            (entity_type IN ('Document') AND entity_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L))
            OR (entity_type IN ('DueDocument') AND entity_id IN (SELECT id FROM %2$I.customers WHERE company_id = %1$L))
            OR (entity_type IN ('Receipt') AND entity_id IN (SELECT id FROM %2$I.receipts WHERE company_id = %1$L))
          $$, NEW.id, NEW.schema_name)
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT,
          NEW.id,
          'document_lines',
          NEW.schema_name,
          format($$
            document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L)
          $$, NEW.id, NEW.schema_name)
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'receipt_lines', NEW.schema_name::text, format($$
          receipt_id IN (SELECT id FROM %2$I.receipts WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT,
          NEW.id,
          'documents_receipts',
          NEW.schema_name,
          format($$
            receipt_id IN (SELECT id FROM %2$I.receipts WHERE company_id = %1$L)
            AND document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L)
          $$, NEW.id, NEW.schema_name)
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT,
          NEW.id,
          'related_documents',
          NEW.schema_name,
          format($$
            child_document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L)
            AND parent_document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L)
          $$, NEW.id, NEW.schema_name)
        );

        _insert_queries := array_cat(
          _insert_queries,
          ARRAY[
            'documents_extra_data',
            format('DELETE FROM %1$I.documents_extra_data RETURNING -1', NEW.schema_name)
          ]
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'documents_extra_data', NEW.schema_name::text, format($$
          document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_mission_map_lines', NEW.schema_name::text, format($$
          mission_map_id IN (SELECT id FROM %2$I.purchases_mission_maps WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_workflows', NEW.schema_name::text, format($$
          (entity_type = 'Purchases::MissionMap' and entity_id IN (SELECT id FROM %2$I.purchases_mission_maps WHERE company_id = %1$L))
          OR (entity_type = 'Purchases::Document' and entity_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT,
          NEW.id,
          'purchases_document_lines',
          NEW.schema_name,
          format($$
            document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L)
          $$, NEW.id, NEW.schema_name)
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payment_lines', NEW.schema_name::text, format($$
          payment_id IN (SELECT id FROM %2$I.payments WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_expenses_document_extra_data', NEW.schema_name::text, format($$
          expense_document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_related_documents', NEW.schema_name::text, format($$
          child_document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L)
          AND parent_document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchase_documents_payments', NEW.schema_name::text, format($$
          purchase_document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L)
          AND payment_id IN (SELECT id FROM %2$I.payments WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'purchases_mission_map_payments', NEW.schema_name::text, format($$
          mission_map_id IN (SELECT id FROM %2$I.purchases_mission_maps WHERE company_id = %1$L)
          AND payment_id IN (SELECT id FROM %2$I.payments WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'stocks_suggestion_lines', NEW.schema_name::text, format($$
          suggestion_id IN (SELECT id FROM %2$I.stocks_suggestions WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT,
          NEW.id,
          'stocks_document_lines',
          NEW.schema_name,
          format($$
            document_id IN (SELECT id FROM %2$I.stocks_documents WHERE company_id = %1$L)
          $$, NEW.id, NEW.schema_name)
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'stocks_stockable_data', NEW.schema_name::text, format($$
          stockable_type IN ('Item', 'Product') and stockable_id IN (SELECT id FROM %2$I.items WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT,
          NEW.id,
          'stocks_stock_movements',
          NEW.schema_name,
          format($$
            (stock_affector_type = 'Document' AND stock_affector_detail_type = 'DocumentLine' AND stock_affector_id IN (SELECT id FROM %2$I.documents) AND stock_affector_detail_id IN (SELECT id FROM %2$I.document_lines))
            OR (stock_affector_type = 'Purchases::Document' AND stock_affector_detail_type = 'Purchases::DocumentLine' AND stock_affector_id IN (SELECT id FROM %2$I.purchases_documents) AND stock_affector_detail_id IN (SELECT id FROM %2$I.purchases_document_lines))
            OR (stock_affector_type = 'Stocks::Document' AND stock_affector_detail_type = 'Stocks::DocumentLine' AND stock_affector_id IN (SELECT id FROM %2$I.stocks_documents) AND stock_affector_detail_id IN (SELECT id FROM %2$I.stocks_document_lines))
          $$, NEW.id, NEW.schema_name)
        );

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'transaction_lines', NEW.schema_name::text, format($$
          transaction_id IN (SELECT id FROM %2$I.transactions)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'analytical_transaction_lines', NEW.schema_name::text, format($$
          transaction_line_id IN (SELECT id FROM %2$I.transaction_lines)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'cost_dimension_templates', NEW.schema_name::text, format($$
          cost_center_id IN (SELECT id FROM %2$I.cost_centers)
          OR cost_dimension_id IN (SELECT id FROM %2$I.cost_dimensions)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'non_attendance_days', NEW.schema_name::text, format($$
          non_attendance_id IN (SELECT id FROM %2$I.non_attendances)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_dismissals', NEW.schema_name::text, format($$
          user_id IN (SELECT id FROM public.users WHERE company_id = %1$L)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_calculation_result_lines', NEW.schema_name::text, format($$
          payroll_calculation_result_id IN (SELECT id FROM %2$I.payroll_calculation_results)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'address_holidays', NEW.schema_name::text, format($$
          address_id IN (SELECT id FROM %2$I.addresses)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_tax_report_workflow_histories', NEW.schema_name::text, format($$
          payroll_tax_report_id IN (SELECT id FROM %2$I.payroll_tax_reports)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_item_overrides_deduction_entities', NEW.schema_name::text, format($$
          payroll_item_override_id IN (SELECT id FROM public.payroll_item_overrides WHERE company_id = %1$L)
          AND payroll_deduction_entity_id IN (SELECT id FROM %2$I.payroll_deduction_entities)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_deduction_settings', NEW.schema_name::text, format($$
          payroll_deduction_entity_id IN (SELECT id FROM %2$I.payroll_deduction_entities)
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'settlements', NEW.schema_name::text, format($$
          (associated_type = 'PaymentLine' AND associated_id IN (SELECT id FROM %2$I.payment_lines))
          OR (associated_type = 'DocumentLine' AND associated_id IN (SELECT id FROM %2$I.document_lines))
          OR (associated_type = 'Purchases::DocumentLine' AND associated_id IN (SELECT id FROM %2$I.purchases_document_lines))
          OR (associated_type = 'Document' AND associated_id IN (SELECT id FROM %2$I.documents))
          OR (associated_type = 'Purchases::Document' AND associated_id IN (SELECT id FROM %2$I.purchases_documents))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'applied_taxes', NEW.schema_name::text, format($$
          (taxable_type = 'ReceiptLine' AND taxable_id IN (SELECT id FROM %2$I.receipt_lines))
          OR (taxable_type = 'Document' AND taxable_id IN (SELECT id FROM %2$I.documents))
          OR (taxable_type = 'Receipt' AND taxable_id IN (SELECT id FROM %2$I.receipts))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'applied_vats', NEW.schema_name::text, format($$
          (reference_type = 'DocumentLine' AND reference_id IN (SELECT id FROM %2$I.document_lines))
          OR (reference_type = 'Purchases::DocumentLine' AND reference_id IN (SELECT id FROM %2$I.purchases_document_lines))
          OR (reference_type = 'Document' AND reference_id IN (SELECT id FROM %2$I.documents))
          OR (reference_type = 'Purchases::Document' AND reference_id IN (SELECT id FROM %2$I.purchases_documents))
        $$, NEW.id, NEW.schema_name));

        SELECT insert_queries::TEXT[][], delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
          FROM sharding.shard_table_data(_insert_queries::TEXT, _delete_queries::TEXT, NEW.id, 'payroll_extended_attributes', NEW.schema_name::text, format($$
          (entity_type IN ('Company') AND entity_id = %1$L)
          OR (entity_type IN ('User', 'Manager', 'Employee', 'Accountant') AND entity_id IN (SELECT id FROM public.users WHERE company_id = %1$L))
        $$, NEW.id, NEW.schema_name));

        FOR query IN SELECT reduce_array_dimension FROM public.reduce_array_dimension(_insert_queries) LOOP
          -- raise DEBUG 'INSERT query: %', query[2];

          _start_time := clock_timestamp();
          EXECUTE query[2] INTO _record_count;
          _end_time := clock_timestamp();

          IF _record_count >= 0 THEN
            _duration := _end_time - _start_time;
            _index := common.array_search(query[1], _tables);

            IF _index IS NOT NULL THEN
              _durations[_index] := _durations[_index] + _duration;
              _record_counts[_index] := _record_counts[_index] + _record_count;
            ELSE
              _tables := _tables || query[1];
              _durations := _durations || _duration;
              _record_counts := _record_counts || _record_count;
            END IF;
          END IF;
        END LOOP;

        raise notice '% % %', _tables, _durations, _record_counts;

        FOR query IN SELECT reduce_array_dimension FROM public.reduce_array_dimension(_delete_queries) LOOP
          -- raise DEBUG 'DELETE query: %', query;
          _start_time := clock_timestamp();
          EXECUTE query[2];
          _end_time := clock_timestamp();

          IF _record_count >= 0 THEN
            _duration := _end_time - _start_time;
            _index := common.array_search(query[1], _tables);

            IF _index IS NOT NULL THEN
              _durations[_index] := _durations[_index] + _duration;
            END IF;
          END IF;
        END LOOP;

        -- Build the JSON with the durations and record counts per table, and update the sharding_statistics row
        WITH "data" AS (
          SELECT table_name, duration, record_count
          FROM unnest(_tables) WITH ORDINALITY AS a(table_name),
            unnest(_durations) WITH ORDINALITY AS b(duration),
            unnest(_record_counts) WITH ORDINALITY AS c(record_count)
          WHERE a.ORDINALITY = b.ORDINALITY
            AND a.ORDINALITY = c.ORDINALITY
        ),
        "json_data" AS (
          SELECT ('{' || string_agg(format('"%1$s": { "count": %2$s, "duration": "%3$s" }', table_name, record_count, duration), ', ') || '}')::JSONB AS "json"
          FROM "data"
        )
        UPDATE sharding.sharding_statistics
        SET per_table = "json"
        FROM "json_data"
        WHERE sharding_key = NEW.id;

        SELECT * INTO _row FROM sharding.sharding_statistics WHERE sharding_key = NEW.id;

        RAISE DEBUG 'recreate accounting views';
        PERFORM sharding.recreate_accounting_views(NEW.id);

        RAISE DEBUG 'recreate fixedassets views';
        PERFORM sharding.recreate_fixedassets_views(cm.company_id)
          FROM public.company_modules cm
         WHERE cm.company_id = NEW.id
           AND cm.name = 'fixedassets'
           AND cm.has_schema_structure = TRUE;

        RAISE DEBUG 'recreate payroll views';
        PERFORM sharding.recreate_payroll_views(cm.company_id)
          FROM public.company_modules cm
         WHERE cm.company_id = NEW.id
           AND cm.name = 'payroll'
           AND cm.has_schema_structure = TRUE;

        -- Update all the users associated with this company, to make sure the Redis cache is refreshed
        UPDATE public.users
        SET updated_at = LOCALTIMESTAMP
        WHERE company_id = NEW.id
          OR id = NEW.accountant_id
          OR id IN (SELECT accountant_id FROM accounting.accounting_companies WHERE company_id = NEW.id);

        RETURN NEW;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS sharding.shard_table_data(TEXT, TEXT, INTEGER, TEXT, TEXT, TEXT);
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

        -- Execute the query directly
        -- p_insert_queries := p_insert_queries || format(
        --   'INSERT INTO %1$I.%2$I (SELECT * FROM ONLY public.%2$I WHERE ' || p_where_clause || ');',
        --   p_schema_name,
        --   p_table,
        --   p_company_id
        -- );

        -- Execute the query outputting the affected record count
        p_insert_queries := p_insert_queries || regexp_replace(format('
          SELECT common.execute_and_log_count(
            ''INSERT INTO %1$I.%2$I (SELECT * FROM ONLY public.%2$I WHERE %4$s)'',
            ''Inserted %% rows from table public.%2$s into %1$s.%2$s'',
            ''DEBUG''
          );',
          p_schema_name,
          p_table,
          p_company_id,
          regexp_replace(format(p_where_clause, p_schema_name, p_table, p_company_id), '''', '''''', 'gn')
        ), '\s+', ' ', 'gn');

        -- Store the sharded records into a separate table
        IF sharding.table_exists(format('sharded.%1$I', p_table)) THEN
          query := regexp_replace(format('INSERT INTO sharded.%2$I (SELECT * FROM ONLY public.%2$I WHERE ' || p_where_clause || ')', p_schema_name, p_table, p_company_id), '\s+', ' ', 'gn');
        ELSE
          query := regexp_replace(format('CREATE TABLE sharded.%2$I AS SELECT * FROM ONLY public.%2$I WHERE ' || p_where_clause, p_schema_name, p_table, p_company_id), '\s+', ' ', 'gn');
        END IF;

        p_insert_queries := p_insert_queries || query;

        -- And build the delete sharded records from the original table query (only, not from new inherited), to return from the function

        -- Execute the query directly
        -- p_delete_queries := array_prepend(format(
        --   'DELETE FROM ONLY public.%2$I WHERE ' || p_where_clause || ';',
        --   p_schema_name,
        --   p_table,
        --   p_company_id
        -- ), p_delete_queries);

        -- Execute the query outputting the affected record count
        p_delete_queries := array_prepend(regexp_replace(format('
          SELECT common.execute_and_log_count(
            ''DELETE FROM ONLY public.%2$I WHERE %4$s'',
            ''Deleted %% rows from table public.%2$s for company %3$s'',
            ''DEBUG''
          );',
          p_schema_name,
          p_table,
          p_company_id,
          regexp_replace(format(p_where_clause, p_schema_name, p_table, p_company_id), '''', '''''', 'gn')
        ), '\s+', ' ', 'gn'), p_delete_queries);

        RETURN;
      -- EXCEPTION
      --   WHEN OTHERS THEN
      --     RAISE WARNING '%', SQLERRM;
      --     RETURN NULL;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS common.array_search(ANYELEMENT, ANYARRAY);
    SQL
  end
end
