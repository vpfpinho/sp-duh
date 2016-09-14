DROP FUNCTION IF EXISTS sharding.trf_shard_existing_data() CASCADE;

CREATE OR REPLACE FUNCTION sharding.trf_shard_existing_data()
RETURNS TRIGGER AS $BODY$
DECLARE
  _insert_queries TEXT[];
  _delete_queries TEXT[];
  query TEXT;
BEGIN

  _insert_queries := '{}';
  _delete_queries := '{}';

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'other_entities', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'tax_descriptors', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'bank_accounts', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'document_communication_jobs', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'general_ledgers', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'document_series', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'customers', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'cash_accounts', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'receipts', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'template_options', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'messages', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'item_families', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'notifications', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'units_of_measure', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'background_jobs', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'settings', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'banks', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'items', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'archives', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'suppliers', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'records', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'company_divisions', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'historical_licenses', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'documents', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'purchases_expense_categories', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'payments', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'purchases_mission_maps', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'purchases_reports_infos', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'purchases_documents', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'stocks_suggestions', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'stocks_documents', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'journals', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'transactions', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'customer_general_ledger_customers', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'cost_dimensions', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'service_groups', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'transactions_document_type_settings', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'cost_centers', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'non_attendances', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'payroll_deduction_entities', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'payroll_calculation_results', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'payroll_accounting_accounts', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'payroll_calculations', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'payroll_tax_reports', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'payroll_professional_categories', NEW.schema_name::text);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'sub_types', NEW.schema_name::text);

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'paperclip_database_storage_attachments', NEW.schema_name::text, format($$
    (attached_type = 'Company' AND attached_id = %1$L)
    OR (attached_type IN ('Manager', 'Accountant', 'User', 'Employee') AND attached_id IN (SELECT id FROM public.users WHERE company_id = %1$L))
    OR (attached_type IN ('Archive') AND attached_id IN (SELECT id FROM public.archives WHERE company_id = %1$L))
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'document_communication_jobs_documents', NEW.schema_name::text, format($$
    document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L)
    AND document_communication_job_id IN (SELECT id FROM %2$I.document_communication_jobs WHERE company_id = %1$L)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'customer_defaults', NEW.schema_name::text, format($$
    customer_id IN (SELECT id FROM %2$I.customers WHERE company_id = %1$L)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'user_message_statuses', NEW.schema_name::text, format($$
    message_id IN (SELECT id FROM %2$I.messages WHERE company_id = %1$L)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'support_messages', NEW.schema_name::text, format($$
    user_id IN (SELECT id FROM public.users WHERE company_id = %1$L)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'background_job_errors', NEW.schema_name::text, format($$
    background_job_id IN (SELECT id FROM %2$I.background_jobs WHERE company_id = %1$L)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'supplier_defaults', NEW.schema_name::text, format($$
    supplier_id IN (SELECT id FROM %2$I.suppliers WHERE company_id = %1$L)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'company_certificates', NEW.schema_name::text, format($$
    entity_type = 'Company' AND entity_id = %1$L
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'email_addresses', NEW.schema_name::text, format($$
    (email_addressable_type IN ('Supplier') AND email_addressable_id IN (SELECT id FROM %2$I.suppliers WHERE company_id = %1$L))
    OR (email_addressable_type IN ('Customer') AND email_addressable_id IN (SELECT id FROM %2$I.customers WHERE company_id = %1$L))
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'contacts', NEW.schema_name::text, format($$
    (contactable_type IN ('Supplier') AND contactable_id IN (SELECT id FROM %2$I.suppliers WHERE company_id = %1$L))
    OR (contactable_type IN ('Customer') AND contactable_id IN (SELECT id FROM %2$I.customers WHERE company_id = %1$L))
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries,
    NEW.id,
    'document_communication_logs',
    NEW.schema_name,
    format($$
      document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L)
      AND document_communication_job_id IN (SELECT id FROM %2$I.document_communication_jobs WHERE company_id = %1$L)
    $$, NEW.id, NEW.schema_name)
  );

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries,
    NEW.id,
    'public_links',
    NEW.schema_name,
    format($$
      (entity_type IN ('Document') AND entity_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L))
      OR (entity_type IN ('DueDocument') AND entity_id IN (SELECT id FROM %2$I.customers WHERE company_id = %1$L))
      OR (entity_type IN ('Receipt') AND entity_id IN (SELECT id FROM %2$I.receipts WHERE company_id = %1$L))
    $$, NEW.id, NEW.schema_name)
  );

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'addresses', NEW.schema_name::text, format($$
    (addressable_type IN ('Company') AND addressable_id = %1$L)
    OR (addressable_type IN ('Customer') AND addressable_id IN (SELECT id FROM %2$I.customers WHERE company_id = %1$L))
    OR (addressable_type IN ('Supplier') AND addressable_id IN (SELECT id FROM %2$I.suppliers WHERE company_id = %1$L))
    OR (addressable_type IN ('User', 'Manager', 'Employee', 'Accountant') AND addressable_id IN (SELECT id FROM public.users WHERE company_id = %1$L))
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries,
    NEW.id,
    'document_lines',
    NEW.schema_name,
    format($$
      document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L)
    $$, NEW.id, NEW.schema_name)
  );

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'receipt_lines', NEW.schema_name::text, format($$
    receipt_id IN (SELECT id FROM %2$I.receipts WHERE company_id = %1$L)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries,
    NEW.id,
    'documents_receipts',
    NEW.schema_name,
    format($$
      receipt_id IN (SELECT id FROM %2$I.receipts WHERE company_id = %1$L)
      AND document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L)
    $$, NEW.id, NEW.schema_name)
  );

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries,
    NEW.id,
    'related_documents',
    NEW.schema_name,
    format($$
      child_document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L)
      AND parent_document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L)
    $$, NEW.id, NEW.schema_name)
  );

  _insert_queries := _insert_queries || format('DELETE FROM %1$I.documents_extra_data', NEW.schema_name);
  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'documents_extra_data', NEW.schema_name::text, format($$
    document_id IN (SELECT id FROM %2$I.documents WHERE company_id = %1$L)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'purchases_mission_map_lines', NEW.schema_name::text, format($$
    mission_map_id IN (SELECT id FROM %2$I.purchases_mission_maps WHERE company_id = %1$L)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'purchases_workflows', NEW.schema_name::text, format($$
    (entity_type = 'Purchases::MissionMap' and entity_id IN (SELECT id FROM %2$I.purchases_mission_maps WHERE company_id = %1$L))
    OR (entity_type = 'Purchases::Document' and entity_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L))
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries,
    NEW.id,
    'purchases_document_lines',
    NEW.schema_name,
    format($$
      document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L)
    $$, NEW.id, NEW.schema_name)
  );

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'payment_lines', NEW.schema_name::text, format($$
    payment_id IN (SELECT id FROM %2$I.payments WHERE company_id = %1$L)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'purchases_expenses_document_extra_data', NEW.schema_name::text, format($$
    expense_document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'purchases_related_documents', NEW.schema_name::text, format($$
    child_document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L)
    AND parent_document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'purchase_documents_payments', NEW.schema_name::text, format($$
    purchase_document_id IN (SELECT id FROM %2$I.purchases_documents WHERE company_id = %1$L)
    AND payment_id IN (SELECT id FROM %2$I.payments WHERE company_id = %1$L)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'purchases_mission_map_payments', NEW.schema_name::text, format($$
    mission_map_id IN (SELECT id FROM %2$I.purchases_mission_maps WHERE company_id = %1$L)
    AND payment_id IN (SELECT id FROM %2$I.payments WHERE company_id = %1$L)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'stocks_suggestion_lines', NEW.schema_name::text, format($$
    suggestion_id IN (SELECT id FROM %2$I.stocks_suggestions WHERE company_id = %1$L)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries,
    NEW.id,
    'stocks_document_lines',
    NEW.schema_name,
    format($$
      document_id IN (SELECT id FROM %2$I.stocks_documents WHERE company_id = %1$L)
    $$, NEW.id, NEW.schema_name)
  );

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'stocks_stockable_data', NEW.schema_name::text, format($$
    stockable_type IN ('Item', 'Product') and stockable_id IN (SELECT id FROM %2$I.items WHERE company_id = %1$L)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries,
    NEW.id,
    'stocks_stock_movements',
    NEW.schema_name,
    format($$
      (stock_affector_type = 'Document' AND stock_affector_detail_type = 'DocumentLine' AND stock_affector_id IN (SELECT id FROM %2$I.documents) AND stock_affector_detail_id IN (SELECT id FROM %2$I.document_lines))
      OR (stock_affector_type = 'Purchases::Document' AND stock_affector_detail_type = 'Purchases::DocumentLine' AND stock_affector_id IN (SELECT id FROM %2$I.purchases_documents) AND stock_affector_detail_id IN (SELECT id FROM %2$I.purchases_document_lines))
      OR (stock_affector_type = 'Stocks::Document' AND stock_affector_detail_type = 'Stocks::DocumentLine' AND stock_affector_id IN (SELECT id FROM %2$I.stocks_documents) AND stock_affector_detail_id IN (SELECT id FROM %2$I.stocks_document_lines))
    $$, NEW.id, NEW.schema_name)
  );

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'transaction_lines', NEW.schema_name::text, format($$
    transaction_id IN (SELECT id FROM %2$I.transactions)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'analytical_transaction_lines', NEW.schema_name::text, format($$
    transaction_line_id IN (SELECT id FROM %2$I.transaction_lines)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'cost_dimension_templates', NEW.schema_name::text, format($$
    cost_center_id IN (SELECT id FROM %2$I.cost_centers)
    OR cost_dimension_id IN (SELECT id FROM %2$I.cost_dimensions)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'non_attendance_days', NEW.schema_name::text, format($$
    non_attendance_id IN (SELECT id FROM %2$I.non_attendances)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'payroll_dismissals', NEW.schema_name::text, format($$
    user_id IN (SELECT id FROM public.users WHERE company_id = %1$L)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'payroll_calculation_result_lines', NEW.schema_name::text, format($$
    payroll_calculation_result_id IN (SELECT id FROM %2$I.payroll_calculation_results)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'address_holidays', NEW.schema_name::text, format($$
    address_id IN (SELECT id FROM %2$I.addresses)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'payroll_tax_report_workflow_histories', NEW.schema_name::text, format($$
    payroll_tax_report_id IN (SELECT id FROM %2$I.payroll_tax_reports)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'payroll_item_overrides_deduction_entities', NEW.schema_name::text, format($$
    payroll_item_override_id IN (SELECT id FROM public.payroll_item_overrides WHERE company_id = %1$L)
    AND payroll_deduction_entity_id IN (SELECT id FROM %2$I.payroll_deduction_entities)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'payroll_deduction_settings', NEW.schema_name::text, format($$
    payroll_deduction_entity_id IN (SELECT id FROM %2$I.payroll_deduction_entities)
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'settlements', NEW.schema_name::text, format($$
    (associated_type = 'PaymentLine' AND associated_id IN (SELECT id FROM %2$I.payment_lines))
    OR (associated_type = 'DocumentLine' AND associated_id IN (SELECT id FROM %2$I.document_lines))
    OR (associated_type = 'Purchases::DocumentLine' AND associated_id IN (SELECT id FROM %2$I.purchases_document_lines))
    OR (associated_type = 'Document' AND associated_id IN (SELECT id FROM %2$I.documents))
    OR (associated_type = 'Purchases::Document' AND associated_id IN (SELECT id FROM %2$I.purchases_documents))
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'applied_taxes', NEW.schema_name::text, format($$
    (taxable_type = 'ReceiptLine' AND taxable_id IN (SELECT id FROM %2$I.receipt_lines))
    OR (taxable_type = 'Document' AND taxable_id IN (SELECT id FROM %2$I.documents))
    OR (taxable_type = 'Receipt' AND taxable_id IN (SELECT id FROM %2$I.receipts))
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'applied_vats', NEW.schema_name::text, format($$
    (reference_type = 'DocumentLine' AND reference_id IN (SELECT id FROM %2$I.document_lines))
    OR (reference_type = 'Purchases::DocumentLine' AND reference_id IN (SELECT id FROM %2$I.purchases_document_lines))
    OR (reference_type = 'Document' AND reference_id IN (SELECT id FROM %2$I.documents))
    OR (reference_type = 'Purchases::Document' AND reference_id IN (SELECT id FROM %2$I.purchases_documents))
  $$, NEW.id, NEW.schema_name));

  SELECT insert_queries, delete_queries INTO _insert_queries, _delete_queries
    FROM sharding.shard_table_data(_insert_queries, _delete_queries, NEW.id, 'payroll_extended_attributes', NEW.schema_name::text, format($$
    (entity_type IN ('Company') AND entity_id = %1$L)
    OR (entity_type IN ('User', 'Manager', 'Employee', 'Accountant') AND entity_id IN (SELECT id FROM public.users WHERE company_id = %1$L))
  $$, NEW.id, NEW.schema_name));

  FOR query IN SELECT unnest FROM unnest(_insert_queries) LOOP
    raise NOTICE 'INSERT query: %', query;
    EXECUTE query;
  END LOOP;
  FOR query IN SELECT unnest FROM unnest(_delete_queries) LOOP
    raise NOTICE 'DELETE query: %', query;
    EXECUTE query;
  END LOOP;

  RAISE NOTICE 'recreate accounting views';
  PERFORM sharding.recreate_accounting_views(NEW.id);

  RETURN NEW;
END;
$BODY$ LANGUAGE plpgsql;