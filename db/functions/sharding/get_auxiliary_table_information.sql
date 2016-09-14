DROP FUNCTION IF EXISTS sharding.get_auxiliary_table_information();

CREATE OR REPLACE FUNCTION sharding.get_auxiliary_table_information(
  OUT auxiliary_table_information JSONB
)
RETURNS JSONB AS $BODY$
BEGIN

  -- 'payroll_items' was moved from inherited_tables to unsharded_tables to postpone all needed changes on toconline
  auxiliary_table_information = '{
    "unsharded_tables": [
      "admin_users",
      "banks_lookup",
      "companies",
      "company_modules",
      "countries",
      "currencies",
      "document_themes",
      "enumerations_document_types",
      "enumerations_expense_category_groupings",
      "enumerations_profit_and_losses_categories",
      "helpdesk_learnings",
      "impersonated_logins",
      "jsonapi_config",
      "mb_references",
      "payroll_employee_types",
      "payroll_enumerations_globals",
      "payroll_enumerations_holidays",
      "payroll_enumerations_lookup_insurance_companies",
      "payroll_enumerations_marital_statuses",
      "payroll_enumerations_ruas",
      "payroll_enumerations_statement_codes",
      "payroll_enumerations_tax_offices",
      "payroll_global_settings",
      "payroll_grant_types",
      "payroll_historical_irs_withholding_limits",
      "payroll_historical_irs_withholdings",
      "payroll_irs_withholding_limits",
      "payroll_irs_withholdings",
      "payroll_item_overrides",
      "payroll_items",
      "pending_company_creations",
      "purchased_periods",
      "roles",
      "saft_sources",
      "schema_migrations",
      "system_counties",
      "system_districts",
      "system_post_codes",
      "task_histories",
      "tax_exemption_reasons",
      "taxes",
      "temporary_uploads",
      "users",
      "users_roles",
      "vat_taxes"
    ],
    "inherited_tables": [
    ]
  }'::JSONB;

  RETURN;

END;
$BODY$ LANGUAGE 'plpgsql' STABLE;
