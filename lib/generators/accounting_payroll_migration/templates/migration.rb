class <%= migration_class_name %> < ActiveRecord::MigrationWithoutTransaction
  def up
    
    # If migration DOES NOT MAKE STRUCTURAL CHANGES on public schema, you may want to regenarete the sharding.create_company_shard to be able to create companies while migrating

    migrate_fiscal_years do |schema, table_prefix, fy, company_id|
      result = execute %Q[ SELECT sharded_schema FROM sharding.get_sharded_schema_name(#{company_id}); ]
      sharded_schema = result.first["sharded_schema"]
      execute <<-SQL
        DO $BODY$
        DECLARE
        BEGIN

            WITH _needs_update AS (
              SELECT tcda.id,
                     accounting.get_account_best_match('#{schema}','#{table_prefix}',COALESCE(tcda.social_organ_account::text,po.social_organ_account,pi.social_organ_account)) AS social_organ_account,
                     accounting.get_account_best_match('#{schema}','#{table_prefix}',COALESCE(tcda.employee_account::text,po.employee_account,pi.employee_account)) AS employee_account
                FROM accounting.transaction_suggestions_categories tc
                JOIN public.payroll_items pi ON pi.type = tc.id
                LEFT JOIN public.payroll_item_overrides po ON (po.company_id = #{company_id} AND po.payroll_item_id = pi.id)
                JOIN #{schema}.#{table_prefix}transaction_suggestions_default_accounts tcda ON (tcda.category_id = pi.type AND tcda.item_id = pi.id)
               WHERE ( pi.company_id = #{company_id} OR pi.company_id IS NULL )
                 AND (tcda.social_organ_account IS NULL OR tcda.employee_account IS NULL)
            )
            UPDATE #{schema}.#{table_prefix}transaction_suggestions_default_accounts
               SET social_organ_account =  _needs_update.social_organ_account,
                   employee_account =  _needs_update.employee_account
              FROM _needs_update
             WHERE #{schema}.#{table_prefix}transaction_suggestions_default_accounts.id = _needs_update.id
               AND (_needs_update.social_organ_account IS NOT NULL OR _needs_update.employee_account IS NOT NULL);

            INSERT INTO #{schema}.#{table_prefix}transaction_suggestions_default_accounts
                   (category_id, item_id, social_organ_account, employee_account, use_employee_account)
            SELECT pi.type AS category_id,
                   pi.id AS item_id,
                   accounting.get_account_best_match('#{schema}','#{table_prefix}',COALESCE(po.social_organ_account,pi.social_organ_account)) AS social_organ_account,
                   accounting.get_account_best_match('#{schema}','#{table_prefix}',COALESCE(po.employee_account,pi.employee_account)) AS employee_account,
                   COALESCE(po.use_employee_account,pi.use_employee_account) AS use_employee_account
              FROM accounting.transaction_suggestions_categories tc
              JOIN public.payroll_items pi ON pi.type = tc.id
              LEFT JOIN public.payroll_item_overrides po ON (po.company_id = #{company_id} AND po.payroll_item_id = pi.id)
              LEFT JOIN #{schema}.#{table_prefix}transaction_suggestions_default_accounts tcda ON (tcda.category_id = pi.type AND tcda.item_id = pi.id)
             WHERE ( pi.company_id = #{company_id} OR pi.company_id IS NULL )
               AND tcda IS NULL
             ORDER BY pi.id;

            INSERT INTO #{schema}.#{table_prefix}transaction_suggestions_default_accounts
                   (category_id, item_id, social_organ_account, employee_account, use_employee_account)
            SELECT CASE
                        WHEN paa.category = 'Taxes'         THEN 'Payroll::Tax'
                        WHEN paa.category = 'Third parties' THEN 'Payroll::ThirdParty'
                    END AS category_id,
                   paa.id AS item_id,
                   accounting.get_account_best_match('#{schema}','#{table_prefix}',paa.social_organ_account) AS social_organ_account,
                   accounting.get_account_best_match('#{schema}','#{table_prefix}',paa.employee_account) AS employee_account,
                   paa.use_employee_account
              FROM #{sharded_schema}.payroll_accounting_accounts paa
              LEFT JOIN #{schema}.#{table_prefix}transaction_suggestions_default_accounts tcda ON (tcda.category_id IN ('Payroll::Tax','Payroll::ThirdParty') AND tcda.item_id = paa.id)
             WHERE paa.company_id = #{company_id}
               AND paa.category IN ('Taxes', 'Third parties')
               AND paa.code NOT IN ('BBNK','BCSH')
               AND tcda IS NULL
             ORDER BY paa.id;
        END;
        $BODY$ LANGUAGE 'plpgsql';
      SQL
    end
  end

  def down
    puts "Not reverting"
    rollback_fiscal_years {}

  end

end
