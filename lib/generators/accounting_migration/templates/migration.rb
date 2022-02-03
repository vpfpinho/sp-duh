class <%= migration_class_name %> < ActiveRecord::MigrationWithoutTransaction
  def up

    # If migration DOES NOT MAKE STRUCTURAL CHANGES on public schema
    # regenerate the sharding.create_company_shard to be able to create companies while migrating
    execute %Q[ SELECT sharding.generate_create_company_shard_function(); ]

    schema = 'accounting'
    ['base_', 'small_', 'micro_', 'independent_', 'nonprofit_'].each do |table_prefix|
      # Code to run on "accounting" schema

    end

    migrate_user_templates do |schema, table_prefix, ut, tablespace_name|
      # Code to run on user templates' schemas

      # IF CHANGING TABLE we need to keep low cpu usage on DB
      sleep 0.250
    end

    migrate_fiscal_years do |schema, table_prefix, fy, company_id, tablespace_name, company_schema, use_sharded_company|
      # Code to run on fiscal years' schemas

      # IF CHANGING TABLE we need to keep low cpu usage on DB
      sleep 0.250
    end
  end

  def down
    schema = 'accounting'
    ['base_', 'small_', 'micro_', 'independent_', 'nonprofit_'].each do |table_prefix|
      # Code to run on "accounting" schema

    end

    rollback_user_templates do |schema, table_prefix, ut, tablespace_name|
      # Code to run on user templates' schemas

      # IF CHANGING TABLE we need to keep low cpu usage on DB
      sleep 0.250
    end

    rollback_fiscal_years do |schema, table_prefix, fy, company_id, tablespace_name, company_schema, use_sharded_company|
      # Code to run on fiscal years' schemas

      # IF CHANGING TABLE we need to keep low cpu usage on DB
      sleep 0.250
    end
  end

end
