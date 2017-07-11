class <%= migration_class_name %> < ActiveRecord::MigrationWithoutTransaction
  def up
    schema = 'accounting'
    ['base_', 'small_', 'micro_', 'independent_', 'nonprofit_'].each do |table_prefix|
      # Code to run on "accounting" schema

    end

    migrate_user_templates do |schema, table_prefix, ut|
      # Code to run on user templates' schemas

      # IF CHANGING TABLE we need to keep low cpu usage on DB
      sleep 0.050
    end

    migrate_fiscal_years do |schema, table_prefix, fy|
      # Code to run on fiscal years' schemas

      # IF CHANGING TABLE we need to keep low cpu usage on DB
      sleep 0.050
    end
  end

  def down
    schema = 'accounting'
    ['base_', 'small_', 'micro_', 'independent_', 'nonprofit_'].each do |table_prefix|
      # Code to run on "accounting" schema

    end

    rollback_user_templates do |schema, table_prefix, ut|
      # Code to run on user templates' schemas

    end

    rollback_fiscal_years do |schema, table_prefix, fy|
      # Code to run on fiscal years' schemas

    end
  end

end
