class <%= migration_class_name %> < ActiveRecord::MigrationWithoutTransaction
  def up
    migrate_companies do |schema_name, company_id|
      case schema_name
        when 'public'
          # Code to run on "public" schema

        when 'sharded'
          # Code to run on "sharded" schema
          # STUCTURE CHANGES TO TABLES IN THIS SCHEMA MUST ONLY RUN IF ADDING OR REMOVING COLUMNS, NOTHING ELSE!

        else
          # Code to run on sharded companies' schemas

      end
      # IF CHANGING TABLE we need to keep low cpu usage on DB
      sleep 0.100
    end

  end

  def down
    rollback_companies do |schema_name, company_id|
      case schema_name
        when 'public'
          # Code to run on "public" schema

        when 'sharded'
          # Code to run on "sharded" schema
          # STUCTURE CHANGES TO TABLES IN THIS SCHEMA MUST ONLY RUN IF ADDING OR REMOVING COLUMNS, NOTHING ELSE!

        else
          # Code to run on sharded companies' schemas

      end
    end
  end
end