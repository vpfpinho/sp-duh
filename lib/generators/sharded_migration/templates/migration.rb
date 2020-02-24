class <%= migration_class_name %> < ActiveRecord::MigrationWithoutTransaction
  def up

    # If migration DOES NOT MAKE STRUCTURAL CHANGES on public schema, you may want to regenarete the sharding.create_company_shard to be able to create companies while migrating
    # execute %Q[ SELECT sharding.generate_create_company_shard_function(); ]

    migrate_companies do |schema_name, company_id, use_sharded_company, tablespace_name|
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
      sleep 0.250
    end

    # If migration ADD STRUCTURAL CHANGES on public objects and want run migration without a full deploy, please uncomment the next line
    # invalidate_postgresql_redis_cache!
  end

  def down
    rollback_companies do |schema_name, company_id, use_sharded_company, tablespace_name|
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
      sleep 0.250
    end

    # If migration add structural changes on public objects and want run migration
    # without a full deploy, please uncomment the next line
    # invalidate_postgresql_redis_cache!
  end
end
