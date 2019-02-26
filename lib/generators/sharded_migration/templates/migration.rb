class <%= migration_class_name %> < ActiveRecord::MigrationWithoutTransaction
  def up
    # If migration add structural changes on public objects and want run migration
    # without a full deploy, please uncomment the next line (this is not the best way to grant this, but it will work for sure...)
    # execute %Q[ DROP FUNCTION IF EXISTS sharding.create_company_shard ( integer, text, sharding.sharding_triggered_by); ]

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

    # If migration add structural changes on public objects and want run migration
    # without a full deploy, please uncomment the next line
    # invalidate_postgresql_redis_cache!
    # execute %Q[ SELECT sharding.generate_create_company_shard_function(FALSE); ]
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
