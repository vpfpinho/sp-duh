class AddPerTableToShardingStatistics < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      ALTER TABLE sharding.sharding_statistics ADD COLUMN per_table JSONB;
    SQL

    execute <<-'SQL'
      CREATE INDEX "sharding_statistics_per_table_idx" ON sharding.sharding_statistics USING GIN(per_table);
    SQL
  end

  def down
    execute <<-'SQL'
      DROP INDEX "sharding_statistics_per_table_idx" ON sharding.sharding_statistics;
    SQL

    execute <<-'SQL'
      ALTER TABLE sharding.sharding_statistics DROP COLUMN per_table JSONB;
    SQL
  end
end
