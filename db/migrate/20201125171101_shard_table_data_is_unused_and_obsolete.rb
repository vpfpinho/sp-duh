class ShardTableDataIsUnusedAndObsolete < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS sharding.shard_table_data(TEXT, TEXT, INTEGER, TEXT, TEXT, TEXT);
      DROP FUNCTION IF EXISTS sharding.shard_table_data(TEXT, TEXT, INTEGER, TEXT, TEXT, BOOLEAN);
      DROP FUNCTION IF EXISTS sharding.shard_table_data(TEXT, TEXT, INTEGER, TEXT, TEXT, TEXT, BOOLEAN);
    SQL
  end

  def down
    puts "Not recovering obsolete functions to shard data within the DB".red
  end
end
