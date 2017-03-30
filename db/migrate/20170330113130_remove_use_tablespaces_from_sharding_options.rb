class RemoveUseTablespacesFromShardingOptions < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      ALTER TABLE sharding.options DROP COLUMN use_tablespaces;
    SQL
  end

  def down
    execute <<-'SQL'
      ALTER TABLE sharding.options ADD COLUMN use_tablespaces BOOLEAN DEFAULT FALSE;
    SQL
  end
end
