class AddShardedSchemaToJsonapi < ActiveRecord::Migration
  def up
    # drop existing version because function parameters will change
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS jsonapi(text, text, text, text, text);
    SQL
    JSONAPI.service.setup
  end

  def down
    puts "NOT REVERTING FUNCTION DECLARATION!"
  end
end
