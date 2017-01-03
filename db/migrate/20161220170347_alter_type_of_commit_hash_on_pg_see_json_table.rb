class AlterTypeOfCommitHashOnPgSeeJsonTable < ActiveRecord::Migration
  def up
    execute <<-SQL
      ALTER TABLE public.pg_see_json_table
     ALTER COLUMN commit_hash TYPE CHARACTER VARYING(255);
    SQL
  end

  def down
    puts 'Nothing to undo'
  end
end
