class AddCommitHashOnPgSeeJsonTable < ActiveRecord::Migration
  def up
    execute <<-SQL
      DO $$
        BEGIN
          BEGIN
            ALTER TABLE public.pg_see_json_table
              ADD COLUMN commit_hash CHARACTER VARYING(40);
          EXCEPTION
            WHEN duplicate_column THEN RAISE NOTICE 'column <commit_hash> already exists in <pg_see_json_table>.';
          END;
        END;
      $$
    SQL
  end

  def down
    execute <<-SQL
      ALTER TABLE public.pg_see_json_table
        DROP COLUMN commit_hash;
    SQL
  end
end
