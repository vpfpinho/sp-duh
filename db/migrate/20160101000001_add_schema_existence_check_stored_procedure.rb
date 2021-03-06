class AddSchemaExistenceCheckStoredProcedure < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION common.schema_exists(schema_name TEXT)
      RETURNS BOOLEAN AS $BODY$
      BEGIN
        RETURN (SELECT EXISTS (
          SELECT n.nspname
          FROM pg_catalog.pg_namespace n
          WHERE n.nspname = schema_name
        ));
      END;
      $BODY$ language plpgsql;
    SQL
  end

  def down
    execute %Q[DROP FUNCTION IF EXISTS common.schema_exists(TEXT);]
  end
end
