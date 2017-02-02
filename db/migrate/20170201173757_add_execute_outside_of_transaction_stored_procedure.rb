class AddExecuteOutsideOfTransactionStoredProcedure < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION common.execute_outside_of_transaction(query TEXT)
      RETURNS TEXT AS
      $BODY$
      DECLARE
        port INTEGER;
        dbname TEXT;
        server_connection TEXT;
      BEGIN
        port := (SELECT setting FROM pg_settings WHERE name = 'port');
        dbname := current_database();

        server_connection := format('port=%1$s dbname=%2$s', port, dbname);

        RETURN (SELECT public.dblink_exec(server_connection, query));
      END;
      $BODY$ language plpgsql;
  end

  def down
    execute %Q[DROP FUNCTION IF EXISTS common.execute_outside_of_transaction(TEXT);]
  end
end
