class CreateFunctionsForJsonapiArguments < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION get_jsonapi_schema (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_schema' LANGUAGE C;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION get_jsonapi_table_prefix (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_table_prefix' LANGUAGE C;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION get_jsonapi_sharded_schema (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_sharded_schema' LANGUAGE C;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION get_jsonapi_user (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_user' LANGUAGE C;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION get_jsonapi_company (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_company' LANGUAGE C;
    SQL
  end

  def down
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS get_jsonapi_schema ();
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS get_jsonapi_table_prefix ();
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS get_jsonapi_sharded_schema ();
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS get_jsonapi_user ();
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS get_jsonapi_company ();
    SQL
  end
end
