class InstallJsonapiStatus < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION jsonapi_status (
        IN method               text,
        IN uri                  text,
        IN body                 text DEFAULT NULL,
        IN user_id              text DEFAULT NULL,
        IN company_id           text DEFAULT NULL,
        IN company_schema       text DEFAULT NULL,
        IN sharded_schema       text DEFAULT NULL,
        IN accounting_schema    text DEFAULT NULL,
        IN accounting_prefix    text DEFAULT NULL,
        OUT http_status         integer,
        OUT response            text
      ) RETURNS record AS '$libdir/pg-jsonapi.so', 'jsonapi_status' LANGUAGE C;
    SQL
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION get_jsonapi_company_schema (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_company_schema' LANGUAGE C;
    SQL
  end

  def down
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS jsonapi_status(text,text,text,text,text,text,text,text,text);
    SQL
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS get_jsonapi_company_schema ();
    SQL
  end
end
