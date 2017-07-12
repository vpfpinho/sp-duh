class InstallJsonapiStatus < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION jsonapi (
        IN method               text,
        IN uri                  text,
        IN body                 text,
        IN user_id              text,
        IN company_id           text,
        IN company_schema       text,
        IN sharded_schema       text,
        IN accounting_schema    text,
        IN accounting_prefix    text,
        OUT http_status         integer,
        OUT response            text
      ) RETURNS record AS '$libdir/pg-jsonapi.so', 'jsonapi' LANGUAGE C;
    SQL
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION get_jsonapi_company_schema (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_company_schema' LANGUAGE C;
    SQL
  end

  def down
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS jsonapi(text,text,text,text,text,text,text,text,text);
    SQL
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS get_jsonapi_company_schema ();
      DROP FUNCTION IF EXISTS get_jsonapi_accounting_schema ();
      DROP FUNCTION IF EXISTS get_jsonapi_company_schema ();
    SQL
  end
end
