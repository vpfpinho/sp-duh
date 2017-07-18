class InstallNewJsonapi < ActiveRecord::Migration
  def up
    execute %Q[
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

      CREATE OR REPLACE FUNCTION inside_jsonapi (
      ) RETURNS boolean AS '$libdir/pg-jsonapi.so', 'inside_jsonapi' LANGUAGE C;

      CREATE OR REPLACE FUNCTION get_jsonapi_user (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_user' LANGUAGE C;

      CREATE OR REPLACE FUNCTION get_jsonapi_company (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_company' LANGUAGE C;

      CREATE OR REPLACE FUNCTION get_jsonapi_company_schema (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_company_schema' LANGUAGE C;

      CREATE OR REPLACE FUNCTION get_jsonapi_sharded_schema (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_sharded_schema' LANGUAGE C;

      CREATE OR REPLACE FUNCTION get_jsonapi_accounting_schema (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_accounting_schema' LANGUAGE C;

      CREATE OR REPLACE FUNCTION get_jsonapi_accounting_prefix (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_accounting_prefix' LANGUAGE C;
    ]
  end

  def down
    execute %Q[
      DROP FUNCTION IF EXISTS jsonapi(text,text,text,text,text,text,text,text,text);
      DROP FUNCTION IF EXISTS inside_jsonapi();
      DROP FUNCTION IF EXISTS get_jsonapi_user();
      DROP FUNCTION IF EXISTS get_jsonapi_company();
      DROP FUNCTION IF EXISTS get_jsonapi_company_schema();
      DROP FUNCTION IF EXISTS get_jsonapi_sharded_schema();
      DROP FUNCTION IF EXISTS get_jsonapi_accounting_schema();
      DROP FUNCTION IF EXISTS get_jsonapi_accounting_prefix();
    ]
  end
end
