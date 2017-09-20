class ResetJsonapiToNewVersion < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS public.jsonapi(text,text,text,text,text,text);
      DROP FUNCTION IF EXISTS public.jsonapi(text,text,text,text,text,text,text,text);
      DROP FUNCTION IF EXISTS public.jsonapi(text,text,text,text,text,text,text,text,text);
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS public.get_jsonapi_schema ();
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS public.get_jsonapi_table_prefix ();
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION public.jsonapi (
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
      CREATE OR REPLACE FUNCTION public.inside_jsonapi (
      ) RETURNS boolean AS '$libdir/pg-jsonapi.so', 'inside_jsonapi' LANGUAGE C;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION public.get_jsonapi_user (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_user' LANGUAGE C;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION public.get_jsonapi_company (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_company' LANGUAGE C;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION public.get_jsonapi_company_schema (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_company_schema' LANGUAGE C;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION public.get_jsonapi_sharded_schema (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_sharded_schema' LANGUAGE C;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION public.get_jsonapi_accounting_schema (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_accounting_schema' LANGUAGE C;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION public.get_jsonapi_accounting_prefix (
      ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_accounting_prefix' LANGUAGE C;
    SQL

  end

  def down
    puts "WARNING: Dropping new functions but NOT recreating old functions!"

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS public.jsonapi(text,text,text,text,text,text,text,text,text);
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS public.inside_jsonapi ()
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS public.get_jsonapi_user();
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS public.get_jsonapi_company();
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS public.get_jsonapi_company_schema();
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS public.get_jsonapi_sharded_schema();
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS public.get_jsonapi_accounting_schema();
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS public.get_jsonapi_accounting_prefix();
    SQL
  end
end
