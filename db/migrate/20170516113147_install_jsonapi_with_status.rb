class InstallJsonapiWithStatus < ActiveRecord::Migration
  def up
    execute %Q[
      CREATE OR REPLACE FUNCTION jsonapi (
        IN method         text,
        IN uri            text,
        IN body           text, -- DEFAULT NULL
        IN schema         text, -- DEFAULT NULL
        IN prefix         text, -- DEFAULT NULL
        IN sharded_schema text, -- DEFAULT NULL
        IN user_id        text, -- DEFAULT NULL
        IN company_id     text, -- DEFAULT NULL
        OUT http_status   integer,
        OUT response      text
      ) RETURNS record AS '$libdir/pg-jsonapi.so', 'jsonapi_with_status' LANGUAGE C;
    ]
  end

  def down
    execute %Q[
      DROP FUNCTION IF EXISTS jsonapi(text,text,text,text,text,text,text,text);
    ]
  end
end
