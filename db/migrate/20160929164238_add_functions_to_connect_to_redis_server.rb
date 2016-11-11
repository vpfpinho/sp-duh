class AddFunctionsToConnectToRedisServer < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE SCHEMA IF NOT EXISTS redis;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION redis.create_redis_server_connection(
        IN server_name      TEXT,
        IN host             TEXT DEFAULT 'localhost',
        IN port             INTEGER DEFAULT 6379,
        IN database_number  INTEGER DEFAULT 0
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
      BEGIN
        CREATE EXTENSION IF NOT EXISTS redis_fdw;

        -- Create the server
        EXECUTE format($$
          CREATE SERVER %1$s_redis_server
            FOREIGN DATA WRAPPER redis_fdw
            OPTIONS (host '%2$s', port '%3$s');
        $$, server_name, host, port, database_number);

        -- Create the user mapping
        EXECUTE format($$
          CREATE USER MAPPING FOR PUBLIC
            SERVER %1$s_redis_server;
        $$, server_name, host, port, database_number);

        -- Create the cache entries table
        EXECUTE format($$
          CREATE FOREIGN TABLE redis.%1$s_cache_entries (
            "key" TEXT,
            "value" TEXT
          )
            SERVER %1$s_redis_server
            OPTIONS (tabletype 'string', database '%4$s');
        $$, server_name, host, port, database_number);

        EXECUTE format($$
          ALTER FOREIGN TABLE redis.%1$s_cache_entries
            ALTER COLUMN "value"
            OPTIONS (ADD redis 'value');
        $$, server_name, host, port, database_number);

        EXECUTE format($$
          CREATE FOREIGN TABLE redis.%1$s_cache_keys(
            "key" TEXT
          )
            SERVER %1$s_redis_server
            OPTIONS (tabletype 'keys', database '%4$s');
        $$, server_name, host, port, database_number);

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION redis.create_trigger_to_delete_cache_keys(
        IN server_name      TEXT,
        IN table_name       TEXT,
        IN key_pattern      TEXT
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        table_schema TEXT;
      BEGIN

        table_schema := COALESCE(NULLIF(regexp_replace(table_name, '^(?:(.*?\.))?(.*?)$', '\1'), ''), 'public');
        table_name := regexp_replace(table_name, '^(?:.*?\.)?(.*?)$', '\1');

        EXECUTE format($$
          CREATE CONSTRAINT TRIGGER trg_clear_redis_cache_from_%1$s
          AFTER UPDATE OR DELETE ON %2$I.%3$I
            DEFERRABLE INITIALLY DEFERRED
          FOR EACH ROW
            EXECUTE PROCEDURE redis.trf_delete_affected_optimizations_cache_entries('%1$s', '%4$s');
        $$, server_name, table_schema, table_name, key_pattern);

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION redis.trf_delete_affected_optimizations_cache_entries()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        redis_server_name   TEXT;
        key_pattern         TEXT;
        key_field_values    TEXT[];
        affected_key        TEXT;
        aux                 TEXT;
        affected_keys       TEXT[];
        triggering_record   RECORD;
      BEGIN
        redis_server_name := TG_ARGV[0];
        key_pattern := TG_ARGV[1];

        IF TG_OP = 'DELETE' THEN
          triggering_record := OLD;
        ELSE
          triggering_record := NEW;
        END IF;

        SELECT format('$1.%1$s', string_agg(unnest, ', $1.'))
        INTO aux
        FROM unnest((
          SELECT array_agg(regexp_matches)
          FROM regexp_matches(key_pattern, '\$([a-zA-Z0-9_]+)\$', 'g')
        ));

        key_pattern := regexp_replace(key_pattern, '\$([a-zA-Z0-9_]+)\$', '%s', 'g');

        EXECUTE format('SELECT ARRAY[%1$s]::TEXT[]', aux)
        USING triggering_record
        INTO key_field_values;

        key_pattern := regexp_replace(format(key_pattern, VARIADIC key_field_values), '\*', '%', 'g');

        EXECUTE format($$
          SELECT array_agg("key")
          FROM "redis"."%1$s_cache_keys"
          WHERE "key" LIKE '%2$s'
        $$, redis_server_name, key_pattern) INTO affected_keys;

        IF affected_keys IS NOT NULL THEN
          FOREACH affected_key IN ARRAY affected_keys LOOP
            EXECUTE format('DELETE FROM "redis"."%1$s_cache_entries" WHERE "key" = %2$L;', redis_server_name, affected_key);
          END LOOP;
        END IF;

        RETURN triggering_record;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION redis.delete_trigger_to_delete_cache_keys(
        IN server_name      TEXT,
        IN table_name       TEXT
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        table_schema TEXT;
      BEGIN

        table_schema := COALESCE(NULLIF(regexp_replace(table_name, '^(?:(.*?\.))?(.*?)$', '\1'), ''), 'public');
        table_name := regexp_replace(table_name, '^(?:.*?\.)?(.*?)$', '\1');

        EXECUTE format($$
          DROP TRIGGER trg_clear_redis_cache_from_%1$s ON %2$I.%3$I;
        $$, server_name, table_schema, table_name);

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION redis.delete_redis_server_connection(
        IN server_name      TEXT
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
      BEGIN
        -- Create the server
        EXECUTE format($$
          DROP SERVER IF EXISTS %1$s_redis_server CASCADE;
        $$, server_name);

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL
  end

  def down
    execute %Q[DROP FUNCTION IF EXISTS redis.trf_delete_affected_optimizations_cache_entries() CASCADE;]
    execute %Q[DROP FUNCTION IF EXISTS redis.create_trigger_to_delete_cache_keys(TEXT, TEXT, TEXT);]
    execute %Q[DROP FUNCTION IF EXISTS redis.create_redis_server_connection(TEXT, TEXT, INTEGER, INTEGER);]
    execute %Q[DROP FUNCTION IF EXISTS redis.delete_trigger_to_delete_cache_keys(TEXT, TEXT);]
    execute %Q[DROP FUNCTION IF EXISTS redis.delete_redis_server_connection(TEXT);]
  end
end
