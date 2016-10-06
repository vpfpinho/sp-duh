class AddGetTableSchemaAndNameStoredProcedure < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION common.get_table_schema_and_name(
        IN  full_table_name      TEXT,
        IN  default_schema_name  TEXT DEFAULT NULL,
        OUT "name" TEXT,
        OUT "schema" TEXT
      )
      RETURNS record AS $BODY$
      BEGIN
        IF default_schema_name IS NULL THEN
          default_schema_name := 'public';
        END IF;

        "schema" := COALESCE(NULLIF(regexp_replace(full_table_name, '^(?:(.*?)\.)?(.*?)$', '\1'), ''), default_schema_name);
        "name" := regexp_replace(full_table_name, '^(?:.*?\.)?(.*?)$', '\1');

        RETURN;
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
        SELECT "name", "schema"
        FROM common.get_table_schema_and_name(table_name)
        INTO table_name, table_schema;

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
      CREATE OR REPLACE FUNCTION redis.delete_trigger_to_delete_cache_keys(
        IN server_name      TEXT,
        IN table_name       TEXT
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        table_schema TEXT;
      BEGIN
        SELECT "name", "schema"
        FROM common.get_table_schema_and_name(table_name)
        INTO table_name, table_schema;

        EXECUTE format($$
          DROP TRIGGER trg_clear_redis_cache_from_%1$s ON %2$I.%3$I;
        $$, server_name, table_schema, table_name);

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL
  end

  def down
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

    execute %Q[DROP FUNCTION IF EXISTS common.get_table_schema_and_name(TEXT, TEXT);]
  end
end



