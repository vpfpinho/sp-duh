class SplitTriggerFunctionsToDeleteCacheForPerformanceReasons < ActiveRecord::Migration
  def up
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

        RAISE NOTICE 'table: %.%', table_schema, table_name;

        EXECUTE format($$
          CREATE TRIGGER trg_mark_redis_cache_to_clear_from_%1$s
          BEFORE UPDATE OR DELETE ON %2$I.%3$I
          FOR EACH ROW
            EXECUTE PROCEDURE redis.trf_mark_optimizations_cache_entries_for_deletion('%1$s', '%4$s');
        $$, server_name, table_schema, table_name, key_pattern);

        EXECUTE format($$
          CREATE TRIGGER trg_clear_redis_cache_from_%1$s
          AFTER UPDATE OR DELETE ON %2$I.%3$I
          FOR EACH STATEMENT
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

        EXECUTE format($$
          DROP TRIGGER trg_mark_redis_cache_to_clear_from_%1$s ON %2$I.%3$I;
        $$, server_name, table_schema, table_name);

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION redis.trf_delete_affected_optimizations_cache_entries()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        _redis_server_name  TEXT;
        _key_pattern        TEXT;
        _setting_name       TEXT;
        _query              TEXT;
        _records_to_update  JSONB;
        _all_keys           TEXT[];
        _record_type        TEXT;
        _record             RECORD;
        _json_record        JSONB;
        _key_field_names    TEXT[];
        _key_field_name     TEXT;
        _record_key_pattern TEXT;
        _affected_keys      TEXT[];
        _affected_key       TEXT;
      BEGIN
        _setting_name = format('redis.records.%1$s', TG_TABLE_NAME);

        BEGIN
          SELECT COALESCE(current_setting(_setting_name), '')::JSONB INTO _records_to_update;
        EXCEPTION WHEN undefined_object OR invalid_text_representation THEN
          -- No setting found, nothing to do
          RETURN NULL;
        END;

        _redis_server_name := TG_ARGV[0];
        _key_pattern := TG_ARGV[1];

        -- Get the names to replace in the query
        SELECT array_agg(matches)
        FROM (
          SELECT unnest(matches) AS matches
          FROM regexp_matches(_key_pattern, '\$([a-zA-Z0-9_]+)\$', 'g') AS matches
        ) data
        INTO _key_field_names;

        IF jsonb_array_length(_records_to_update) > 1 THEN
          RAISE NOTICE 'Updated more than 1 record: %', jsonb_array_length(_records_to_update);
          -- We are working on more than one record, so fetch all keys only once, and match over the array
          EXECUTE format($$
            SELECT array_agg("key")
            FROM "redis"."%1$s_cache_keys"
          $$, _redis_server_name) INTO _all_keys;
        ELSE
          RAISE NOTICE 'Updated 1 record: %', jsonb_array_length(_records_to_update);
          -- We only have one record (the most common case), so fetch the affected keys directly
          _json_record := jsonb_array_elements(_records_to_update);
          _record_key_pattern := _key_pattern;

          RAISE NOTICE '_json_record: %', _json_record;
          RAISE NOTICE '_record_key_pattern: %', _record_key_pattern;

          FOREACH _key_field_name IN ARRAY _key_field_names LOOP
            _record_key_pattern := regexp_replace(_record_key_pattern, '\$' || _key_field_name || '\$', _json_record->>_key_field_name, 'g');
            RAISE NOTICE '_record_key_pattern: %', _record_key_pattern;
          END LOOP;

          _query := format($$
            SELECT array_agg("key")
            FROM "redis"."%1$s_cache_keys"
            WHERE "key" LIKE '%2$s*'
          $$, _redis_server_name, _record_key_pattern);

          RAISE NOTICE '_query: %', _query;

          EXECUTE _query INTO _all_keys;
        END IF;

        RAISE NOTICE '_all_keys: %', _all_keys;

        SELECT  format('rec (%1$s TEXT)', string_agg(match, ' TEXT, '))
        INTO _record_type
        FROM (
          SELECT unnest(regexp_matches) AS match
          FROM regexp_matches(_key_pattern, '\$([a-zA-Z0-9_]+)\$', 'g')
        ) data;

        -- Iterate over all updated records
        FOR _json_record IN SELECT * FROM jsonb_array_elements(_records_to_update) LOOP
          -- Build the _record_key_pattern by replacing the placeholders
          _record_key_pattern := _key_pattern;

          FOREACH _key_field_name IN ARRAY _key_field_names LOOP
            _record_key_pattern := regexp_replace(_record_key_pattern, '\$' || _key_field_name || '\$', _json_record->>_key_field_name, 'g');
          END LOOP;

          -- Delete each affected key
          FOREACH _affected_key IN ARRAY (SELECT common.get_items_matching_regexp(_all_keys, _record_key_pattern)) LOOP
            EXECUTE format('DELETE FROM "redis"."%1$s_cache_entries" WHERE "key" = %2$L;', _redis_server_name, _affected_key);
          END LOOP;
        END LOOP;

        EXECUTE format('SET LOCAL %1$s = '''';', _setting_name);

        RETURN NULL;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION redis.trf_mark_optimizations_cache_entries_for_deletion()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        _triggering_record   RECORD;
        _current_data JSONB;
        _setting_name TEXT;
        _query TEXT;
        _key_pattern TEXT;
        _aux TEXT;
        _key_field_names    TEXT[];
        _key_field_values    TEXT[];

        _key_data JSONB;

      BEGIN
        IF TG_OP = 'UPDATE' AND OLD = NEW THEN
          -- Record hasn't changed, no need to do anything
          RETURN NEW;
        END IF ;

        IF TG_OP = 'DELETE' THEN
          _triggering_record := OLD;
        ELSE
          _triggering_record := NEW;
        END IF;

        RAISE NOTICE 'trf_mark_optimizations_cache_entries_for_deletion: %', _triggering_record.id;

        _key_pattern := TG_ARGV[1];


        SELECT format('$1.%1$s', string_agg(match, ', $1.')), array_agg(match)
        INTO _aux, _key_field_names
        FROM (
          SELECT unnest(regexp_matches) AS match
          FROM regexp_matches(_key_pattern, '\$([a-zA-Z0-9_]+)\$', 'g')
        ) data;

        RAISE DEBUG '_aux: %', _aux;
        RAISE DEBUG '_key_field_names: %', _key_field_names;

        _key_pattern := regexp_replace(_key_pattern, '\$([a-zA-Z0-9_]+)\$', '%s', 'g');

        RAISE DEBUG '_key_pattern: %', _key_pattern;

        EXECUTE format('SELECT ARRAY[%1$s]::TEXT[]', _aux)
        USING _triggering_record
        INTO _key_field_values;

        RAISE DEBUG '_key_field_values: %', _key_field_values;

        SELECT json_object(_key_field_names, _key_field_values)
        INTO _key_data;

        _setting_name = format('redis.records.%1$s', TG_TABLE_NAME);

        BEGIN
          SELECT COALESCE(current_setting(_setting_name), '')::JSONB INTO _current_data;
        EXCEPTION WHEN undefined_object OR invalid_text_representation THEN
          -- Catch exception in case the setting doesn't exist (first row, for example)
        END;

        _query := format('SET LOCAL %1$s = %2$L;', _setting_name, common.merge_json_arrays(_current_data, _key_data)::TEXT);

        EXECUTE _query;

        RETURN _triggering_record;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL
  end

  def down
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

    execute %Q[DROP FUNCTION IF EXISTS redis.trf_mark_optimizations_cache_entries_for_deletion()]
  end
end
