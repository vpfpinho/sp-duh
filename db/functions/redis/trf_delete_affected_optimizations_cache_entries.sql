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