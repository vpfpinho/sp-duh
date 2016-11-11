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