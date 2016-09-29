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


SELECT redis.create_redis_server_connection('optimizations');
SELECT redis.create_trigger_to_delete_cache_keys('optimizations', 'companies', 'company-data:$id$:*');

-- UPDATE companies SET use_sharded_company = use_sharded_company WHERE id = 20289;

ROLLBACK;