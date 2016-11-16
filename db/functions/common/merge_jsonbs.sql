DROP FUNCTION IF EXISTS common.merge_jsonbs(VARIADIC JSONB[]);

CREATE OR REPLACE FUNCTION common.merge_jsonbs(
  VARIADIC p_jsons JSONB[],
  OUT merged_json JSONB
)
RETURNS JSONB AS $BODY$
DECLARE
  query TEXT;
  result JSONB;
BEGIN
  -- RAISE NOTICE 'sharding.merge_jsonb_with_arrays_of_keys_and_values(%, %, %)', p_jsonb, p_keys, p_values;

  SELECT string_agg(format('SELECT * FROM jsonb_each_text(%1$L)', j), ' UNION ALL ')
  FROM unnest(p_jsons) AS j
  INTO query;

  query := format($$
    SELECT json_object(array_agg(key), array_agg(value::TEXT)::TEXT[])::JSONB
    FROM (%1$s) x
  $$, query);

  EXECUTE query INTO merged_json;

  RETURN;
END;
$BODY$ LANGUAGE 'plpgsql';