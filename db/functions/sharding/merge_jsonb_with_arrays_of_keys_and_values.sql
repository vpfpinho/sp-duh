-- DROP FUNCTION IF EXISTS sharding.merge_jsonb_with_arrays_of_keys_and_values(JSONB, TEXT[], TEXT[]);

CREATE OR REPLACE FUNCTION sharding.merge_jsonb_with_arrays_of_keys_and_values(
  IN p_jsonb JSONB,
  IN p_keys TEXT[],
  IN p_values TEXT[]
)
RETURNS JSONB AS $BODY$
DECLARE
  query TEXT;
  result JSONB;
BEGIN
  -- RAISE DEBUG 'sharding.merge_jsonb_with_arrays_of_keys_and_values(%, %, %)', p_jsonb, p_keys, p_values;

  query := $$SELECT format('{ %1$s }', array_to_string(part, ', '))
    FROM (
      SELECT array_agg(format('"%1$s": %2$s', "key", "value")) AS part
      FROM (
        SELECT * FROM jsonb_each_text($$ || CASE WHEN p_jsonb IS NULL THEN 'NULL' ELSE ''''||p_jsonb::TEXT||'''' END || $$)
        UNION SELECT * FROM jsonb_each_text('$$
        || (
          SELECT format('{ %s }', array_to_string((SELECT array_agg(format('"%1$s": ["%2$s"]', field, val)) FROM (
            SELECT * FROM unnest(p_keys, p_values)
          ) AS data(field, val)), ', ')))
        || $$')
      ) data
    ) x
  $$;
  -- RAISE DEBUG 'query: %', query;

  EXECUTE query INTO result;

  RETURN result;
END;
$BODY$ LANGUAGE 'plpgsql';
