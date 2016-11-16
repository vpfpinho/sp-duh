DROP FUNCTION IF EXISTS common.merge_json_arrays(JSONB, VARIADIC JSONB[]);

CREATE OR REPLACE FUNCTION common.merge_json_arrays(
  IN p_json         JSONB,
  VARIADIC p_arrays JSONB[],
  OUT merged_json   JSONB
)
RETURNS JSONB AS $BODY$
DECLARE
  query TEXT;
BEGIN
  -- RAISE NOTICE 'common.merge_json_arrays(%, %, %)', p_jsonb, p_keys, p_values;
  IF p_json IS NULL OR p_json::TEXT = '' THEN
    p_json := '[]'::JSONB;
  END IF;

  EXECUTE format(
    $$
      SELECT array_to_json(array_agg(value))
      FROM (
        SELECT * FROM jsonb_array_elements(%1$L)
        UNION ALL
        SELECT * FROM jsonb_array_elements(array_to_json(%2$L::JSONB[])::JSONB)
      ) merged_json
    $$, p_json, p_arrays)
  INTO merged_json;

  RETURN;
END;
$BODY$ LANGUAGE 'plpgsql';