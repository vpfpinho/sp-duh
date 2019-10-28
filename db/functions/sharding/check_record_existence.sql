-- DROP FUNCTION IF EXISTS sharding.check_record_existence(TEXT, JSONB);

CREATE OR REPLACE FUNCTION sharding.check_record_existence(
  IN p_table_name TEXT,
  IN p_columns_and_values JSONB
)
RETURNS BOOLEAN AS $BODY$
DECLARE
  query TEXT;
  record_exists BOOLEAN;
  clauses TEXT;
  clause_fields TEXT[];

  clause_format_expression TEXT;
  clause_columns_extract_expression TEXT;
  clause_columns_definition_expression TEXT;
BEGIN
  record_exists := FALSE;

  IF p_table_name IS NULL OR p_columns_and_values IS NULL THEN
    RAISE EXCEPTION 'Invalid arguments calling: %', format('sharding.check_record_existence(%L,%L)', p_table_name, p_columns_and_values);
  END IF;

  clause_fields := (SELECT array_agg(jsonb_object_keys) FROM jsonb_object_keys(p_columns_and_values));

  SELECT
    array_to_string(array_agg(format('%1$s TEXT', unnest)), ', ')
  INTO
    clause_columns_definition_expression
  FROM unnest(clause_fields);

  SELECT
    format('''(%1$s)'', %2$s', array_to_string(array_agg('%' || i || '$L'), ', '), array_to_string(array_agg(field), ', ')),
    array_to_string(
      array_agg(format(
        '(SELECT jsonb_array_elements_text(%1$s::JSONB) AS %1$s FROM jsonb_to_record($1) AS data(%2$s)) data_%1$s',
        field,
        clause_columns_definition_expression
      )),
      E'\nCROSS JOIN '
    )
  INTO
    clause_format_expression,
    clause_columns_extract_expression
  FROM unnest(clause_fields) WITH ORDINALITY AS fields(field, i);

  EXECUTE format($$SELECT
    array_to_string((
      SELECT array_agg(format(%1$s))
      FROM %2$s
    ),
    ', ')
  $$,
    clause_format_expression,
    clause_columns_extract_expression,
    clause_columns_definition_expression,
    p_columns_and_values
  ) INTO clauses USING p_columns_and_values;

  query := format('SELECT EXISTS (SELECT 1 FROM %1$s WHERE (%2$s) IN (%3$s))', p_table_name, array_to_string(clause_fields, ', '), clauses);
  -- RAISE DEBUG 'query: %', query;
  EXECUTE query INTO record_exists;

  RETURN record_exists;
END;
$BODY$ LANGUAGE 'plpgsql';
