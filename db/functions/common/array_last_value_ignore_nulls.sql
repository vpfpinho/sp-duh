DROP FUNCTION IF EXISTS common.array_last_value_ignore_nulls;
CREATE OR REPLACE FUNCTION common.array_last_value_ignore_nulls(
    IN a_array  anyarray
)
RETURNS anyelement AS
$BODY$
DECLARE
    not_null $1%type;
    last_position integer;
BEGIN
    not_null := array_remove(a_array, NULL);
    last_position := cardinality(not_null);

    RETURN not_null[last_position];
END;
$BODY$
LANGUAGE plpgsql IMMUTABLE;
