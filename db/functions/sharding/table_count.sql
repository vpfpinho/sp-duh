-- DROP FUNCTION IF EXISTS sharding.table_count (text);

CREATE OR REPLACE FUNCTION sharding.table_count (
  IN  p_table_name text,
  OUT table_count  integer
) RETURNS integer AS $BODY$
BEGIN
  EXECUTE ('SELECT count(1) FROM '||p_table_name) INTO table_count;
  RETURN;
END;
$BODY$ LANGUAGE 'plpgsql';