-- DROP FUNCTION IF EXISTS sharding.wrap_with_duplicate_check(TEXT, TEXT, TEXT);

CREATE OR REPLACE FUNCTION sharding.wrap_with_duplicate_check(
  IN p_query        TEXT,
  IN p_table_name   TEXT,
  IN p_trigger_name TEXT
)
RETURNS TEXT AS $BODY$
DECLARE
BEGIN
  RETURN format(
    $RETURN$
      DO $BLOCK$
        BEGIN
            %1$s
        EXCEPTION WHEN duplicate_object THEN
            RAISE DEBUG 'Ignoring duplicate %2$s::%3$s';
        END;
      $BLOCK$
    $RETURN$,
    p_query, p_table_name, p_trigger_name
  );
END;
$BODY$ LANGUAGE plpgsql;

