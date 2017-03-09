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
          IF NOT '%2$s::%3$s' = ANY(%4$s::TEXT[]) THEN
            %1$s
          END IF;
        END;
      $BLOCK$
    $RETURN$,
    p_query, p_table_name, p_trigger_name, '%3$L'
  );
END;
$BODY$ LANGUAGE plpgsql;