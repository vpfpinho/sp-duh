DROP FUNCTION IF EXISTS sharding.wrap_with_duplicate_check(TEXT);

CREATE OR REPLACE FUNCTION sharding.wrap_with_duplicate_check(
  IN p_query TEXT
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
        END;
      $BLOCK$
    $RETURN$,
    p_query
  );
END;
$BODY$ LANGUAGE plpgsql;