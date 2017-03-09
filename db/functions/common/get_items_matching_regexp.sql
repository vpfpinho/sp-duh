-- DROP FUNCTION IF EXISTS common.get_items_matching_regexp(TEXT[], TEXT);

CREATE OR REPLACE FUNCTION common.get_items_matching_regexp(
  IN p_items TEXT[],
  IN p_regexp TEXT,
  OUT matching_items TEXT[]
)
RETURNS TEXT[] AS $BODY$
DECLARE
  query TEXT;
BEGIN
  SELECT array_agg(items)
  FROM unnest(p_items) AS items
  WHERE items ~* p_regexp
  INTO matching_items;

  IF matching_items IS NULL THEN
    matching_items := '{}'::TEXT[];
  END IF;

  RETURN;
END;
$BODY$ LANGUAGE 'plpgsql';