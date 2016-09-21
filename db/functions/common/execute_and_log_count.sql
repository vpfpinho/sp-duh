DROP FUNCTION IF EXISTS common.execute_and_log_count(TEXT, TEXT, TEXT);

CREATE OR REPLACE FUNCTION common.execute_and_log_count(
  IN query TEXT,
  IN message_template TEXT DEFAULT NULL,
  IN log_level TEXT DEFAULT 'NOTICE',
  OUT total_affected_records INTEGER
)
RETURNS INTEGER AS $BODY$
DECLARE
  message TEXT;
BEGIN
  EXECUTE query;
  GET DIAGNOSTICS total_affected_records = ROW_COUNT;

  IF message_template IS NULL THEN
    message_template := 'Affected rows: %';
  END IF;

  message_template := regexp_replace(regexp_replace(regexp_replace(message_template, '%%', '~~'), '%', '%s'), '~~', '%%');

  CASE upper(log_level)
    WHEN 'DEBUG'      THEN RAISE DEBUG '%', format(message_template, total_affected_records);
    WHEN 'LOG'        THEN RAISE LOG '%', format(message_template, total_affected_records);
    WHEN 'INFO'       THEN RAISE INFO '%', format(message_template, total_affected_records);
    WHEN 'NOTICE'     THEN RAISE NOTICE '%', format(message_template, total_affected_records);
    WHEN 'WARNING'    THEN RAISE WARNING '%', format(message_template, total_affected_records);
    WHEN 'EXCEPTION'  THEN RAISE EXCEPTION '%', format(message_template, total_affected_records);
  END CASE;

  RETURN;
END;
$BODY$ LANGUAGE plpgsql;