-- DROP FUNCTION IF EXISTS common.get_table_schema_and_name(TEXT, TEXT);

CREATE OR REPLACE FUNCTION common.get_table_schema_and_name(
  IN  full_table_name      TEXT,
  IN  default_schema_name  TEXT DEFAULT NULL,
  OUT "name" TEXT,
  OUT "schema" TEXT
)
RETURNS record AS $BODY$
BEGIN
  IF default_schema_name IS NULL THEN
    default_schema_name := 'public';
  END IF;

  "schema" := COALESCE(NULLIF(regexp_replace(full_table_name, '^(?:(.*?)\.)?(.*?)$', '\1'), ''), default_schema_name);
  "name" := regexp_replace(full_table_name, '^(?:.*?\.)?(.*?)$', '\1');

  RETURN;
END;
$BODY$ LANGUAGE plpgsql;
