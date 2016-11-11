DROP FUNCTION IF EXISTS common.enable_triggers_on_table(TEXT, VARIADIC TEXT[]);

CREATE OR REPLACE FUNCTION common.enable_triggers_on_table(
  IN        table_name       TEXT,
  VARIADIC  trigger_names    TEXT[]
)
RETURNS BOOLEAN AS $BODY$
DECLARE
  table_schema TEXT;
  trigger_name TEXT;
BEGIN

  SELECT "name", "schema"
  FROM common.get_table_schema_and_name(table_name)
  INTO table_name, table_schema;

  FOREACH trigger_name IN ARRAY trigger_names LOOP
    EXECUTE format($$
      ALTER TABLE %1$I.%2$I
      ENABLE TRIGGER %3$I;
    $$, table_schema, table_name, trigger_name);
  END LOOP;

  RETURN TRUE;
END;
$BODY$ LANGUAGE plpgsql;
