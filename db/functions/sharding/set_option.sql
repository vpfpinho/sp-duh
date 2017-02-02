DROP FUNCTION IF EXISTS sharding.set_option(TEXT, BOOLEAN);

CREATE OR REPLACE FUNCTION sharding.set_option(
  IN option_name TEXT,
  IN option_value BOOLEAN
)
RETURNS BOOLEAN AS $BODY$
DECLARE
  _value BOOLEAN;
  _query TEXT;
BEGIN
  EXECUTE format($$UPDATE sharding.options SET %1$I = %2$L RETURNING %1$I;$$, option_name, option_value) INTO _value;
  RETURN _value;
EXCEPTION WHEN undefined_column THEN
  RAISE EXCEPTION
    USING MESSAGE = format($$Sharding configuration option '%1$s' does not exist! Available options: %2$s.$$
      , option_name
      , (SELECT string_agg(a.attname, ', ') FROM  pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON a.attrelid = c.oid AND c.oid = 'sharding.options'::regclass::oid WHERE a.attnum > 0 AND NOT a.attisdropped)
    );
END;
$BODY$ LANGUAGE 'plpgsql';