DROP FUNCTION IF EXISTS sharding.get_option(TEXT);

CREATE OR REPLACE FUNCTION sharding.get_option(
  IN option_name TEXT
)
RETURNS BOOLEAN AS $BODY$
DECLARE
  _value BOOLEAN;
  _query TEXT;
BEGIN
  EXECUTE format($$SELECT %1$I FROM sharding.options LIMIT 1;$$, option_name) INTO _value;
  RETURN _value;
EXCEPTION WHEN undefined_column THEN
  RAISE EXCEPTION
    USING MESSAGE = format($$Sharding configuration option '%1$s' does not exist! Available options: %2$s.$$
      , option_name
      , (SELECT string_agg(a.attname, ', ') FROM  pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON a.attrelid = c.oid AND c.oid = 'sharding.options'::regclass::oid WHERE a.attnum > 0 AND NOT a.attisdropped)
    );
END;
$BODY$ LANGUAGE 'plpgsql' STABLE;
