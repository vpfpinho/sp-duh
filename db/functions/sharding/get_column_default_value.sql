-- DROP FUNCTION IF EXISTS sharding.get_column_default_value(TEXT, TEXT, TEXT);

CREATE OR REPLACE FUNCTION sharding.get_column_default_value(
  IN p_table_name TEXT,
  IN p_column_name TEXT,
  IN p_table_schema TEXT DEFAULT 'public'
)
RETURNS TEXT AS $BODY$
BEGIN
  RETURN (
    SELECT d.adsrc AS default_value
    FROM   pg_catalog.pg_attribute a
    LEFT   JOIN pg_catalog.pg_attrdef d ON (a.attrelid, a.attnum)
                                         = (d.adrelid,  d.adnum)
    WHERE  NOT a.attisdropped   -- no dropped (dead) columns
    AND    a.attnum > 0         -- no system columns
    AND    a.attrelid = (p_table_schema || '.' || p_table_name)::regclass
    AND    a.attname = p_column_name
  );
END;
$BODY$ LANGUAGE 'plpgsql';