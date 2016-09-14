DROP FUNCTION IF EXISTS sharding.recreate_accounting_views (INTEGER);

CREATE OR REPLACE FUNCTION sharding.recreate_accounting_views (
    IN company_id       INTEGER
)
RETURNS void AS $BODY$
DECLARE
    _company_id ALIAS FOR company_id;
    _schema_name    TEXT;
    _table_prefix   TEXT;
BEGIN
    RAISE DEBUG 'Recreate views for company_id: %', _company_id;

    -- only search open fiscal years on specified company
    FOR _schema_name IN EXECUTE 'SELECT schema_name FROM accounting.accounting_companies WHERE company_id = $1' USING _company_id LOOP
        FOR _table_prefix IN EXECUTE 'SELECT table_prefix FROM '||_schema_name||'.fiscal_years' LOOP
            RAISE DEBUG '  => %.%', _schema_name, _table_prefix;
            PERFORM accounting.drop_fiscal_year_views(_schema_name, _table_prefix);
            PERFORM accounting.create_fiscal_year_views(_schema_name, _table_prefix);
            PERFORM accounting.create_fiscal_year_views_triggers(_schema_name, _table_prefix);
        END LOOP;
    END LOOP;

    RETURN;
END;
$BODY$ LANGUAGE 'plpgsql' STABLE;
