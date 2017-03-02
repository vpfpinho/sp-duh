DROP FUNCTION IF EXISTS transfer.get_restore_templates(bigint);
CREATE OR REPLACE FUNCTION transfer.get_restore_templates(
  p_company_id                bigint
)
RETURNS TABLE (
  main_schema_template        text,
  accounting_schema_template  text,
  fiscal_year_template        text
) AS $BODY$
DECLARE
  query                       text;
  companies                   RECORD;
  main_schema_template        text;
  accounting_schema_template  text;
  fiscal_year_template        text;
BEGIN

  main_schema_template := NULL;
  accounting_schema_template := NULL;

  FOR companies IN
    SELECT
      c.schema_name::text AS main_schema_template,
      a.schema_name::text AS accounting_schema_template
    FROM
      public.companies c
    LEFT JOIN
      accounting.accounting_companies a ON a.company_id = c.id
    WHERE
      c.use_sharded_company = true AND
      c.id <> p_company_id
    ORDER BY
      c.id,
      a.id
    LIMIT 10
  LOOP

    main_schema_template := companies.main_schema_template;
    accounting_schema_template := companies.accounting_schema_template;

    IF accounting_schema_template IS NOT NULL THEN
      EXECUTE FORMAT('
        SELECT
          y.table_prefix::text AS fiscal_year_template
        FROM
          %1$s.fiscal_years y
        ORDER BY
          y.id
        LIMIT 1
      ', accounting_schema_template)
      INTO fiscal_year_template;

      EXIT WHEN fiscal_year_template IS NOT NULL;
    END IF;

  END LOOP;

  query := FORMAT('
    SELECT
      ''%1$s''::text AS main_schema_template,
      ''%2$s''::text AS accounting_schema_template,
      ''%3$s''::text AS fiscal_year_template
  ', main_schema_template, accounting_schema_template, fiscal_year_template);

  RETURN QUERY EXECUTE query;

END;
$BODY$ LANGUAGE 'plpgsql';