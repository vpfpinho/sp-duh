DROP FUNCTION IF EXISTS transfer.get_restore_templates(bigint, bigint);
CREATE OR REPLACE FUNCTION transfer.get_restore_templates(
  IN  p_company_id                bigint,
  IN  p_template_company_id       bigint,
  OUT main_schema_template        text,
  OUT accounting_schema_template  text,
  OUT fiscal_year_template        text
)
RETURNS record AS $BODY$
BEGIN

  EXECUTE '
    SELECT
      c.schema_name::text,
      a.schema_name::text
    FROM
      public.companies c
    LEFT JOIN
      accounting.accounting_companies a ON a.company_id = c.id
    WHERE
      c.use_sharded_company = true AND
      c.id <> $1 AND
      c.id = $2
  '
  USING p_company_id, p_template_company_id
  INTO main_schema_template, accounting_schema_template;

  fiscal_year_template := NULL;
  IF accounting_schema_template IS NOT NULL THEN
    EXECUTE FORMAT('
      SELECT
        y.table_prefix::text
      FROM
        %1$s.fiscal_years y
      ORDER BY
        y.id
      LIMIT 1
    ', accounting_schema_template)
    INTO fiscal_year_template;
  END IF;

  RETURN;

END;
$BODY$ LANGUAGE 'plpgsql';