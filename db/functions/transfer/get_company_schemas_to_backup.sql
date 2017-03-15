DROP FUNCTION IF EXISTS transfer.get_company_schemas_to_backup(bigint, boolean);
CREATE OR REPLACE FUNCTION transfer.get_company_schemas_to_backup(
  company_id                bigint,
  transfer_user_templates   boolean DEFAULT false
) RETURNS TABLE (
  schema_name   text,
  schema_type   text
) AS $BODY$
DECLARE
  query         text;
BEGIN

  query := FORMAT('
    WITH accounting_schemas AS (
      SELECT ac.schema_name
        FROM accounting.accounting_companies ac
        WHERE ac.company_id = %1$L
    )
  ', company_id);

  IF transfer_user_templates THEN
    query := query || FORMAT('
      ,
      user_schemas AS (
        SELECT ut.schema_name
          FROM accounting.user_templates ut
          WHERE ut.user_id IN (
            SELECT user_id FROM transfer.get_company_users(%1$L)
          )
      )
    ', company_id);
  END IF;

  query := query || FORMAT('
    SELECT c.schema_name::text, ''main''::text AS schema_type
      FROM companies c
      WHERE c.id = %1$L AND COALESCE(c.schema_name, '''') <> ''''
    UNION
    SELECT c.schema_name::text, ''accounting''::text AS schema_type
      FROM accounting_schemas c
    UNION
    SELECT (SELECT * FROM transfer.get_meta_schema_name(%1$L)) AS schema_name, ''meta''::text AS schema_type
  ', company_id);

  IF transfer_user_templates THEN
    query := query || '
      UNION
      SELECT c.schema_name::text, ''user''::text AS schema_type
        FROM user_schemas c
    ';
  END IF;

  RETURN QUERY EXECUTE query;

END;
$BODY$ LANGUAGE 'plpgsql';