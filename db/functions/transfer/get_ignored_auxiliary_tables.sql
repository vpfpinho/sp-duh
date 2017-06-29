DROP FUNCTION IF EXISTS transfer.get_ignored_auxiliary_tables();
CREATE OR REPLACE FUNCTION transfer.get_ignored_auxiliary_tables(
  OUT table_name          text[]
) AS $BODY$
DECLARE
  query                   text;
BEGIN

  table_name := '{
    "audits",
    "company_database_locks",
    "impersonated_logins",
    "user_message_statuses"
  }'
  RETURN;

END;
$BODY$ LANGUAGE 'plpgsql';