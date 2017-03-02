DROP FUNCTION IF EXISTS transfer._prepare_company_to_backup(bigint);
CREATE OR REPLACE FUNCTION transfer._prepare_company_to_backup(
  company_id      bigint
) RETURNS VOID AS $BODY$
DECLARE
  query           text;
  schema          text;
BEGIN

  -- Assert that the company exists and can be backed up!
  PERFORM transfer.validate_company(company_id);

  SELECT schema_name FROM public.companies WHERE id = company_id
  INTO schema;

  -- Convert all public sequences to private sequences
  IF NOT EXISTS(SELECT 1 FROM to_regclass((schema || '.customers_id_seq')::cstring) s WHERE s IS NOT NULL) THEN
    RAISE NOTICE 'Converting public sequences to private (shard) sequences for company schema %', schema;
    PERFORM sharding.convert_sequences_to_schema_qualified(schema);
  ELSE
    RAISE NOTICE '[NOT DOING ANYTHING] Public sequences were already converted to private (shard) sequences for company schema %', schema;
  END IF;

  -- Drop tables that were sharded but are no longer

  IF EXISTS(SELECT 1 FROM to_regclass((schema || '.company_certificates')::cstring) s WHERE s IS NOT NULL) THEN
    RAISE NOTICE 'Dropping no longer sharded table ''%'' in company schema %', 'company_certificates', schema;
    EXECUTE 'DROP TABLE ' || schema || '.company_certificates';
  ELSE
    RAISE NOTICE '[NOT DOING ANYTHING] No longer sharded table ''%'' was already dropped from company schema %', 'company_certificates', schema;
  END IF;
  IF EXISTS(SELECT 1 FROM to_regclass((schema || '.user_message_statuses')::cstring) s WHERE s IS NOT NULL) THEN
    RAISE NOTICE 'Dropping no longer sharded table ''%'' in company schema %', 'user_message_statuses', schema;
    EXECUTE 'DROP TABLE ' || schema || '.user_message_statuses';
  ELSE
    RAISE NOTICE '[NOT DOING ANYTHING] No longer sharded table ''%'' was already dropped from company schema %', 'user_message_statuses', schema;
  END IF;

END;
$BODY$ LANGUAGE 'plpgsql';