DROP FUNCTION IF EXISTS transfer.create_meta_schema(bigint);
CREATE OR REPLACE FUNCTION transfer.create_meta_schema(
  company_id    bigint
) RETURNS text AS $BODY$
DECLARE
  meta_schema   text;
BEGIN

  EXECUTE
    FORMAT('SELECT * FROM transfer.get_meta_schema_name(%1$L)', company_id)
  INTO STRICT meta_schema;

  EXECUTE FORMAT('

    DROP SCHEMA IF EXISTS %1$s CASCADE;
    CREATE SCHEMA %1$s;

    CREATE UNLOGGED TABLE %1$s.info (
      company_id                integer,
      tax_registration_number   character varying(255),
      company_name              character varying(255),
      schema_version            character varying(255),
      backed_up_at              timestamp with time zone,
      backed_up_schemas         text[],
      main_schema               text,
      accounting_schemas        text[],
      fiscal_years              JSON DEFAULT NULL
    );
  ', meta_schema);

  RETURN meta_schema;

END;
$BODY$ LANGUAGE 'plpgsql';