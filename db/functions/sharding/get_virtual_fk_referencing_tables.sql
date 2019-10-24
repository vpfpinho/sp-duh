DROP FUNCTION IF EXISTS sharding.get_virtual_fk_referencing_tables(text, text, integer, text);

CREATE OR REPLACE FUNCTION sharding.get_virtual_fk_referencing_tables (
    IN  referenced_schema    text,
    IN  referencing_table    text,
    IN  specific_company_id  integer DEFAULT NULL,
    IN  specific_schema_name text DEFAULT NULL,
    OUT referencing_schema   text
) RETURNS SETOF text AS $BODY$
DECLARE
  current_cluster integer;
BEGIN

    SHOW cloudware.cluster INTO current_cluster;

    RETURN QUERY
    SELECT pg_namespace.nspname::text
      FROM pg_catalog.pg_class
      JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
      LEFT JOIN public.companies   ON companies.schema_name = pg_namespace.nspname
     WHERE pg_class.relkind = 'r' AND pg_class.relname = referencing_table
       AND pg_namespace.nspname <> referenced_schema
       AND ( companies.id IS NOT NULL OR pg_namespace.nspname IN ('accounting','fixedassets','payroll','purchases','public') )
       AND ( specific_company_id IS NULL OR companies.id = specific_company_id )
       AND ( specific_schema_name IS NULL OR pg_namespace.nspname = specific_schema_name )
       ;

END;
$BODY$ LANGUAGE 'plpgsql';
