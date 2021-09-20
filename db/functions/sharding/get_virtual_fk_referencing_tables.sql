DROP FUNCTION IF EXISTS sharding.get_virtual_fk_referencing_tables(text, text, integer, text);

CREATE OR REPLACE FUNCTION sharding.get_virtual_fk_referencing_tables (
    IN  referenced_schema    text,
    IN  referencing_table    text,
    IN  specific_company_id  integer DEFAULT NULL,
    IN  specific_schema_name text    DEFAULT NULL,
    OUT referencing_schema   text
) RETURNS SETOF text AS $BODY$
BEGIN

    -- RAISE DEBUG '(referenced_schema, referencing_table, specific_company_id, specific_schema_name) = (%,%,%,%)', referenced_schema, referencing_table, specific_company_id, specific_schema_name;


    IF specific_schema_name IS NOT NULL THEN
        -- RAISE DEBUG 'specific_schema_name = %', specific_schema_name;

        RETURN QUERY
        SELECT specific_schema_name;

    ELSIF specific_company_id IS NOT NULL THEN

        -- if table is supposed to be sharded, it *is* sharded, because all companies are sharded
        IF (SELECT EXISTS (SELECT 1 FROM sharding.get_sharded_tables() WHERE sharded_table_name = referencing_table)) THEN
            -- RAISE DEBUG 'COMPANIES - specific_company_id = %', specific_company_id;
            RETURN QUERY
            SELECT companies.schema_name::text
              FROM public.companies
             WHERE companies.id = specific_company_id;

        ELSE
            -- RAISE DEBUG 'CATALOG - specific_company_id = %', specific_company_id;
            RETURN QUERY
            SELECT pg_namespace.nspname::text
              FROM pg_catalog.pg_namespace 
              JOIN public.companies ON pg_namespace.nspname = companies.schema_name
              JOIN pg_catalog.pg_class     ON pg_class.relnamespace = pg_namespace.oid
             WHERE companies.id = specific_company_id
              AND pg_class.relname = referencing_table;

        END IF;
    ELSE
        -- RAISE DEBUG 'CATALOG - NO specific_company_id = %', specific_company_id;

        RETURN QUERY
        SELECT pg_namespace.nspname::text
          FROM pg_catalog.pg_class
          JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
          LEFT JOIN public.companies   ON companies.schema_name = pg_namespace.nspname
         WHERE pg_class.relkind = 'r' AND pg_class.relname = referencing_table
           AND ( companies.id IS NOT NULL OR pg_namespace.nspname IN ('accounting','fixedassets','payroll','purchases','public') )
           ;

    END IF;

END;
$BODY$ LANGUAGE 'plpgsql';
