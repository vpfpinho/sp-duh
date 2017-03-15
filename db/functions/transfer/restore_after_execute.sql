DROP FUNCTION IF EXISTS transfer.restore_after_execute(bigint);
CREATE OR REPLACE FUNCTION transfer.restore_after_execute(
  company_id      bigint
) RETURNS VOID AS $BODY$
DECLARE
  meta_schema                 text;
  source_info                 RECORD;
  query                       text;
  schema                      text;
  main_schema_template        text;
  accounting_schema_template  text;
  fiscal_year_template        text;
  accounting_schema           text;
  prefixes                    JSON;
  prefix                      text;
  excluded_prefixes           text[];
  schema_templates            text[];
  schemas                     text[];
BEGIN

  -- Assert that the company was restored and is valid!
  PERFORM transfer.validate_company(company_id);

  EXECUTE
    FORMAT('SELECT * FROM transfer.get_meta_schema_name(%1$L)', company_id)
  INTO STRICT meta_schema;

  EXECUTE
    FORMAT('SELECT * FROM %1$s.info', meta_schema)
  INTO STRICT source_info;

  SELECT * FROM transfer.get_restore_templates(company_id)
  INTO main_schema_template, accounting_schema_template, fiscal_year_template;

  -----------------------------------------------------------------------------------------------
  -- Create the CONSTRAINTS, INDEXES, FOREIGN KEYS AND TRIGGERS for the tables being restored  --
  -----------------------------------------------------------------------------------------------

  -- MAIN company schema non-table objects
  RAISE NOTICE 'Creating non-table objects in schema %', source_info.main_schema;
  PERFORM transfer.create_shard_non_table_objects(company_id, main_schema_template, source_info.main_schema);

  -- ACCOUNTING companies schema non-table objects
  EXECUTE FORMAT('
    SELECT array_agg(table_prefix) from %1$s.fiscal_years
  ', accounting_schema_template)
  INTO STRICT excluded_prefixes;
  FOREACH accounting_schema IN ARRAY source_info.accounting_schemas LOOP
    RAISE NOTICE 'Creating (global) non-table objects in schema %', accounting_schema;
    PERFORM transfer.create_shard_non_table_objects(company_id, accounting_schema_template, accounting_schema, '', '', excluded_prefixes);
  END LOOP;

  -- Accounting companies FISCAL YEARS schema non-table objects
  FOR accounting_schema, prefixes IN SELECT * FROM json_each(source_info.fiscal_years) LOOP
    FOREACH prefix IN ARRAY ARRAY(SELECT trim(fy::text, '"') FROM json_array_elements(prefixes->'prefixes') fy) LOOP
      RAISE NOTICE 'Creating non-table objects in schema % with prefix %', accounting_schema, prefix;
      PERFORM transfer.create_shard_non_table_objects(company_id, accounting_schema_template, accounting_schema, fiscal_year_template, prefix);
    END LOOP;
  END LOOP;

  ------------------------
  -- Create the VIEWS   --
  ------------------------

  schema_templates := '{}';
  schema_templates := schema_templates || main_schema_template;
  schemas := '{}';
  schemas := schemas || source_info.main_schema;

  -- MAIN company schema views
  RAISE NOTICE 'Creating views in schema %', source_info.main_schema;
  PERFORM transfer.create_shard_views(schema_templates, schemas);

  -- ACCOUNTING companies schema views
  EXECUTE FORMAT('
    SELECT array_agg(table_prefix) from %1$s.fiscal_years
  ', accounting_schema_template)
  INTO STRICT excluded_prefixes;
  FOREACH accounting_schema IN ARRAY source_info.accounting_schemas LOOP
    schema_templates := '{}';
    schema_templates := schema_templates || accounting_schema_template;
    schema_templates := schema_templates || main_schema_template;
    schemas := '{}';
    schemas := schemas || accounting_schema;
    schemas := schemas || source_info.main_schema;
    RAISE NOTICE 'Creating (global) views in schema %', accounting_schema;
    PERFORM transfer.create_shard_views(schema_templates, schemas, '', '', excluded_prefixes);
  END LOOP;

  -- Accounting companies FISCAL YEARS schema views
  FOR accounting_schema, prefixes IN SELECT * FROM json_each(source_info.fiscal_years) LOOP
    schema_templates := '{}';
    schema_templates := schema_templates || accounting_schema_template;
    schema_templates := schema_templates || main_schema_template;
    schemas := '{}';
    schemas := schemas || accounting_schema;
    schemas := schemas || source_info.main_schema;
    FOREACH prefix IN ARRAY ARRAY(SELECT trim(fy::text, '"') FROM json_array_elements(prefixes->'prefixes') fy) LOOP
      RAISE NOTICE 'Creating views in schema % with prefix %', accounting_schema, prefix;
      PERFORM transfer.create_shard_views(schema_templates, schemas, fiscal_year_template, prefix);
    END LOOP;
  END LOOP;

END;
$BODY$ LANGUAGE 'plpgsql';