DROP FUNCTION IF EXISTS sharding.shard_table_data(TEXT[], TEXT[], INTEGER, TEXT, TEXT, TEXT);

CREATE OR REPLACE FUNCTION sharding.shard_table_data(
  IN OUT insert_queries TEXT[],
  IN OUT delete_queries TEXT[],
  IN p_company_id INTEGER,
  IN p_table TEXT,
  IN p_schema_name TEXT,
  IN p_where_clause TEXT DEFAULT NULL
)
RETURNS record AS $BODY$
DECLARE
  p_insert_queries ALIAS FOR insert_queries;
  p_delete_queries ALIAS FOR delete_queries;
  query TEXT;
BEGIN
  -- RAISE DEBUG 'sharding.shard_table_data(''%'', ''%'', %, ''%'', ''%'', ''%'');', cardinality(p_insert_queries), cardinality(p_delete_queries), p_company_id, p_table, p_schema_name, p_where_clause;
  IF p_where_clause IS NULL THEN
    p_where_clause := 'company_id = %3$L';
  END IF;

  RAISE NOTICE '% || %', p_where_clause, regexp_replace(format(p_where_clause, p_schema_name, p_table, p_company_id), '''', '''''', 'gn');

  p_insert_queries := p_insert_queries || regexp_replace(format('
    SELECT common.execute_and_log_count(
      ''INSERT INTO %1$I.%2$I (SELECT * FROM ONLY public.%2$I WHERE %4$s)'',
      ''Inserted %% rows from table public.%2$s into %1$s.%2$s'',
      ''NOTICE''
    );',
    p_schema_name,
    p_table,
    p_company_id,
    regexp_replace(format(p_where_clause, p_schema_name, p_table, p_company_id), '''', '''''', 'gn')
  ), '\s+', ' ', 'gn');

  -- Store the sharded records into a separate table
  IF sharding.table_exists(format('sharded.%1$I', p_table)) THEN
    query := regexp_replace(format('INSERT INTO sharded.%2$I (SELECT * FROM ONLY public.%2$I WHERE ' || p_where_clause || ')', p_schema_name, p_table, p_company_id), '\s+', ' ', 'gn');
  ELSE
    query := regexp_replace(format('CREATE TABLE sharded.%2$I AS SELECT * FROM ONLY public.%2$I WHERE ' || p_where_clause, p_schema_name, p_table, p_company_id), '\s+', ' ', 'gn');
  END IF;

  p_insert_queries := p_insert_queries || query;

  -- And build the delete sharded records from the original table query (only, not from new inherited), to return from the function
  p_delete_queries := array_prepend(regexp_replace(format('
    SELECT common.execute_and_log_count(
      ''DELETE FROM ONLY public.%2$I WHERE %4$s'',
      ''Deleted %% rows from table public.%2$s for company %3$s'',
      ''NOTICE''
    );',
    p_schema_name,
    p_table,
    p_company_id,
    regexp_replace(format(p_where_clause, p_schema_name, p_table, p_company_id), '''', '''''', 'gn')
  ), '\s+', ' ', 'gn'), p_delete_queries);

  RETURN;
-- EXCEPTION
--   WHEN OTHERS THEN
--     RAISE WARNING '%', SQLERRM;
--     RETURN NULL;
END;
$BODY$ LANGUAGE plpgsql;