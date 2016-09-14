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

  p_insert_queries := p_insert_queries || regexp_replace(format('INSERT INTO %1$I.%2$I (SELECT * FROM public.%2$I WHERE ' || p_where_clause || ')', p_schema_name, p_table, p_company_id), '\s+', ' ', 'gn');

  -- Store the sharded records into a separate table
  IF sharding.table_exists(format('sharded.%1$I', p_table)) THEN
    query := regexp_replace(format('INSERT INTO sharded.%2$I (SELECT * FROM public.%2$I WHERE ' || p_where_clause || ')', p_schema_name, p_table, p_company_id), '\s+', ' ', 'gn');
  ELSE
    query := regexp_replace(format('CREATE TABLE sharded.%2$I AS SELECT * FROM public.%2$I WHERE ' || p_where_clause, p_schema_name, p_table, p_company_id), '\s+', ' ', 'gn');
  END IF;

  p_insert_queries := p_insert_queries || query;

  -- And build the delete sharded records from the original table query (only, not from new inherited), to return from the function
  p_delete_queries := array_prepend(regexp_replace(format('DELETE FROM ONLY public.%2$I WHERE ' || p_where_clause, p_schema_name, p_table, p_company_id), '\s+', ' ', 'gn'),p_delete_queries);

  RETURN;
-- EXCEPTION
--   WHEN OTHERS THEN
--     RAISE WARNING '%', SQLERRM;
--     RETURN NULL;
END;
$BODY$ LANGUAGE plpgsql;