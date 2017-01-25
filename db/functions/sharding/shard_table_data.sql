DROP FUNCTION IF EXISTS sharding.shard_table_data(TEXT, TEXT, INTEGER, TEXT, TEXT, TEXT);
DROP FUNCTION IF EXISTS sharding.shard_table_data(TEXT, TEXT, INTEGER, TEXT, TEXT, TEXT, BOOLEAN);

CREATE OR REPLACE FUNCTION sharding.shard_table_data(
  IN OUT insert_queries             TEXT,
  IN OUT delete_queries             TEXT,
  IN p_company_id                   INTEGER,
  IN p_table                        TEXT,
  IN p_schema_name                  TEXT,
  IN p_generate_delete_data_queries BOOLEAN
)
RETURNS record AS $BODY$
DECLARE
  _i_queries TEXT;
  _d_queries TEXT;
BEGIN
  SELECT *
  FROM sharding.shard_table_data(insert_queries, delete_queries, p_company_id, p_table, p_schema_name, NULL, p_generate_delete_data_queries)
  INTO _i_queries, _d_queries;

  RETURN;
END;
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sharding.shard_table_data(
  IN OUT insert_queries             TEXT,
  IN OUT delete_queries             TEXT,
  IN p_company_id                   INTEGER,
  IN p_table                        TEXT,
  IN p_schema_name                  TEXT,
  IN p_where_clause                 TEXT DEFAULT NULL,
  IN p_generate_delete_data_queries BOOLEAN DEFAULT TRUE
)
RETURNS record AS $BODY$
DECLARE
  p_insert_queries TEXT[][];
  p_delete_queries TEXT[][];
  query TEXT;
BEGIN
  p_insert_queries := insert_queries::TEXT[][];
  p_delete_queries := delete_queries::TEXT[][];
  -- RAISE DEBUG 'sharding.shard_table_data(''%'', ''%'', %, ''%'', ''%'', ''%'');', cardinality(p_insert_queries), cardinality(p_delete_queries), p_company_id, p_table, p_schema_name, p_where_clause;
  IF p_where_clause IS NULL THEN
    p_where_clause := 'company_id = %3$L';
  END IF;

  query := regexp_replace(format(
    $${{ %2$I, "SELECT common.execute_and_log_count('INSERT INTO %1$I.%2$I (SELECT * FROM ONLY public.%2$I WHERE %4$s)', 'Inserted %% rows from table public.%2$s into %1$s.%2$s', 'NOTICE');" }}$$,
    p_schema_name,
    p_table,
    p_company_id,
    regexp_replace(format(p_where_clause, p_schema_name, p_table, p_company_id), '''', '''''', 'gn')
  ), '\s+', ' ', 'gn');

  p_insert_queries := format(
    '%1$s, %2$s',
    substr(p_insert_queries::TEXT, 1, length(p_insert_queries::TEXT) - 1),
    substr(query, 2)
  )::TEXT[][];

  -- Store the sharded records into a separate table
  IF sharding.table_exists(format('sharded.%1$I', p_table)) THEN
    query := regexp_replace(format('INSERT INTO sharded.%2$I (SELECT * FROM ONLY public.%2$I WHERE ' || p_where_clause || ') RETURNING -1', p_schema_name, p_table, p_company_id), '\s+', ' ', 'gn');
  ELSE
    query := regexp_replace(format('CREATE TABLE sharded.%2$I AS SELECT * FROM ONLY public.%2$I WHERE ' || p_where_clause, p_schema_name, p_table, p_company_id || ' RETURNING -1'), '\s+', ' ', 'gn');
  END IF;

  query := format(
    $${{ sharded.%1$I, "%2$s" }}$$,
    p_table,
    query
  );

  p_insert_queries := format(
    '%1$s, %2$s',
    substr(p_insert_queries::TEXT, 1, length(p_insert_queries::TEXT) - 1),
    substr(query, 2)
  )::TEXT[][];

  IF p_generate_delete_data_queries THEN
    -- And build the delete sharded records from the original table query (only, not from new inherited), to return from the function

    -- Execute the query outputting the affected record count
    query := format(
      $${{ %2$I, "SELECT common.execute_and_log_count('DELETE FROM ONLY public.%2$I WHERE %4$s', 'Deleted %% rows from table public.%2$s for company %3$s', 'NOTICE');" }}$$,
      p_schema_name,
      p_table,
      p_company_id,
      regexp_replace(format(p_where_clause, p_schema_name, p_table, p_company_id), '''', '''''', 'gn')
    );

    p_delete_queries := format(
      '%1$s, %2$s',
      substr(query::TEXT, 1, length(query::TEXT) - 1),
      substr(p_delete_queries::TEXT, 2)
    )::TEXT[][];
  END IF;

  insert_queries := p_insert_queries::TEXT;
  delete_queries := p_delete_queries::TEXT;

  RETURN;
-- EXCEPTION
--   WHEN OTHERS THEN
--     RAISE WARNING '%', SQLERRM;
--     RETURN NULL;
END;
$BODY$ LANGUAGE plpgsql;