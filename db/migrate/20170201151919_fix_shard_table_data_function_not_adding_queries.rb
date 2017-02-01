class FixShardTableDataFunctionNotAddingQueries < ActiveRecord::Migration
  def up
    execute <<-'SQL'
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
        _insert_queries TEXT[][];
        _delete_queries TEXT[][];
      BEGIN

        SELECT std.insert_queries::TEXT[][], std.delete_queries::TEXT[][] INTO _insert_queries, _delete_queries
        FROM sharding.shard_table_data(insert_queries::TEXT, delete_queries::TEXT, p_company_id, p_table, p_schema_name, NULL, p_generate_delete_data_queries) std;

        insert_queries := _insert_queries::TEXT;
        delete_queries := _delete_queries::TEXT;

        RETURN;
      END;
      $BODY$ LANGUAGE plpgsql;
      SQL
  end

  def down
    execute <<-'SQL'
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
    SQL
  end
end
