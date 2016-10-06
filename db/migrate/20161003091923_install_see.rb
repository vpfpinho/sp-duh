class InstallSee < ActiveRecord::Migration
  def up
    execute <<-'SQL'

      CREATE TYPE see_record AS (json text, status text);

      CREATE OR REPLACE FUNCTION see (
        a_module          text,
        a_version         text,
        a_query_map       text,
        a_calc_parameters text,
        a_log             text default null,
        a_debug           boolean default false
      ) RETURNS see_record AS '$libdir/pg-see.so', 'see' LANGUAGE C STRICT;

      CREATE OR REPLACE FUNCTION see_payroll (
        a_module          text,
        a_version         text,
        a_query_map       text,
        a_calc_parameters text,
        a_clones          text,
        a_log             text default null,
        a_debug           boolean default false
      ) RETURNS see_record AS '$libdir/pg-see.so', 'see_payroll' LANGUAGE C STRICT;

      CREATE OR REPLACE FUNCTION see_evaluate_expression (
        a_expression text
      ) RETURNS see_record AS '$libdir/pg-see.so', 'see_evaluate_expression' LANGUAGE C STRICT;

      CREATE TABLE pg_see_json_table (
        namespace  character varying(255),
        table_name character varying(255),
        version    character varying(20),
        is_model   boolean NOT NULL DEFAULT FALSE,
        json       text,
        CONSTRAINT pg_see_json_table_pkey PRIMARY KEY(namespace, table_name, version)
      );

    SQL
  end

  def down
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS see (
        a_module          text,
        a_version         text,
        a_query_map       text,
        a_calc_parameters text
      );

      DROP FUNCTION IF EXISTS see (
        a_module          text,
        a_version         text,
        a_query_map       text,
        a_calc_parameters text,
        a_log             text,
        a_debug           boolean
      );

      DROP FUNCTION IF EXISTS see_payroll (
        a_module          text,
        a_version         text,
        a_query_map       text,
        a_calc_parameters text,
        a_clones          text,
        a_log             text,
        a_debug           boolean
      );

      DROP FUNCTION IF EXISTS see_evaluate_expression (
        a_expression text
      );
      DROP TYPE IF EXISTS see_record;
      DROP TABLE IF EXISTS pg_see_json_table;
    SQL
  end
end
