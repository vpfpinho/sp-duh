
class CreatePublicLinkFunction < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS pg_cpp_utils_public_link(text, float8, text, float8, text, text);
      DROP TYPE IF EXISTS pg_cpp_utils_public_link_record;
      CREATE TYPE pg_cpp_utils_public_link_record AS (url text);
      CREATE OR REPLACE FUNCTION pg_cpp_utils_public_link (
        a_base_url text,
        a_company_id float8,
        a_entity_type text,
        a_entity_id float8,
        a_key text,
        a_iv text
      ) RETURNS pg_cpp_utils_public_link_record AS '$libdir/pg-cpp-utils.so', 'pg_cpp_utils_public_link' LANGUAGE C STRICT;
    SQL
  end

  def down
    execute <<-'SQL'
      DROP FUNCTION pg_cpp_utils_public_link(text, float8, text, float8, text, text);
      DROP TYPE pg_cpp_utils_public_link_record;
    SQL
  end
end
