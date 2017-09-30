class InstallPgCppUtilsFormatMessage < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE TYPE public.pg_cpp_utils_format_message_record AS (formatted text);
      CREATE OR REPLACE FUNCTION public.pg_cpp_utils_format_message(
        a_locale  varchar(5),
        a_format  varchar(5),
        VARIADIC a_args text[]
      ) RETURNS public.pg_cpp_utils_format_message_record AS '$libdir/pg-cpp-utils.so', 'pg_cpp_utils_format_message' LANGUAGE C STRICT;
    SQL
  end

  def down
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS public.pg_cpp_utils_format_message(character varying, character varying, text[]);
      DROP TYPE IF EXISTS public.pg_cpp_utils_format_message_record;
    SQL
  end
end
