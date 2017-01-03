class CreateCommonFunctionExistsStoredProcedure < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION common.function_exists (p_function_name TEXT)
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        query TEXT;
        result BOOLEAN;
        schema_name TEXT;
        function_name TEXT;
      BEGIN
        IF (SELECT EXISTS (SELECT 1 FROM regexp_matches(p_function_name, '^.+\..+$'))) THEN
          SELECT (regexp_matches(p_function_name, '^(.+?)\..+?'))[1] INTO schema_name;
          SELECT regexp_replace(p_function_name, schema_name || '.', '') INTO function_name;
        ELSE
          schema_name := NULL;
          function_name := p_function_name;
        END IF;

        query := format(
          $$
            SELECT EXISTS (
              SELECT 1
              FROM pg_catalog.pg_proc p
                %1$s
              WHERE p.proname = %3$L
                %2$s
            );
          $$,
          CASE WHEN schema_name IS NOT NULL THEN 'JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace' END,
          CASE WHEN schema_name IS NOT NULL THEN format('AND n.nspname = %1$L', schema_name) END,
          function_name
        );

        EXECUTE query INTO result;

        RETURN result;
      END;
      $BODY$ LANGUAGE 'plpgsql' STABLE;
    SQL
  end

  def down
    execute %Q[DROP FUNCTION IF EXISTS common.function_exists(TEXT);]
  end
end
