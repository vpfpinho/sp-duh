class OptimizeMergeJsonbStoredProcedure < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION common.merge_jsonbs(
        VARIADIC p_jsons JSONB[],
        OUT merged_json JSONB
      )
      RETURNS JSONB AS $BODY$
      DECLARE
        query TEXT;
        _separator TEXT;
        _json JSONB;
        _json_type CHAR;
      BEGIN
        -- RAISE NOTICE 'sharding.merge_jsonb_with_arrays_of_keys_and_values(%, %, %)', p_jsonb, p_keys, p_values;
        merged_json := regexp_replace(p_jsons[array_lower(p_jsons, 1)]::TEXT, '^(.).*?(.)$', '\1\2')::JSONB;
        _json_type := left(merged_json::TEXT, 1);

        FOREACH _json IN ARRAY p_jsons LOOP
          IF left(_json::TEXT, 1) != _json_type THEN
            RAISE EXCEPTION 'Can''t merge JSON arrays with JSON objects!';
          END IF;

          merged_json := format(
            '%1$s%3$s%2$s',
            substr(merged_json::TEXT, 1, length(merged_json::TEXT) - 1),
            substr(_json::TEXT, 2),
            _separator
          )::JSONB;
          _separator := ', ';
        END LOOP;

        RETURN;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
  end

  def down
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION common.merge_jsonbs(
        VARIADIC p_jsons JSONB[],
        OUT merged_json JSONB
      )
      RETURNS JSONB AS $BODY$
      DECLARE
        query TEXT;
        result JSONB;
      BEGIN
        -- RAISE NOTICE 'sharding.merge_jsonb_with_arrays_of_keys_and_values(%, %, %)', p_jsonb, p_keys, p_values;

        SELECT string_agg(format('SELECT * FROM jsonb_each_text(%1$L)', j), ' UNION ALL ')
        FROM unnest(p_jsons) AS j
        INTO query;

        query := format($$
          SELECT json_object(array_agg(key), array_agg(value::TEXT)::TEXT[])::JSONB
          FROM (%1$s) x
        $$, query);

        EXECUTE query INTO merged_json;

        RETURN;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
  end
end
