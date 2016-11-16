class CreateSomeHelperStoredProceduresOnCommonSchema < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION common.get_items_matching_regexp(
        IN p_items TEXT[],
        IN p_regexp TEXT,
        OUT matching_items TEXT[]
      )
      RETURNS TEXT[] AS $BODY$
      DECLARE
        query TEXT;
      BEGIN
        SELECT array_agg(items)
        FROM unnest(p_items) AS items
        WHERE items ~* p_regexp
        INTO matching_items;

        IF matching_items IS NULL THEN
          matching_items := '{}'::TEXT[];
        END IF;

        RETURN;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION common.merge_json_arrays(
        IN p_json         JSONB,
        VARIADIC p_arrays JSONB[],
        OUT merged_json   JSONB
      )
      RETURNS JSONB AS $BODY$
      DECLARE
        query TEXT;
      BEGIN
        -- RAISE NOTICE 'common.merge_json_arrays(%, %, %)', p_jsonb, p_keys, p_values;
        IF p_json IS NULL OR p_json::TEXT = '' THEN
          p_json := '[]'::JSONB;
        END IF;

        EXECUTE format(
          $$
            SELECT array_to_json(array_agg(value))
            FROM (
              SELECT * FROM jsonb_array_elements(%1$L)
              UNION ALL
              SELECT * FROM jsonb_array_elements(array_to_json(%2$L::JSONB[])::JSONB)
            ) merged_json
          $$, p_json, p_arrays)
        INTO merged_json;

        RETURN;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

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

  def down
    execute %Q[DROP FUNCTION IF EXISTS common.get_items_matching_regexp(TEXT[], TEXT);]
    execute %Q[DROP FUNCTION IF EXISTS common.merge_json_arrays(JSONB, VARIADIC JSONB[]);]
    execute %Q[DROP FUNCTION IF EXISTS common.merge_jsonbs(VARIADIC JSONB[]);]
  end
end
