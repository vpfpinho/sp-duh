class FixSchemaNamesAndCompanyIdsInRestore < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.create_shard_triggers(TEXT, TEXT, TEXT, TEXT, JSONB);
      CREATE OR REPLACE FUNCTION transfer.create_shard_triggers(
        template_schema_name    TEXT,
        schema_name             TEXT,
        template_prefix         TEXT DEFAULT '',
        prefix                  TEXT DEFAULT '',
        all_objects_data        JSONB DEFAULT NULL
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        object_data             JSON;
        qualified_object_name   TEXT;
        object_name             TEXT;
        json_object             JSON;
        query                   TEXT;
      BEGIN

        IF all_objects_data IS NULL THEN
          -- Get the necessary data to create the new triggers
          SELECT
            json_object_agg(i.qualified_object_name,
              json_build_object(
                'triggers', i.triggers
              )
            )::JSONB INTO all_objects_data
          FROM sharding.get_tables_info(template_schema_name, template_prefix) i;
        END IF;

        ------------------------
        -- Build the triggers --
        ------------------------

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP

          object_name := regexp_replace(qualified_object_name, '^(?:' || template_schema_name || '\.' || template_prefix || ')?(.*)$', '\1');

          RAISE DEBUG '-- [TRIGGERS] TABLE: %', object_name;

          IF (object_data->>'triggers') IS NOT NULL THEN
            FOR json_object IN SELECT * FROM json_array_elements(object_data->'triggers') LOOP
              query := regexp_replace(
                json_object->>'definition',
                ' ON (?:' || template_schema_name || '\.' || template_prefix || ')?',
                format(' ON %1$s.%2$s', schema_name, prefix)
              );
              query := replace(
                query,
                template_schema_name || '.' || template_prefix,
                format('%1$s.%2$s', schema_name, prefix)
              );
              EXECUTE query;
            END LOOP;
          END IF;
        END LOOP;

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.create_shard_constraints(BIGINT, TEXT, TEXT, TEXT, TEXT, JSONB);
      CREATE OR REPLACE FUNCTION transfer.create_shard_constraints(
        company_id              BIGINT,
        template_schema_name    TEXT,
        schema_name             TEXT,
        template_prefix         TEXT DEFAULT '',
        prefix                  TEXT DEFAULT '',
        all_objects_data        JSONB DEFAULT NULL
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        template_company_id     integer;
        object_data             JSON;
        qualified_object_name   TEXT;
        object_name             TEXT;
        json_object             JSON;
        query                   TEXT;
        name                    TEXT;
        aux                     TEXT;
      BEGIN

        template_company_id := split_part(template_schema_name, '_c', 2)::integer;

        IF all_objects_data IS NULL THEN
          -- Get the necessary data to create the new constraints
          SELECT
            json_object_agg(i.qualified_object_name,
              json_build_object(
                'constraints', i.constraints
              )
            )::JSONB INTO all_objects_data
          FROM sharding.get_tables_info(template_schema_name, template_prefix) i;
        END IF;

        ---------------------------
        -- Build the constraints --
        ---------------------------

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP

          object_name := regexp_replace(qualified_object_name, '^(?:' || template_schema_name || '\.' || template_prefix || ')?(.*)$', '\1');

          RAISE DEBUG '-- [CONSTRAINTS] TABLE: %', object_name;

          IF (object_data->>'constraints') IS NOT NULL THEN

            FOR json_object IN SELECT * FROM json_array_elements(object_data->'constraints') LOOP

              aux := regexp_replace(json_object->>'definition', 'company_id\s*=\s*\d+', format('company_id = %1$s', company_id));

              name := json_object->>'name';
              name := replace(name, format('''%1$s''', template_company_id), format('''%1$s''', company_id));
              IF template_prefix <> '' THEN
                name := regexp_replace(name, template_prefix, prefix);
              END IF;

              FOREACH query IN ARRAY ARRAY[format('ALTER TABLE %1$s.%5$s%2$I ADD CONSTRAINT %3$I %4$s;',
                schema_name,
                object_name,
                name,
                aux,
                prefix
              )]
              LOOP
                -- RAISE DEBUG '%', query;
                EXECUTE query;
              END LOOP;
            END LOOP;
          END IF;
        END LOOP;

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
  end

  def down
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.create_shard_constraints(BIGINT, TEXT, TEXT, TEXT, TEXT, JSONB);
      CREATE OR REPLACE FUNCTION transfer.create_shard_constraints(
        company_id              BIGINT,
        template_schema_name    TEXT,
        schema_name             TEXT,
        template_prefix         TEXT DEFAULT '',
        prefix                  TEXT DEFAULT '',
        all_objects_data        JSONB DEFAULT NULL
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        object_data             JSON;
        qualified_object_name   TEXT;
        object_name             TEXT;
        json_object             JSON;
        query                   TEXT;
        name                    TEXT;
        aux                     TEXT;
      BEGIN

        IF all_objects_data IS NULL THEN
          -- Get the necessary data to create the new constraints
          SELECT
            json_object_agg(i.qualified_object_name,
              json_build_object(
                'constraints', i.constraints
              )
            )::JSONB INTO all_objects_data
          FROM sharding.get_tables_info(template_schema_name, template_prefix) i;
        END IF;

        ---------------------------
        -- Build the constraints --
        ---------------------------

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP

          object_name := regexp_replace(qualified_object_name, '^(?:' || template_schema_name || '\.' || template_prefix || ')?(.*)$', '\1');

          RAISE DEBUG '-- [CONSTRAINTS] TABLE: %', object_name;

          IF (object_data->>'constraints') IS NOT NULL THEN

            FOR json_object IN SELECT * FROM json_array_elements(object_data->'constraints') LOOP

              aux := regexp_replace(json_object->>'definition', 'company_id\s*=\s*\d+', format('company_id = %1$s', company_id));

              name := json_object->>'name';
              IF template_prefix <> '' THEN
                name := regexp_replace(name, template_prefix, prefix);
              END IF;

              FOREACH query IN ARRAY ARRAY[format('ALTER TABLE %1$s.%5$s%2$I ADD CONSTRAINT %3$I %4$s;',
                schema_name,
                object_name,
                name,
                aux,
                prefix
              )]
              LOOP
                -- RAISE DEBUG '%', query;
                EXECUTE query;
              END LOOP;
            END LOOP;
          END IF;
        END LOOP;

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.create_shard_triggers(TEXT, TEXT, TEXT, TEXT, JSONB);
      CREATE OR REPLACE FUNCTION transfer.create_shard_triggers(
        template_schema_name    TEXT,
        schema_name             TEXT,
        template_prefix         TEXT DEFAULT '',
        prefix                  TEXT DEFAULT '',
        all_objects_data        JSONB DEFAULT NULL
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        object_data             JSON;
        qualified_object_name   TEXT;
        object_name             TEXT;
        json_object             JSON;
        query                   TEXT;
      BEGIN

        IF all_objects_data IS NULL THEN
          -- Get the necessary data to create the new triggers
          SELECT
            json_object_agg(i.qualified_object_name,
              json_build_object(
                'triggers', i.triggers
              )
            )::JSONB INTO all_objects_data
          FROM sharding.get_tables_info(template_schema_name, template_prefix) i;
        END IF;

        ------------------------
        -- Build the triggers --
        ------------------------

        FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP

          object_name := regexp_replace(qualified_object_name, '^(?:' || template_schema_name || '\.' || template_prefix || ')?(.*)$', '\1');

          RAISE DEBUG '-- [TRIGGERS] TABLE: %', object_name;

          IF (object_data->>'triggers') IS NOT NULL THEN
            FOR json_object IN SELECT * FROM json_array_elements(object_data->'triggers') LOOP
              query := regexp_replace(
                json_object->>'definition',
                ' ON (?:' || template_schema_name || '\.' || template_prefix || ')?',
                format(' ON %1$s.%2$s', schema_name, prefix)
              );
              EXECUTE query;
            END LOOP;
          END IF;
        END LOOP;

        RETURN TRUE;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
  end
end
