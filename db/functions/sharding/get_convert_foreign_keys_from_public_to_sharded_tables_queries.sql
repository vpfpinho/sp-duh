-- DROP FUNCTION IF EXISTS sharding.get_convert_foreign_keys_from_public_to_sharded_tables_queries();
CREATE OR REPLACE FUNCTION sharding.get_convert_foreign_keys_from_public_to_sharded_tables_queries(
  OUT queries TEXT[]
)
RETURNS TEXT[] AS $BODY$
DECLARE
  aux TEXT;
  all_objects_data JSONB;
  qualified_object_name TEXT;
  object_data JSONB;
  foreign_key JSONB;
  referenced_table TEXT;
  aux_array TEXT[];
  update_action "char";
  delete_action "char";
BEGIN
  SELECT
    json_object_agg(fk.qualified_object_name,
      fk.foreign_keys
    )::JSONB INTO all_objects_data
  FROM (
    SELECT
      format('%1$I.%2$I', t.schemaname, t.tablename) AS qualified_object_name,
      (t.schemaname || '.' || t.tablename)::regclass::oid AS table_oid,
      json_agg(json_build_object(
        'name', c.conname,
        'update_action', c.confupdtype,
        'delete_action', c.confdeltype,
        'definition', pg_catalog.pg_get_constraintdef(c.oid, true)
      )::JSONB) AS foreign_keys
    FROM pg_catalog.pg_constraint c
      LEFT JOIN pg_catalog.pg_tables t ON c.conrelid = (t.schemaname || '.' || t.tablename)::regclass::oid
    WHERE c.contype = 'f'
      AND t.schemaname = 'public'
      AND t.tablename IN (
          SELECT jsonb_array_elements_text FROM jsonb_array_elements_text(sharding.get_auxiliary_table_information()->'unsharded_tables')
      )
    GROUP BY t.schemaname, t.tablename
  ) fk;

  FOR qualified_object_name, object_data IN SELECT * FROM jsonb_each(all_objects_data) LOOP
    -- RAISE NOTICE '%: %', qualified_object_name, object_data;

    FOR foreign_key IN SELECT * FROM jsonb_array_elements(object_data) LOOP
      aux_array := regexp_matches(foreign_key->>'definition', 'FOREIGN KEY \((.*?)\) REFERENCES (?:.*?\.)?(.*?)\((.*?)\)');

      IF NOT sharding.get_auxiliary_table_information()->'unsharded_tables' ? aux_array[2] THEN
        update_action := foreign_key->>'update_action'::"char";
        delete_action := foreign_key->>'delete_action'::"char";

        queries := queries || sharding.get_create_virtual_foreign_key_queries(
          qualified_object_name,
          ARRAY[aux_array[1]]::TEXT[],
          aux_array[2],
          ARRAY[aux_array[3]]::TEXT[],
          foreign_key->>'name',
          update_action,
          delete_action
        );


        queries := queries || format('ALTER TABLE %1$s DROP CONSTRAINT %2$I;', qualified_object_name, foreign_key->>'name');
        queries := queries || format('ALTER TABLE %1$s ADD CONSTRAINT %2$I %3$s;', qualified_object_name, foreign_key->>'name', foreign_key->>'definition');
      END IF;
    END LOOP;


  END LOOP;

  RETURN;
END;
$BODY$ LANGUAGE plpgsql;