class FixCreateVirtualForeignKeyFunction < ActiveRecord::Migration
  def up
    execute %Q[DROP FUNCTION IF EXISTS sharding.create_virtual_foreign_key(TEXT, TEXT[], TEXT, TEXT[], TEXT, TEXT, TEXT, BOOLEAN);]

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.create_virtual_foreign_key(
        IN p_referencing_table TEXT,
        IN p_referencing_columns TEXT[],
        IN p_referenced_table TEXT,
        IN p_referenced_columns TEXT[],
        IN p_template_fk_name TEXT DEFAULT NULL,
        IN p_update_condition "char" DEFAULT NULL,
        IN p_delete_condition "char" DEFAULT NULL
      )
      RETURNS VOID AS $BODY$
      DECLARE
        query TEXT;
      BEGIN

        FOR query IN SELECT unnest(sharding.get_create_virtual_foreign_key_queries(
          p_referencing_table,
          p_referencing_columns,
          p_referenced_table,
          p_referenced_columns,
          p_template_fk_name,
          p_update_condition,
          p_delete_condition
        )) LOOP
          EXECUTE query;
        END LOOP;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL
  end

  def down
    execute %Q[DROP FUNCTION IF EXISTS sharding.create_virtual_foreign_key(TEXT, TEXT[], TEXT, TEXT[], TEXT, TEXT, TEXT);]

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.create_virtual_foreign_key(
        IN p_referencing_table TEXT, -- p_destination_schema_name.p_template_table_name
        IN p_referencing_columns TEXT[], -- aux_array[1]
        IN p_referenced_table TEXT, -- aux_array[2]
        IN p_referenced_columns TEXT[], -- aux_array[3]
        IN p_template_fk_name TEXT DEFAULT NULL, -- catalog_info.conname
        IN p_update_condition TEXT DEFAULT 'RESTRICT',
        IN p_delete_condition TEXT DEFAULT 'RESTRICT',
        IN p_check_single_table BOOLEAN DEFAULT false
      )
      RETURNS VOID AS $BODY$
      DECLARE
        query TEXT;
      BEGIN

        FOR query IN SELECT unnest(sharding.get_create_virtual_foreign_key_queries(
          p_referencing_table,
          p_referencing_columns,
          p_referenced_table,
          p_referenced_columns,
          p_template_fk_name,
          p_update_condition,
          p_delete_condition,
          p_check_single_table
        )) LOOP
          EXECUTE query;
        END LOOP;
      END;
      $BODY$ LANGUAGE plpgsql;
    SQL
  end
end
