-- DROP FUNCTION IF EXISTS sharding.create_virtual_polymorphic_foreign_key(TEXT, TEXT, JSONB, TEXT, "char", "char", JSONB);

CREATE OR REPLACE FUNCTION sharding.create_virtual_polymorphic_foreign_key(
  IN p_referencing_table TEXT,
  IN p_referenced_table TEXT,
  IN p_column_mappings JSONB, -- { "local_col_a": "remote_col_a", "local_col_b": "remote_col_b", "local_col_c": null }
  IN p_template_fk_name TEXT DEFAULT NULL,
  IN p_update_condition "char" DEFAULT NULL,
  IN p_delete_condition "char" DEFAULT NULL,
  IN p_trigger_conditions JSONB DEFAULT NULL -- { "local_col_c": [ "value_a", "value_b" ] }
)
RETURNS VOID AS $BODY$
DECLARE
  query TEXT;
BEGIN

  FOR query IN SELECT unnest(sharding.get_create_virtual_polymorphic_foreign_key_queries(
    p_referencing_table,
    p_referenced_table,
    p_column_mappings,
    p_template_fk_name,
    p_update_condition,
    p_delete_condition,
    p_trigger_conditions
  )) LOOP
    -- RAISE 'query: %', query;

    EXECUTE query;
  END LOOP;
END;
$BODY$ LANGUAGE plpgsql;