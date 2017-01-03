DROP FUNCTION IF EXISTS sharding.create_virtual_foreign_key(TEXT, TEXT[], TEXT, TEXT[], TEXT, TEXT, TEXT);

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