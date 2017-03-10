-- DROP FUNCTION IF EXISTS sharding.drop_virtual_polymorphic_foreign_key(TEXT, TEXT, JSONB, TEXT);

CREATE OR REPLACE FUNCTION sharding.drop_virtual_polymorphic_foreign_key(
  IN p_referencing_table TEXT,
  IN p_referenced_table TEXT,
  IN p_column_mappings JSONB, -- { "local_col_a": "remote_col_a", "local_col_b": "remote_col_b", "local_col_c": null }
  IN p_template_fk_name TEXT DEFAULT NULL
)
RETURNS VOID AS $BODY$
DECLARE
  query TEXT;
BEGIN

  FOR query IN SELECT unnest(sharding.get_drop_virtual_polymorphic_foreign_key_queries(
    p_referencing_table,
    p_referenced_table,
    p_column_mappings,
    p_template_fk_name
  )) LOOP
    -- RAISE 'query: %', query;

    EXECUTE query;
  END LOOP;
END;
$BODY$ LANGUAGE plpgsql;