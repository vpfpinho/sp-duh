-- DROP FUNCTION IF EXISTS sharding.get_auxiliary_table_information();

CREATE OR REPLACE FUNCTION sharding.get_auxiliary_table_information(
  OUT auxiliary_table_information JSONB
)
RETURNS JSONB AS $BODY$
BEGIN
  auxiliary_table_information = '{
    "unsharded_tables": [],
    "inherited_tables": []
  }'::JSONB;

  RETURN;
END;
$BODY$ LANGUAGE 'plpgsql' STABLE;
