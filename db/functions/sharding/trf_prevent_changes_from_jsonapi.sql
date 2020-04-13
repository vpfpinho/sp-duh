-- DROP FUNCTION IF EXISTS sharding.trf_prevent_changes_from_jsonapi();

CREATE OR REPLACE FUNCTION sharding.trf_prevent_changes_from_jsonapi()
RETURNS TRIGGER AS $BODY$
BEGIN
  
  IF inside_jsonapi() THEN
    RAISE WARNING '%', format('invalid operation attempt via API! %s: %s.%s (company_id:%s user_id:%s)', TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME, get_jsonapi_company(), get_jsonapi_user());
    RAISE EXCEPTION 'Não é possível efectuar alterações na resource através da API.' USING ERRCODE = 'JA002';
  END IF;

  RETURN CASE TG_OP WHEN 'DELETE' THEN OLD ELSE NEW END;
END;
$BODY$ LANGUAGE 'plpgsql';
