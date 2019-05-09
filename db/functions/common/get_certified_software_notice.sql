CREATE OR REPLACE FUNCTION common.get_certified_software_notice(
  _company_id   INTEGER,
  locale    TEXT DEFAULT 'pt_PT'
)
RETURNS text AS $BODY$
DECLARE
  _i18n_text  text;
  _product    text;
  _brand      text;
BEGIN
    SELECT product, brand FROM common.get_company_product_and_brand(_company_id) INTO _product, _brand;
    _i18n_text := common.get_i18n('certified_software_notice_' || _product || '_' || _brand, locale);
    
    RETURN _i18n_text;
END;
$BODY$ LANGUAGE plpgsql IMMUTABLE;