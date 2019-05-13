--DROP FUNCTION common.get_certified_software_notice(integer, text);
CREATE OR REPLACE FUNCTION common.get_certified_software_notice(
  IN _company_id   INTEGER,
  IN locale    TEXT DEFAULT 'pt_PT',
  OUT certified_software_notice text,
  OUT get_certified_software_notice text,
  OUT document_certified_notice_non_hashed text,
  OUT document_certified_notice text,
  OUT document_certified_notice_short text
)

RETURNS RECORD AS $BODY$
DECLARE
  _product    text;
  _brand      text;
BEGIN
  SELECT product, brand FROM common.get_company_product_and_brand(_company_id) INTO _product, _brand;
  SELECT common.get_i18n('certified_software_notice_' || _product || '_' || _brand, locale)::TEXT AS certified_software_notice,
          common.get_i18n('certified_software_notice_' || _product || '_' || _brand, locale)::TEXT AS get_certified_software_notice,
          common.get_i18n('document_certified_notice_non_hashed_' || _product || '_' || _brand, locale)::TEXT AS document_certified_notice_non_hashed,
          common.get_i18n('document_certified_notice_' || _product || '_' || _brand, locale)::TEXT AS document_certified_notice,
          common.get_i18n('document_certified_notice_short_' || _product || '_' || _brand, locale)::TEXT AS document_certified_notice_short 
          INTO certified_software_notice, get_certified_software_notice, document_certified_notice_non_hashed, document_certified_notice, document_certified_notice_short;
  RETURN;
END;
$BODY$ LANGUAGE plpgsql IMMUTABLE;