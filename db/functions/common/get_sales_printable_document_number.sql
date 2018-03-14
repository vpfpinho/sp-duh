DROP FUNCTION IF EXISTS common.get_sales_printable_document_number(text, text, text, text, text, text);

CREATE OR REPLACE FUNCTION common.get_sales_printable_document_number(
  document_no                text,
  document_type              text,
  document_series_prefix     text,
  manual_registration_type   text,
  manual_registration_series text,
  manual_registration_number text
)
  RETURNS character varying(255) AS $BODY$
DECLARE
  _document_id character varying;
BEGIN

  _document_id := NULL;

  -- Do NOT use the series prefix for deciding if the series is external/manual. We can have an 'E' series that is NOT external.
  -- External documents have all "manual_" attributes set.
  -- Manual documents have the "manual_registration_series" and the "manual_registration_number" attributes set, but NOT the "manual_registration_type".
  IF manual_registration_type IS NOT NULL AND manual_registration_series IS NOT NULL AND manual_registration_number IS NOT NULL THEN
    _document_id := concat(manual_registration_type, ' ', manual_registration_series, '/', manual_registration_number)::character varying;
  ELSIF manual_registration_series IS NOT NULL AND manual_registration_number IS NOT NULL THEN
    _document_id := concat(document_type, ' ', manual_registration_series, '/', manual_registration_number)::character varying;
  ELSE
    _document_id := document_no;
  END IF;

  RETURN _document_id;
END;
$BODY$ LANGUAGE 'plpgsql' STABLE;
