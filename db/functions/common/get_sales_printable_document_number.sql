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

  IF document_type::text = 'RC'::text THEN
    IF manual_registration_type IS NOT NULL AND manual_registration_series IS NOT NULL AND manual_registration_number IS NOT NULL THEN
      _document_id := concat(manual_registration_type, ' ', manual_registration_series, '/', manual_registration_number)::character varying;
    ELSE
      _document_id := document_no;
    END IF;
  ELSE
    IF document_series_prefix::text = 'E'::text THEN
      _document_id := concat(manual_registration_type, ' ', manual_registration_series, '/', manual_registration_number)::character varying;
    ELSIF document_series_prefix::text = 'M'::text THEN
      _document_id := concat(document_type, ' ', manual_registration_series, '/', manual_registration_number)::character varying;
    ELSE
      _document_id := document_no;
    END IF;
  END IF;

  RETURN _document_id;
END;
$BODY$ LANGUAGE 'plpgsql' STABLE;
