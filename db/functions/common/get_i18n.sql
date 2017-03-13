-- DROP FUNCTION IF EXISTS common.get_i18n(TEXT, TEXT);

CREATE OR REPLACE FUNCTION common.get_i18n(
  IN  key       TEXT,
  IN  locale    TEXT DEFAULT 'pt',
  OUT i18n_text TEXT
)
RETURNS text AS $BODY$
BEGIN

  IF locale NOT IN ('pt','en','es','fr','de','it') THEN
      RAISE EXCEPTION 'unknown language in i18n table'
          USING ERRCODE = 'JA001';
  END IF;

  EXECUTE 'SELECT ' || locale || ' FROM public.i18n WHERE key = $1'
  USING key
  INTO i18n_text;

  RETURN;
END;
$BODY$ LANGUAGE plpgsql;
