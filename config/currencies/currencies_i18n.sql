CREATE TABLE i18n (
    key TEXT,
    pt TEXT,
    en TEXT,
    es TEXT,
    fr TEXT,
    de TEXT,
    it TEXT,
    PRIMARY KEY (key)
);

CREATE INDEX i18n_key_index ON i18n(key);

INSERT INTO i18n(key, en) SELECT CONCAT(lower(key),'_major_singular'), name FROM currency_map;
INSERT INTO i18n(key, en) SELECT CONCAT(lower(key),'_major_plural'), name_plural FROM currency_map;

INSERT INTO i18n (key, pt, en) VALUES ('currency_value_spellout', 
	'{3} {0, plural, =1 {{1}} other {{2}}}{4, plural, =0 {} other { e {7} {4, plural, =1 {{5}} other {{6}}}}}',
	'{3} {0, plural, =1 {{1}} other {{2}}}{4, plural, =0 {} other { and {7} {4, plural, =1 {{5}} other {{6}}}}}'

);

INSERT INTO i18n (key, pt, en) VALUES ('eur_minor_singular', 'cêntimo' , 'cent' );
INSERT INTO i18n (key, pt, en) VALUES ('eur_minor_plural', 'cêntimos', 'cents');

INSERT INTO i18n (key, en) VALUES ('usd_minor_singular', 'cent' );
INSERT INTO i18n (key, en) VALUES ('usd_minor_plural', 'cents');

INSERT INTO i18n (key, en) VALUES ('gbp_minor_singular', 'pence' );
INSERT INTO i18n (key, en) VALUES ('gbp_minor_plural', 'pence');

CREATE OR REPLACE FUNCTION currency_spellout (a_value REAL, a_currency_iso_code TEXT, a_locale TEXT)
    RETURNS text AS
$BODY$
DECLARE
    rows text[];
    keys text[];
    major real;
    minor real;
    cc text;
    strresult text;
BEGIN

    cc   := lower(a_currency_iso_code);
    keys := '{' || cc || '_major_singular, ' || cc || '_major_plural, ' || cc || '_minor_singular, ' || cc || '_minor_plural, currency_value_spellout}';
    keys := keys::text[];

    -- TODO pt -> a_locate
    rows:=array(
        SELECT COALESCE(pt, en) FROM i18n
            JOIN unnest(keys) 
                WITH ORDINALITY t(key, ord) USING (key)
            ORDER  BY t.ord
    );

    major := TRUNC(a_value)::real;
    minor := (ROUND((a_value - major)::numeric, 2) * 100)::real;

    SELECT spellout FROM pg_cpp_utils_currency_spellout(a_locale, 
        major, rows[1], rows[2], 
        minor, rows[3], rows[4], 
    rows[5]) into strresult;
    RETURN strresult;
END;
$BODY$
LANGUAGE 'plpgsql' IMMUTABLE;

SELECT currency_spellout(123.45, 'USD', 'pt');

