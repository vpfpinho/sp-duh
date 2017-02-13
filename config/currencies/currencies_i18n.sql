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

CREATE OR REPLACE FUNCTION currency_spellout (a_value NUMERIC, a_currency_iso_code TEXT, a_locale TEXT)
    RETURNS TEXT AS
$BODY$
DECLARE
    rows            TEXT[];
    keys            TEXT[];
    major           NUMERIC;
    minor           NUMERIC;
    cc              TEXT;
    tmp_text        TEXT;
    locale          TEXT;
    locale_exists   BOOLEAN;
    strresult       TEXT;
    nodp            INT;
BEGIN

    -- COUNTRY CODE ISO CODE --
    cc   := lower(a_currency_iso_code);
    nodp := 2;

    -- SEARCH FOR LOCALE --
    locale := lower(a_locale);
    EXECUTE 
        format('SELECT attname FROM pg_attribute WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = ''%1$s'') AND attname = ''%2$s''','i18n', a_locale) 
    INTO tmp_text;
    locale_exists := tmp_text IS NOT NULL;
    IF FALSE = locale_exists
    THEN
        -- locale fallback is language code --
        IF LENGTH(a_locale) = 5
        THEN
            locale := SUBSTR(locale, 1, 2);
            EXECUTE
                format('SELECT attname FROM pg_attribute WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = ''%1$s'') AND attname = ''%2$s''','i18n', 
                    locale
                )
            INTO tmp_text;
            locale_exists := tmp_text IS NOT NULL;
            IF TRUE = locale_exists
            THEN
                locale := locale || ',';
            ELSE
                locale := '';
            END IF;
        ELSE
            locale := '';
        END IF;
    ELSE
        locale := locale || ',';
    END IF;

    RAISE NOTICE 'Locale % -> %', a_locale, locale;

    EXECUTE    
        'SELECT array( SELECT COALESCE(' || locale || 'en)FROM i18n
                JOIN unnest(
                    ARRAY[''' || cc || '_major_singular'', ''' || cc || '_major_plural'', ''' || cc || '_minor_singular'', ''' || cc || '_minor_plural'', ''currency_value_spellout'']
                ) 
                    WITH ORDINALITY t(key, ord) USING (key)
        ORDER  BY t.ord);'
    INTO rows;

    RAISE NOTICE '%', rows;

    major := TRUNC(a_value)::NUMERIC;
    minor := (ROUND((a_value - major)::numeric, nodp) * POWER(10, nodp))::NUMERIC;

    SELECT spellout FROM pg_cpp_utils_currency_spellout(a_locale, 
        major, rows[1], rows[2], 
        minor, rows[3], rows[4], 
    rows[5]) into strresult;
    RETURN strresult;
END;
$BODY$
LANGUAGE 'plpgsql' IMMUTABLE;

SELECT currency_spellout(123.45, 'USD', 'pt');

