
ALTER TABLE currencies ADD COLUMN symbol_at_right BOOLEAN DEFAULT true;
UPDATE currencies SET symbol_at_right=false WHERE iso_code IN ('GBP', 'USD', 'BRL', 'JPY');

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
    nodd            INT;
    pattern         TEXT;
    symbol          TEXT;
    symbol_at_right BOOLEAN;
    major_plural    TEXT;
    spellout_result TEXT;
BEGIN

    -- COUNTRY CODE ISO CODE --
    cc := lower(a_currency_iso_code);

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

    EXECUTE    
        'SELECT array( SELECT COALESCE(' || locale || 'en) FROM i18n
                JOIN unnest(
                    ARRAY[''' || cc || '_major_singular'', ''' || cc || '_major_plural'', ''' || cc || '_minor_singular'', ''' || cc || '_minor_plural'', ''currency_value_spellout'']
                ) 
                    WITH ORDINALITY t(key, ord) USING (key)
        ORDER  BY t.ord);'
    INTO rows;

    IF array_length(rows, 1) < 5 
    THEN
        -- pick currency name --
        EXECUTE 
            format('SELECT COALESCE(%1$sen) FROM i18n WHERE key =''%2$s_major_plural'';', locale, cc)
        INTO major_plural;
        -- pick pattern for this currency --
        EXECUTE 
            format('SELECT pattern, symbol, symbol_at_right, minor_unit FROM currencies WHERE iso_code =''%1$s'';', upper(cc))
        INTO pattern, symbol, symbol_at_right, nodd;

        IF LENGTH(major_plural) > 0
        THEN
            symbol := major_plural;
            symbol_at_right := true;
        END IF;
        -- ask C++ to format value as --
        IF symbol_at_right
        THEN
            pattern := pattern || ' ' || symbol;
        ELSE
            pattern := symbol || ' ' || pattern;
        END IF;
        SELECT formatted FROM pg_cpp_utils_format_number(a_locale, ROUND(a_value, nodd), pattern) INTO spellout_result;
    ELSE
        -- get number of decimal places --
        EXECUTE 
            format('SELECT minor_unit FROM currencies WHERE iso_code =''%1$s'';', upper(cc))
        INTO nodd;
        -- split value components ---
        major := TRUNC(a_value)::NUMERIC;
        minor := (ROUND((a_value - major)::numeric, nodd) * POWER(10, nodd))::NUMERIC;
        -- ask C++ to spell out this number --
        SELECT spellout FROM pg_cpp_utils_currency_spellout(a_locale, 
            major, rows[1], rows[2], 
            minor, rows[3], rows[4], rows[5]
        ) INTO spellout_result;
    END IF;

    RETURN spellout_result;
END;
$BODY$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OR REPLACE FUNCTION currency_format (a_value NUMERIC, a_currency_iso_code TEXT, a_locale TEXT)
    RETURNS TEXT AS
$BODY$
DECLARE
    cc              TEXT;
    nodd            INT;
    pattern         TEXT;
    symbol          TEXT;
    symbol_at_right BOOLEAN;
    spellout_result TEXT;
BEGIN
    -- COUNTRY CODE ISO CODE --
    cc := lower(a_currency_iso_code);
    -- pick pattern for this currency --
    EXECUTE 
        format('SELECT pattern, symbol, symbol_at_right, minor_unit FROM currencies WHERE iso_code =''%1$s'';', upper(cc))
    INTO pattern, symbol, symbol_at_right, nodd;
    -- ask C++ to format value as --
    IF symbol_at_right
    THEN
        pattern := pattern || ' ' || symbol;
    ELSE
        pattern := symbol || ' ' || pattern;
    END IF;
    SELECT formatted FROM pg_cpp_utils_format_number(a_locale, ROUND(a_value, nodd), pattern) INTO spellout_result;

    RETURN spellout_result;
END;
$BODY$
LANGUAGE 'plpgsql' IMMUTABLE;

SELECT currency_spellout(123.45, 'JPY', 'jp_JP');
SELECT currency_spellout(123.45, 'BTN', 'pt');

SELECT currency_spellout(123.45, 'USD', 'en_US');
SELECT currency_spellout(123.45, 'EUR', 'pt_PT');
SELECT currency_spellout(123.45, 'GBP', 'en_BG');


SELECT currency_format(123.45, 'USD', 'en_US');
SELECT currency_format(123.45, 'EUR', 'pt_PT');
SELECT currency_format(123.45, 'GBP', 'en_BG');
