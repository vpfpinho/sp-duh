CREATE OR REPLACE FUNCTION public.currency_spellout (a_value NUMERIC, a_currency_iso_code TEXT, a_locale TEXT)
    RETURNS TEXT AS
$BODY$
DECLARE
    rows              TEXT[];
    keys              TEXT[];
    major             NUMERIC;
    minor             NUMERIC;
    cc                TEXT;
    tmp_text          TEXT;
    locale            TEXT;
    locale_exists     BOOLEAN;
    nodd              INT;
    pattern           TEXT;
    symbol            TEXT;
    symbol_at_right   BOOLEAN;
    major_plural      TEXT;
    spellout_override TEXT;
    spellout_result   TEXT;
BEGIN

    -- COUNTRY CODE ISO CODE --
    cc := lower(a_currency_iso_code);

    -- SEARCH FOR LOCALE --
    locale := lower(a_locale);
    BEGIN
      EXECUTE
          format('SELECT %1$s FROM public.i18n LIMIT 0', a_locale)
      INTO tmp_text;
      locale_exists := TRUE;
    EXCEPTION WHEN undefined_column THEN
      locale_exists := FALSE;
    END;
    IF FALSE = locale_exists
    THEN
        -- locale fallback is language code --
        IF LENGTH(a_locale) = 5
        THEN

            locale := SUBSTR(locale, 1, 2);
            BEGIN
              EXECUTE
                  format('SELECT %1$s FROM public.i18n LIMIT 0', locale)
              INTO tmp_text;
              locale_exists := TRUE;
            EXCEPTION WHEN undefined_column THEN
              locale_exists := FALSE;
            END;
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
        'SELECT array( SELECT COALESCE(' || locale || 'en) FROM public.i18n
                JOIN unnest(
                    ARRAY[''' || cc || '_major_singular'', ''' || cc || '_major_plural'', ''' || cc || '_minor_singular'', ''' || cc || '_minor_plural'', ''currency_value_spellout'', ''custom_currency_spellout'']
                )
                    WITH ORDINALITY t(key, ord) USING (key)
        ORDER  BY t.ord);'
    INTO rows;

    IF array_length(rows, 1) < 5
    THEN
        -- pick currency name --
        EXECUTE
            format('SELECT COALESCE(%1$sen) FROM public.i18n WHERE key =''%2$s_major_plural'';', locale, cc)
        INTO major_plural;
        -- pick pattern for this currency --
        EXECUTE
            format('SELECT pattern, symbol, symbol_at_right, minor_unit FROM public.currencies WHERE iso_code =''%1$s'';', upper(cc))
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
        SELECT formatted FROM public.pg_cpp_utils_format_number(a_locale, ROUND(a_value, nodd), pattern) INTO spellout_result;
    ELSE
        -- get number of decimal places --
        EXECUTE
            format('SELECT minor_unit FROM public.currencies WHERE iso_code =''%1$s'';', upper(cc))
        INTO nodd;
        -- split value components ---
        major := TRUNC(a_value)::NUMERIC;
        minor := (ROUND((a_value - major)::numeric, nodd) * POWER(10, nodd))::NUMERIC;
        -- contains spellout override?
        IF array_length(rows, 1) = 6
        THEN
          IF rows[6] IS NOT NULL
          THEN
            spellout_override := rows[6];
          ELSE
            spellout_override := '';
          END IF;
        ELSE
          spellout_override := '';
        END IF;
        -- ask C++ to spell out this number --
        SELECT spellout FROM public.pg_cpp_utils_currency_spellout(a_locale,
            major, rows[1], rows[2],
            minor, rows[3], rows[4], rows[5], spellout_override
        ) INTO spellout_result;
    END IF;

    RETURN spellout_result;
END;
$BODY$
LANGUAGE 'plpgsql' STABLE;
