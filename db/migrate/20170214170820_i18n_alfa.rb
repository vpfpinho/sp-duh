class I18nAlfa < ActiveRecord::Migration

  def up
    execute <<-'SQLSQLSQL'

      CREATE TYPE public.pg_cpp_utils_version_record AS (version text);
      CREATE TYPE public.pg_cpp_utils_hash_record AS (long_hash text, short_hash text);
      CREATE TYPE public.pg_cpp_utils_number_spellout_record AS (spellout text);
      CREATE TYPE public.pg_cpp_utils_format_number_record AS (formatted text);

      CREATE OR REPLACE FUNCTION public.pg_cpp_utils_version (
      ) RETURNS public.pg_cpp_utils_version_record AS '$libdir/pg-cpp-utils.so', 'pg_cpp_utils_version' LANGUAGE C STRICT;

      CREATE OR REPLACE FUNCTION public.pg_cpp_utils_invoice_hash (
        a_pem_uri text,
        a_payload text
      ) RETURNS public.pg_cpp_utils_hash_record AS '$libdir/pg-cpp-utils.so', 'pg_cpp_utils_invoice_hash' LANGUAGE C STRICT;

      CREATE OR REPLACE FUNCTION public.pg_cpp_utils_number_spellout (
        a_locale            varchar(5),
        a_payload           float8,
        a_spellout_override text default ''
      ) RETURNS public.pg_cpp_utils_number_spellout_record AS '$libdir/pg-cpp-utils.so', 'pg_cpp_utils_number_spellout' LANGUAGE C STRICT;

      CREATE OR REPLACE FUNCTION public.pg_cpp_utils_currency_spellout (
        a_locale            varchar(5),
        a_major           float8,
        a_major_singular    text,
        a_major_plural      text,
        a_minor           float8,
        a_minor_singular    text,
        a_minor_plural      text,
        a_format            text,
        a_spellout_override text default ''
      ) RETURNS public.pg_cpp_utils_number_spellout_record AS '$libdir/pg-cpp-utils.so', 'pg_cpp_utils_currency_spellout' LANGUAGE C STRICT;

      CREATE OR REPLACE FUNCTION public.pg_cpp_utils_format_number (
        a_locale  varchar(5),
        a_value   float8,
        a_pattern text
      ) RETURNS public.pg_cpp_utils_format_number_record AS '$libdir/pg-cpp-utils.so', 'pg_cpp_utils_format_number' LANGUAGE C STRICT;

			ALTER TABLE public.currencies ADD COLUMN symbol_at_right BOOLEAN DEFAULT true;
			UPDATE public.currencies SET symbol_at_right=false WHERE iso_code IN ('GBP', 'USD', 'BRL');

			CREATE TABLE public.i18n (
			    key TEXT,
			    pt TEXT,
			    en TEXT,
			    es TEXT,
			    fr TEXT,
			    de TEXT,
			    it TEXT,
			    PRIMARY KEY (key)
			);

      CREATE INDEX i18n_key_index ON public.i18n(key);

      CREATE TABLE public.currency_map (
          key VARCHAR(3),
          symbol VARCHAR(4),
          name TEXT,
          symbol_native VARCHAR(5),
          decimal_digits INT,
          rounding NUMERIC(3, 2),
          code VARCHAR(3),
          name_plural TEXT
      );
      INSERT INTO public.currency_map VALUES ('AED','AED','United Arab Emirates Dirham','د.إ.‏',2,0.0,'AED','UAE dirhams');
      INSERT INTO public.currency_map VALUES ('AFN','AFN','Afghan Afghani','؋',0,0.0,'AFN','Afghan Afghanis');
      INSERT INTO public.currency_map VALUES ('ALL','ALL','Albanian Lek','Lek',0,0.0,'ALL','Albanian lekë');
      INSERT INTO public.currency_map VALUES ('AMD','AMD','Armenian Dram','դր.',0,0.0,'AMD','Armenian drams');
      INSERT INTO public.currency_map VALUES ('AOA','AOA','Angolan Kwanza','Kz',2,0.0,'AOA','Angolan kwanzas');
      INSERT INTO public.currency_map VALUES ('ARS','ARS','Argentine Peso','$',2,0.0,'ARS','Argentine pesos');
      INSERT INTO public.currency_map VALUES ('AUD','AU$','Australian Dollar','$',2,0.0,'AUD','Australian dollars');
      INSERT INTO public.currency_map VALUES ('AWG','AWG','Aruban Florin','Afl.',2,0.0,'AWG','Aruban florin');
      INSERT INTO public.currency_map VALUES ('AZN','AZN','Azerbaijani Manat','ман.',2,0.0,'AZN','Azerbaijani manats');
      INSERT INTO public.currency_map VALUES ('BAM','BAM','Bosnia-Herzegovina Convertible Mark','KM',2,0.0,'BAM','Bosnia-Herzegovina convertible marks');
      INSERT INTO public.currency_map VALUES ('BBD','BBD','Barbadian Dollar','$',2,0.0,'BBD','Barbadian dollars');
      INSERT INTO public.currency_map VALUES ('BDT','BDT','Bangladeshi Taka','৳',2,0.0,'BDT','Bangladeshi takas');
      INSERT INTO public.currency_map VALUES ('BGN','BGN','Bulgarian Lev','лв.',2,0.0,'BGN','Bulgarian leva');
      INSERT INTO public.currency_map VALUES ('BHD','BHD','Bahraini Dinar','د.ب.‏',3,0.0,'BHD','Bahraini dinars');
      INSERT INTO public.currency_map VALUES ('BIF','BIF','Burundian Franc','FBu',0,0.0,'BIF','Burundian francs');
      INSERT INTO public.currency_map VALUES ('BMD','BMD','Bermudan Dollar','$',2,0.0,'BMD','Bermudan dollars');
      INSERT INTO public.currency_map VALUES ('BND','BND','Brunei Dollar','$',2,0.0,'BND','Brunei dollars');
      INSERT INTO public.currency_map VALUES ('BOB','BOB','Bolivian Boliviano','Bs',2,0.0,'BOB','Bolivian bolivianos');
      INSERT INTO public.currency_map VALUES ('BRL','R$','Brazilian Real','R$',2,0.0,'BRL','Brazilian reals');
      INSERT INTO public.currency_map VALUES ('BWP','BWP','Botswanan Pula','P',2,0.0,'BWP','Botswanan pulas');
      INSERT INTO public.currency_map VALUES ('BYR','BYR','Belarusian Ruble','BYR',0,0.0,'BYR','Belarusian rubles');
      INSERT INTO public.currency_map VALUES ('BZD','BZD','Belize Dollar','$',2,0.0,'BZD','Belize dollars');
      INSERT INTO public.currency_map VALUES ('CAD','CA$','Canadian Dollar','$',2,0.0,'CAD','Canadian dollars');
      INSERT INTO public.currency_map VALUES ('CDF','CDF','Congolese Franc','FrCD',2,0.0,'CDF','Congolese francs');
      INSERT INTO public.currency_map VALUES ('CHF','CHF','Swiss Franc','CHF',2,0.05,'CHF','Swiss francs');
      INSERT INTO public.currency_map VALUES ('CLP','CLP','Chilean Peso','$',0,0.0,'CLP','Chilean pesos');
      INSERT INTO public.currency_map VALUES ('CNY','CN¥','Chinese Yuan','CN¥',2,0.0,'CNY','Chinese yuan');
      INSERT INTO public.currency_map VALUES ('COP','COP','Colombian Peso','$',0,0.0,'COP','Colombian pesos');
      INSERT INTO public.currency_map VALUES ('CRC','CRC','Costa Rican Colón','₡',0,0.0,'CRC','Costa Rican colóns');
      INSERT INTO public.currency_map VALUES ('CVE','CVE','Cape Verdean Escudo','CVE',2,0.0,'CVE','Cape Verdean escudos');
      INSERT INTO public.currency_map VALUES ('CZK','CZK','Czech Republic Koruna','Kč',2,0.0,'CZK','Czech Republic korunas');
      INSERT INTO public.currency_map VALUES ('DJF','DJF','Djiboutian Franc','Fdj',0,0.0,'DJF','Djiboutian francs');
      INSERT INTO public.currency_map VALUES ('DKK','DKK','Danish Krone','kr',2,0.0,'DKK','Danish kroner');
      INSERT INTO public.currency_map VALUES ('DOP','DOP','Dominican Peso','$',2,0.0,'DOP','Dominican pesos');
      INSERT INTO public.currency_map VALUES ('DZD','DZD','Algerian Dinar','د.ج.‏',2,0.0,'DZD','Algerian dinars');
      INSERT INTO public.currency_map VALUES ('EGP','EGP','Egyptian Pound','ج.م.‏',2,0.0,'EGP','Egyptian pounds');
      INSERT INTO public.currency_map VALUES ('ERN','ERN','Eritrean Nakfa','Nfk',2,0.0,'ERN','Eritrean nakfas');
      INSERT INTO public.currency_map VALUES ('ETB','ETB','Ethiopian Birr','ብር',2,0.0,'ETB','Ethiopian birrs');
      INSERT INTO public.currency_map VALUES ('EUR','€','Euro','€',2,0.0,'EUR','euros');
      INSERT INTO public.currency_map VALUES ('GBP','£','British Pound Sterling','£',2,0.0,'GBP','British pounds sterling');
      INSERT INTO public.currency_map VALUES ('GEL','GEL','Georgian Lari','GEL',2,0.0,'GEL','Georgian laris');
      INSERT INTO public.currency_map VALUES ('GHS','GHS','Ghanaian Cedi','GHS',2,0.0,'GHS','Ghanaian cedis');
      INSERT INTO public.currency_map VALUES ('GNF','GNF','Guinean Franc','FG',0,0.0,'GNF','Guinean francs');
      INSERT INTO public.currency_map VALUES ('GTQ','GTQ','Guatemalan Quetzal','Q',2,0.0,'GTQ','Guatemalan quetzals');
      INSERT INTO public.currency_map VALUES ('GYD','GYD','Guyanaese Dollar','GYD',0,0.0,'GYD','Guyanaese dollars');
      INSERT INTO public.currency_map VALUES ('HKD','HK$','Hong Kong Dollar','$',2,0.0,'HKD','Hong Kong dollars');
      INSERT INTO public.currency_map VALUES ('HNL','HNL','Honduran Lempira','L',2,0.0,'HNL','Honduran lempiras');
      INSERT INTO public.currency_map VALUES ('HRK','HRK','Croatian Kuna','kn',2,0.0,'HRK','Croatian kunas');
      INSERT INTO public.currency_map VALUES ('HUF','HUF','Hungarian Forint','Ft',0,0.0,'HUF','Hungarian forints');
      INSERT INTO public.currency_map VALUES ('IDR','IDR','Indonesian Rupiah','Rp',0,0.0,'IDR','Indonesian rupiahs');
      INSERT INTO public.currency_map VALUES ('ILS','₪','Israeli New Sheqel','₪',2,0.0,'ILS','Israeli new sheqels');
      INSERT INTO public.currency_map VALUES ('INR','₹','Indian Rupee','₹',2,0.0,'INR','Indian rupees');
      INSERT INTO public.currency_map VALUES ('IQD','IQD','Iraqi Dinar','د.ع.‏',0,0.0,'IQD','Iraqi dinars');
      INSERT INTO public.currency_map VALUES ('IRR','IRR','Iranian Rial','﷼',0,0.0,'IRR','Iranian rials');
      INSERT INTO public.currency_map VALUES ('ISK','ISK','Icelandic Króna','kr',0,0.0,'ISK','Icelandic krónur');
      INSERT INTO public.currency_map VALUES ('JMD','JMD','Jamaican Dollar','$',2,0.0,'JMD','Jamaican dollars');
      INSERT INTO public.currency_map VALUES ('JOD','JOD','Jordanian Dinar','د.أ.‏',3,0.0,'JOD','Jordanian dinars');
      INSERT INTO public.currency_map VALUES ('JPY','¥','Japanese Yen','￥',0,0.0,'JPY','Japanese yen');
      INSERT INTO public.currency_map VALUES ('KES','KES','Kenyan Shilling','Ksh',2,0.0,'KES','Kenyan shillings');
      INSERT INTO public.currency_map VALUES ('KHR','KHR','Cambodian Riel','៛',2,0.0,'KHR','Cambodian riels');
      INSERT INTO public.currency_map VALUES ('KMF','KMF','Comorian Franc','CF',0,0.0,'KMF','Comorian francs');
      INSERT INTO public.currency_map VALUES ('KRW','₩','South Korean Won','₩',0,0.0,'KRW','South Korean won');
      INSERT INTO public.currency_map VALUES ('KWD','KWD','Kuwaiti Dinar','د.ك.‏',3,0.0,'KWD','Kuwaiti dinars');
      INSERT INTO public.currency_map VALUES ('KZT','KZT','Kazakhstani Tenge','₸',2,0.0,'KZT','Kazakhstani tenges');
      INSERT INTO public.currency_map VALUES ('LBP','LBP','Lebanese Pound','ل.ل.‏',0,0.0,'LBP','Lebanese pounds');
      INSERT INTO public.currency_map VALUES ('LKR','LKR','Sri Lankan Rupee','රු.',2,0.0,'LKR','Sri Lankan rupees');
      INSERT INTO public.currency_map VALUES ('LRD','LRD','Liberian Dollar','$',2,0.0,'LRD','Liberian dollars');
      INSERT INTO public.currency_map VALUES ('LTL','LTL','Lithuanian Litas','Lt',2,0.0,'LTL','Lithuanian litai');
      INSERT INTO public.currency_map VALUES ('LVL','LVL','Latvian Lats','Ls',2,0.0,'LVL','Latvian lati');
      INSERT INTO public.currency_map VALUES ('LYD','LYD','Libyan Dinar','د.ل.‏',3,0.0,'LYD','Libyan dinars');
      INSERT INTO public.currency_map VALUES ('MAD','MAD','Moroccan Dirham','د.م.‏',2,0.0,'MAD','Moroccan dirhams');
      INSERT INTO public.currency_map VALUES ('MDL','MDL','Moldovan Leu','MDL',2,0.0,'MDL','Moldovan lei');
      INSERT INTO public.currency_map VALUES ('MGA','MGA','Malagasy Ariary','MGA',0,0.0,'MGA','Malagasy Ariaries');
      INSERT INTO public.currency_map VALUES ('MKD','MKD','Macedonian Denar','MKD',2,0.0,'MKD','Macedonian denari');
      INSERT INTO public.currency_map VALUES ('MMK','MMK','Myanma Kyat','K',0,0.0,'MMK','Myanma kyats');
      INSERT INTO public.currency_map VALUES ('MOP','MOP','Macanese Pataca','MOP',2,0.0,'MOP','Macanese patacas');
      INSERT INTO public.currency_map VALUES ('MUR','MUR','Mauritian Rupee','MUR',0,0.0,'MUR','Mauritian rupees');
      INSERT INTO public.currency_map VALUES ('MXN','MX$','Mexican Peso','$',2,0.0,'MXN','Mexican pesos');
      INSERT INTO public.currency_map VALUES ('MYR','MYR','Malaysian Ringgit','RM',2,0.0,'MYR','Malaysian ringgits');
      INSERT INTO public.currency_map VALUES ('MZN','MZN','Mozambican Metical','MTn',2,0.0,'MZN','Mozambican meticals');
      INSERT INTO public.currency_map VALUES ('NAD','NAD','Namibian Dollar','$',2,0.0,'NAD','Namibian dollars');
      INSERT INTO public.currency_map VALUES ('NGN','NGN','Nigerian Naira','₦',2,0.0,'NGN','Nigerian nairas');
      INSERT INTO public.currency_map VALUES ('NIO','NIO','Nicaraguan Córdoba','C$',2,0.0,'NIO','Nicaraguan córdobas');
      INSERT INTO public.currency_map VALUES ('NOK','NOK','Norwegian Krone','kr',2,0.0,'NOK','Norwegian kroner');
      INSERT INTO public.currency_map VALUES ('NPR','NPR','Nepalese Rupee','नेरू',2,0.0,'NPR','Nepalese rupees');
      INSERT INTO public.currency_map VALUES ('NZD','NZ$','New Zealand Dollar','$',2,0.0,'NZD','New Zealand dollars');
      INSERT INTO public.currency_map VALUES ('OMR','OMR','Omani Rial','ر.ع.‏',3,0.0,'OMR','Omani rials');
      INSERT INTO public.currency_map VALUES ('PAB','PAB','Panamanian Balboa','B/.',2,0.0,'PAB','Panamanian balboas');
      INSERT INTO public.currency_map VALUES ('PEN','PEN','Peruvian Nuevo Sol','S/.',2,0.0,'PEN','Peruvian nuevos soles');
      INSERT INTO public.currency_map VALUES ('PHP','PHP','Philippine Peso','₱',2,0.0,'PHP','Philippine pesos');
      INSERT INTO public.currency_map VALUES ('PKR','PKR','Pakistani Rupee','₨',0,0.0,'PKR','Pakistani rupees');
      INSERT INTO public.currency_map VALUES ('PLN','PLN','Polish Zloty','zł',2,0.0,'PLN','Polish zlotys');
      INSERT INTO public.currency_map VALUES ('PYG','PYG','Paraguayan Guarani','₲',0,0.0,'PYG','Paraguayan guaranis');
      INSERT INTO public.currency_map VALUES ('QAR','QAR','Qatari Rial','ر.ق.‏',2,0.0,'QAR','Qatari rials');
      INSERT INTO public.currency_map VALUES ('RON','RON','Romanian Leu','RON',2,0.0,'RON','Romanian lei');
      INSERT INTO public.currency_map VALUES ('RSD','RSD','Serbian Dinar','дин.',0,0.0,'RSD','Serbian dinars');
      INSERT INTO public.currency_map VALUES ('RUB','RUB','Russian Ruble','руб.',2,0.0,'RUB','Russian rubles');
      INSERT INTO public.currency_map VALUES ('RWF','RWF','Rwandan Franc','FR',0,0.0,'RWF','Rwandan francs');
      INSERT INTO public.currency_map VALUES ('SAR','SAR','Saudi Riyal','ر.س.‏',2,0.0,'SAR','Saudi riyals');
      INSERT INTO public.currency_map VALUES ('SDG','SDG','Sudanese Pound','SDG',2,0.0,'SDG','Sudanese pounds');
      INSERT INTO public.currency_map VALUES ('SEK','SEK','Swedish Krona','kr',2,0.0,'SEK','Swedish kronor');
      INSERT INTO public.currency_map VALUES ('SGD','SGD','Singapore Dollar','$',2,0.0,'SGD','Singapore dollars');
      INSERT INTO public.currency_map VALUES ('SOS','SOS','Somali Shilling','SOS',0,0.0,'SOS','Somali shillings');
      INSERT INTO public.currency_map VALUES ('STD','STD','São Tomé and Príncipe Dobra','Db',0,0.0,'STD','São Tomé and Príncipe dobras');
      INSERT INTO public.currency_map VALUES ('SYP','SYP','Syrian Pound','ل.س.‏',0,0.0,'SYP','Syrian pounds');
      INSERT INTO public.currency_map VALUES ('THB','฿','Thai Baht','฿',2,0.0,'THB','Thai baht');
      INSERT INTO public.currency_map VALUES ('TND','TND','Tunisian Dinar','د.ت.‏',3,0.0,'TND','Tunisian dinars');
      INSERT INTO public.currency_map VALUES ('TOP','TOP','Tongan Paʻanga','T$',2,0.0,'TOP','Tongan paʻanga');
      INSERT INTO public.currency_map VALUES ('TRY','TRY','Turkish Lira','TL',2,0.0,'TRY','Turkish Lira');
      INSERT INTO public.currency_map VALUES ('TTD','TTD','Trinidad and Tobago Dollar','$',2,0.0,'TTD','Trinidad and Tobago dollars');
      INSERT INTO public.currency_map VALUES ('TWD','NT$','New Taiwan Dollar','NT$',2,0.0,'TWD','New Taiwan dollars');
      INSERT INTO public.currency_map VALUES ('TZS','TZS','Tanzanian Shilling','TSh',0,0.0,'TZS','Tanzanian shillings');
      INSERT INTO public.currency_map VALUES ('UAH','UAH','Ukrainian Hryvnia','₴',2,0.0,'UAH','Ukrainian hryvnias');
      INSERT INTO public.currency_map VALUES ('UGX','UGX','Ugandan Shilling','USh',0,0.0,'UGX','Ugandan shillings');
      INSERT INTO public.currency_map VALUES ('USD','$','US Dollar','$',2,0.0,'USD','US dollars');
      INSERT INTO public.currency_map VALUES ('UYU','UYU','Uruguayan Peso','$',2,0.0,'UYU','Uruguayan pesos');
      INSERT INTO public.currency_map VALUES ('UZS','UZS','Uzbekistan Som','UZS',0,0.0,'UZS','Uzbekistan som');
      INSERT INTO public.currency_map VALUES ('VEF','VEF','Venezuelan Bolívar','Bs.F.',2,0.0,'VEF','Venezuelan bolívars');
      INSERT INTO public.currency_map VALUES ('VND','₫','Vietnamese Dong','₫',0,0.0,'VND','Vietnamese dong');
      INSERT INTO public.currency_map VALUES ('XAF','FCFA','CFA Franc BEAC','FCFA',0,0.0,'XAF','CFA francs BEAC');
      INSERT INTO public.currency_map VALUES ('XOF','CFA','CFA Franc BCEAO','CFA',0,0.0,'XOF','CFA francs BCEAO');
      INSERT INTO public.currency_map VALUES ('YER','YER','Yemeni Rial','ر.ي.‏',0,0.0,'YER','Yemeni rials');
      INSERT INTO public.currency_map VALUES ('ZAR','ZAR','South African Rand','R',2,0.0,'ZAR','South African rand');
      INSERT INTO public.currency_map VALUES ('ZMK','ZMK','Zambian Kwacha','ZK',0,0.0,'ZMK','Zambian kwachas');

      INSERT INTO public.i18n(key, en) SELECT CONCAT(lower(key),'_major_singular'), name FROM public.currency_map;
      INSERT INTO public.i18n(key, en) SELECT CONCAT(lower(key),'_major_plural'), name_plural FROM public.currency_map;

      INSERT INTO public.i18n (key, pt, en) VALUES ('currency_value_spellout',
      	'{3} {0, plural, =1 {{1}} other {{2}}}{4, plural, =0 {} other { e {7} {4, plural, =1 {{5}} other {{6}}}}}',
      	'{3} {0, plural, =1 {{1}} other {{2}}}{4, plural, =0 {} other { and {7} {4, plural, =1 {{5}} other {{6}}}}}'
      );

      INSERT INTO public.i18n (key, pt) VALUES ('custom_currency_spellout',
'%%lenient-parse:
 &[last primary ignorable ] << '' '' << '','' << ''-'' << ''-'';
 %spellout-numbering-year:
 x.x: =#,###0.#=;
 0: =%spellout-numbering=;
 %spellout-numbering:
 0: =%spellout-cardinal-masculine=;
 %spellout-cardinal-masculine:
 -x: menos >>;
 x.x: << vírgula >>;
 0: zero;
 1: um;
 2: dois;
 3: três;
 4: quatro;
 5: cinco;
 6: seis;
 7: sete;
 8: oito;
 9: nove;
 10: dez;
 11: onze;
 12: doze;
 13: treze;
 14: catorze;
 15: quinze;
 16: dezasseis;
 17: dezassete;
 18: dezoito;
 19: dezanove;
 20: vinte[ e >>];
 30: trinta[ e >>];
 40: quarenta[ e >>];
 50: cinquenta[ e >>];
 60: sessenta[ e >>];
 70: setenta[ e >>];
 80: oitenta[ e >>];
 90: noventa[ e >>];
 100: cem;
 101: cento e >>;
 200: duzentos[ e >>];
 300: trezentos[ e >>];
 400: quatrocentos[ e >>];
 500: quinhentos[ e >>];
 600: seiscentos[ e >>];
 700: setecentos[ e >>];
 800: oitocentos[ e >>];
 900: novecentos[ e >>];
 1000: mil[, >>];
 2000: << mil[, >>];
 1000000: um milhão[, >>];
 2000000: << milhões[, >>];
 1000000000: um bilião[, >>];
 2000000000: << biliões[, >>];
 1000000000000: um trilião[, >>];
 2000000000000: << triliões[, >>];
 1000000000000000: um quatrilião[, >>];
 2000000000000000: << quatriliões[, >>];
 1000000000000000000: =#,##0=;');

      INSERT INTO public.i18n (key, pt, en) VALUES ('eur_minor_singular', 'cêntimo' , 'cent' );
      INSERT INTO public.i18n (key, pt, en) VALUES ('eur_minor_plural',  'cêntimos', 'cents');

      INSERT INTO public.i18n (key, pt, en) VALUES ('usd_minor_singular', 'cêntimo' , 'cent' );
      INSERT INTO public.i18n (key, pt, en) VALUES ('usd_minor_plural'  , 'cêntimos', 'cents');

      UPDATE public.i18n SET pt='libra estrelina',   en='pound sterling'  WHERE key = 'gbp_major_singular';
      UPDATE public.i18n SET pt='libras estrelinas', en='pounds sterling' WHERE key = 'gbp_major_plural';

      INSERT INTO public.i18n (key, pt, en) VALUES ('gbp_minor_singular', 'centavo',  'penny' );
      INSERT INTO public.i18n (key, pt, en) VALUES ('gbp_minor_plural'  , 'centavos', 'pence');

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
          EXECUTE
              format('SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = ''i18n'') AND attname = ''%1$s''', a_locale)
          INTO tmp_text;

          locale_exists := tmp_text IS NOT NULL;
          IF FALSE = locale_exists
          THEN
              -- locale fallback is language code --
              IF LENGTH(a_locale) = 5
              THEN
                  locale := SUBSTR(locale, 1, 2);
                  EXECUTE
                      format('SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = ''i18n'') AND attname = ''%1$s''',
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
      LANGUAGE 'plpgsql' IMMUTABLE;

      CREATE OR REPLACE FUNCTION public.currency_format (a_value NUMERIC, a_currency_iso_code TEXT, a_locale TEXT)
          RETURNS TEXT AS
      $BODY$
      DECLARE
          cc               TEXT;
          nodd             INT;
          pattern          TEXT;
          symbol           TEXT;
          symbol_at_right  BOOLEAN;
          formatted_result TEXT;
      BEGIN
          -- COUNTRY CODE ISO CODE --
          cc := lower(a_currency_iso_code);
          -- pick pattern for this currency --
          EXECUTE
              format('SELECT pattern, symbol, symbol_at_right, minor_unit FROM public.currencies WHERE iso_code =''%1$s'';', upper(cc))
          INTO pattern, symbol, symbol_at_right, nodd;
          -- ask C++ to format value as --
          IF symbol_at_right
          THEN
              pattern := pattern || ' ' || symbol;
          ELSE
              pattern := symbol || ' ' || pattern;
          END IF;
          SELECT formatted FROM public.pg_cpp_utils_format_number(a_locale, ROUND(a_value, nodd), pattern) INTO formatted_result;

          RETURN formatted_result;
      END;
      $BODY$
      LANGUAGE 'plpgsql' IMMUTABLE;
     SQLSQLSQL
  end

  def down

    execute <<-'SQLSQLSQL'

      DROP FUNCTION IF EXISTS public.pg_cpp_utils_version ();
      DROP FUNCTION IF EXISTS public.pg_cpp_utils_invoice_hash(TEXT, TEXT);
      DROP FUNCTION IF EXISTS public.pg_cpp_utils_number_spellout(VARCHAR(5), FLOAT8, TEXT);
      DROP FUNCTION IF EXISTS public.pg_cpp_utils_currency_spellout(VARCHAR(5), FLOAT8, TEXT, TEXT, FLOAT8, TEXT, TEXT, TEXT, TEXT);
      DROP FUNCTION IF EXISTS public.pg_cpp_utils_format_number(VARCHAR(5), FLOAT8, TEXT);

      DROP TYPE public.pg_cpp_utils_version_record;
      DROP TYPE public.pg_cpp_utils_hash_record;
      DROP TYPE public.pg_cpp_utils_number_spellout_record;
      DROP TYPE public.pg_cpp_utils_format_number_record;

			ALTER TABLE public.currencies DROP COLUMN symbol_at_right;

			DROP TABLE IF EXISTS public.i18n;
      DROP TABLE IF EXISTS public.currency_map;

      DROP FUNCTION IF EXISTS public.currency_spellout (NUMERIC, TEXT, TEXT);
      DROP FUNCTION IF EXISTS public.currency_format (NUMERIC, TEXT, TEXT);

    SQLSQLSQL
  end
end
