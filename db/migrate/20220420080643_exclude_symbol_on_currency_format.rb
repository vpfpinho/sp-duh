class ExcludeSymbolOnCurrencyFormat < ActiveRecord::Migration
  def up
    # SQLFILE: sp-duh/db/functions/common/currency_format.sql
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS public.currency_format;
      CREATE OR REPLACE FUNCTION public.currency_format (a_value NUMERIC, a_currency_iso_code TEXT, a_locale TEXT, a_include_symbol BOOLEAN DEFAULT TRUE)
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
          IF a_include_symbol THEN
              IF symbol_at_right
              THEN
                  pattern := pattern || ' ' || symbol;
              ELSE
                  pattern := symbol || ' ' || pattern;
              END IF;
          END IF;
          SELECT formatted FROM public.pg_cpp_utils_format_number(a_locale, ROUND(a_value, nodd), pattern) INTO formatted_result;

          RETURN formatted_result;
      END;
      $BODY$
      LANGUAGE 'plpgsql' IMMUTABLE;
    SQL
  end

  def down
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS public.currency_format;
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
    SQL
  end
end
