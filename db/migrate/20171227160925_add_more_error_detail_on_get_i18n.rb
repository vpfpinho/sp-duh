class AddMoreErrorDetailOnGetI18n < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION common.get_i18n(
        key       TEXT,
        locale    TEXT DEFAULT 'pt_PT',
        VARIADIC a_args text[] DEFAULT NULL
      )
      RETURNS text AS $BODY$
      DECLARE
        _i18n_text text;
      BEGIN

        -- RAISE DEBUG '====>  common.get_i18n(''%'',''%'',%)', key, locale, a_args;

        IF left(locale,2) NOT IN ('pt','en','es','fr','de','it') THEN
            RAISE EXCEPTION 'unknown language in i18n table'
                USING ERRCODE = 'JA001';
        END IF;

        EXECUTE 'SELECT ' || left(locale,2) || ' FROM public.i18n WHERE key = $1'
        USING key
        INTO _i18n_text;

        locale := CASE
          WHEN locale = 'pt' THEN 'pt_PT'
          WHEN locale = 'en' THEN 'en_GB'
          ELSE locale
        END;

        IF a_args IS NULL THEN
          SELECT formatted FROM public.pg_cpp_utils_format_message(locale::varchar, _i18n_text::varchar, '{}')
          INTO _i18n_text;
        ELSE
          SELECT formatted FROM public.pg_cpp_utils_format_message(locale::varchar, _i18n_text::varchar, VARIADIC a_args)
          INTO _i18n_text;
        END IF;

        RETURN _i18n_text;

      EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'common.get_i18n(''%'',''%'',%) - _i18n_text:[%] - %', key, locale, a_args, _i18n_text, SQLERRM USING ERRCODE = SQLSTATE;
      END;
      $BODY$ LANGUAGE plpgsql IMMUTABLE;
    SQL
  end

  def down
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION common.get_i18n(
        key       TEXT,
        locale    TEXT DEFAULT 'pt_PT',
        VARIADIC a_args text[] DEFAULT NULL
      )
      RETURNS text AS $BODY$
      DECLARE
        _i18n_text text;
      BEGIN

        IF left(locale,2) NOT IN ('pt','en','es','fr','de','it') THEN
            RAISE EXCEPTION 'unknown language in i18n table'
                USING ERRCODE = 'JA001';
        END IF;

        EXECUTE 'SELECT ' || left(locale,2) || ' FROM public.i18n WHERE key = $1'
        USING key
        INTO _i18n_text;

        locale := CASE
          WHEN locale = 'pt' THEN 'pt_PT'
          WHEN locale = 'en' THEN 'en_GB'
          ELSE locale
        END;

        IF a_args IS NULL THEN
          SELECT formatted FROM public.pg_cpp_utils_format_message(locale::varchar, _i18n_text::varchar, '{}')
          INTO _i18n_text;
        ELSE
          SELECT formatted FROM public.pg_cpp_utils_format_message(locale::varchar, _i18n_text::varchar, VARIADIC a_args)
          INTO _i18n_text;
        END IF;

        RETURN _i18n_text;
      END;
      $BODY$ LANGUAGE plpgsql IMMUTABLE;
    SQL
  end
end
