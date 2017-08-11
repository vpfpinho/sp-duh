class AlterGetI18n < ActiveRecord::Migration
  def up

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS common.get_i18n(TEXT, TEXT);

      CREATE OR REPLACE FUNCTION common.get_i18n(
        key       TEXT,
        locale    TEXT DEFAULT 'pt_PT',
        VARIADIC  a_args text[] DEFAULT '{}'
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

        SELECT formatted FROM pg_cpp_utils_format_message(locale::varchar, _i18n_text::varchar, a_args::text)
        INTO _i18n_text;

        RETURN _i18n_text;
      END;
      $BODY$ LANGUAGE plpgsql IMMUTABLE;
    SQL


  end

  def down

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS common.get_i18n(TEXT, TEXT, VARIADIC);

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
      $BODY$ LANGUAGE plpgsql IMMUTABLE;
    SQL

  end
end
