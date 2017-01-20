class AddFunctionToConvertSequencesToSchemaQualified < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.convert_sequences_to_schema_qualified(p_schema_name TEXT)
      RETURNS INTEGER AS $BODY$
      DECLARE
        _query TEXT;
        _row RECORD;
        _sequence_name TEXT;
        _sequence_next_value BIGINT;
        _total_affected INTEGER;
      BEGIN
        RAISE NOTICE 'Converting sequences on schema % to schema qualified', p_schema_name;

        _total_affected := 0;

        FOR _row IN SELECT * FROM (
            SELECT
              t.schemaname,
              t.tablename,
              a.attname AS columnname,
              (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) AS default_value
            FROM pg_catalog.pg_attribute a
              JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
              JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              JOIN pg_catalog.pg_tables t ON c.oid = (t.schemaname || '.' || t.tablename)::regclass::oid
            WHERE a.attnum > 0
              AND NOT a.attisdropped
              AND n.nspname = p_schema_name
          ) columns
          WHERE default_value ~* '^nextval\(''[^.]+'''
        LOOP
          _total_affected := _total_affected + 1;
          _sequence_name := regexp_replace(_row.default_value, '^nextval\(''(.*?)''.*$', '\1');

          RAISE DEBUG '-> %.%.%', _row.schemaname, _row.tablename, _row.columnname; --, _sequence_name, _row.schemaname, _sequence_name;

          _query := format('CREATE SEQUENCE %1$s.%2$I;', _row.schemaname, _sequence_name);
          RAISE DEBUG '_query: %', _query;
          EXECUTE _query;
          _query := format('ALTER SEQUENCE %1$s.%2$I OWNED BY %1$s.%3$I.%4$I;', _row.schemaname, _sequence_name, _row.tablename, _row.columnname);
          RAISE DEBUG '_query: %', _query;
          EXECUTE _query;
          _query := format('SELECT last_value FROM public.%1$I', _sequence_name);
          RAISE DEBUG '_query: %', _query;
          EXECUTE _query INTO _sequence_next_value;
          _query := format('ALTER SEQUENCE %1$s.%2$I RESTART WITH %3$s', _row.schemaname, _sequence_name, _sequence_next_value);
          RAISE DEBUG '_query: %', _query;
          EXECUTE _query;
          _query := format('ALTER TABLE %1$s.%2$I ALTER COLUMN %3$I SET DEFAULT nextval(''%1$s.%4$I''::regclass)', _row.schemaname, _row.tablename, _row.columnname, _sequence_name);
          RAISE DEBUG '_query: %', _query;
          EXECUTE _query;
        END LOOP;

        RETURN _total_affected;
      END;
      $BODY$ language plpgsql;
    SQL
  end

  def down
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS sharding.convert_sequences_to_schema_qualified(TEXT);
    SQL
  end
end
