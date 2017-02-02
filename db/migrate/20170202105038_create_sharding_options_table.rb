class CreateShardingOptionsTable < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE TABLE sharding.options (
        "sanity_checks" BOOLEAN NOT NULL DEFAULT FALSE,
        "delete_records" BOOLEAN NOT NULL DEFAULT FALSE
      );
    SQL

    execute <<-'SQL'
      INSERT INTO sharding.options VALUES (FALSE, FALSE);
    SQL

    execute %Q[CREATE RULE sharding_options_protect_delete AS ON DELETE TO sharding.options DO INSTEAD NOTHING;]
    execute %Q[CREATE RULE sharding_options_protect_insert AS ON INSERT TO sharding.options DO INSTEAD NOTHING;]

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.get_option(
        IN option_name TEXT
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        _value BOOLEAN;
        _query TEXT;
      BEGIN
        EXECUTE format($$SELECT %1$I FROM sharding.options LIMIT 1;$$, option_name) INTO _value;
        RETURN _value;
      EXCEPTION WHEN undefined_column THEN
        RAISE EXCEPTION
          USING MESSAGE = format($$Sharding configuration option '%1$s' does not exist! Available options: %2$s.$$
            , option_name
            , (SELECT string_agg(a.attname, ', ') FROM  pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON a.attrelid = c.oid AND c.oid = 'sharding.options'::regclass::oid WHERE a.attnum > 0 AND NOT a.attisdropped)
          );
      END;
      $BODY$ LANGUAGE 'plpgsql' STABLE;
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.set_option(
        IN option_name TEXT,
        IN option_value BOOLEAN
      )
      RETURNS BOOLEAN AS $BODY$
      DECLARE
        _value BOOLEAN;
        _query TEXT;
      BEGIN
        EXECUTE format($$UPDATE sharding.options SET %1$I = %2$L RETURNING %1$I;$$, option_name, option_value) INTO _value;
        RETURN _value;
      EXCEPTION WHEN undefined_column THEN
        RAISE EXCEPTION
          USING MESSAGE = format($$Sharding configuration option '%1$s' does not exist! Available options: %2$s.$$
            , option_name
            , (SELECT string_agg(a.attname, ', ') FROM  pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON a.attrelid = c.oid AND c.oid = 'sharding.options'::regclass::oid WHERE a.attnum > 0 AND NOT a.attisdropped)
          );
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
  end

  def down
    execute %Q[DROP FUNCTION IF EXISTS sharding.get_option(TEXT);]
    execute %Q[DROP FUNCTION IF EXISTS sharding.set_option(TEXT, BOOLEAN);]
    execute %Q[DROP TABLE sharding.options;]
  end
end
