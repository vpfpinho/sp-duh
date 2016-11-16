class AddGetShardingSchemaFromSchemaName < ActiveRecord::Migration
  def up

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.get_sharded_schema_name (
        IN  company_schema      TEXT,
        OUT sharded_schema      TEXT)
      RETURNS TEXT AS $BODY$
      DECLARE
        _company_schema ALIAS FOR company_schema;
        _sharded_schema ALIAS FOR sharded_schema;
      BEGIN

        SELECT CASE WHEN c.use_sharded_company THEN c.schema_name ELSE 'public' END
          FROM public.companies c
          WHERE c.schema_name = _company_schema
        INTO STRICT _sharded_schema;

        RETURN;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS sharding.get_sharded_schema_name(INTEGER);
      CREATE OR REPLACE FUNCTION sharding.get_sharded_schema_name (
        IN  company_id          INTEGER,
        OUT sharded_schema      TEXT)
      RETURNS TEXT AS $BODY$
      DECLARE
        _company_id     ALIAS FOR company_id;
        _sharded_schema ALIAS FOR sharded_schema;
      BEGIN

        SELECT CASE WHEN c.use_sharded_company THEN c.schema_name ELSE 'public' END
          FROM public.companies c
          WHERE c.id = _company_id
        INTO STRICT _sharded_schema;

        RETURN;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

  end

  def down

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS sharding.get_sharded_schema_name(TEXT);
    SQL

    execute <<-'SQL'
      DROP FUNCTION IF EXISTS sharding.get_sharded_schema_name(INTEGER);
      CREATE OR REPLACE FUNCTION sharding.get_sharded_schema_name (
        IN  company_id          INTEGER,
        OUT schema_name         TEXT)
      RETURNS TEXT AS $BODY$
      DECLARE
        _company_id  ALIAS FOR company_id;
        _schema_name ALIAS FOR schema_name;
      BEGIN

        SELECT CASE WHEN c.use_sharded_company THEN c.schema_name ELSE 'public' END
          FROM public.companies c
          WHERE c.id = _company_id
        INTO STRICT _schema_name;

        RETURN;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

  end
end
