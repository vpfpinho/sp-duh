class AddShardingFuntionGetSchemaName < ActiveRecord::Migration
  def up
    execute <<-'SQL'
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

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.get_schema_name_for_table(
        IN  company_id          INTEGER,
        IN  table_name          TEXT,
        OUT table_schema_name   TEXT)
      RETURNS TEXT AS $BODY$
      DECLARE
        _company_id ALIAS FOR company_id;
        _table_name ALIAS FOR table_name;
      BEGIN

        IF ( sharding.get_auxiliary_table_information()->'unsharded_tables' ? _table_name ) THEN
          table_schema_name := 'public';
        ELSE
          table_schema_name := sharding.get_sharded_schema_name(_company_id);
        END IF;

        RETURN;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL

    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.get_qualified_table_name(
        IN  company_id      INTEGER,
        IN  table_name      TEXT,
        OUT qualified_table TEXT)
      RETURNS TEXT AS $BODY$
      BEGIN

        qualified_table := sharding.get_schema_name_for_table(company_id, table_name) || '.' || _table_name;

        RETURN;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
  end

  def down
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS sharding.get_qualified_table_name(INTEGER, TEXT);
      DROP FUNCTION IF EXISTS sharding.get_schema_name_for_table(INTEGER, TEXT);
      DROP FUNCTION IF EXISTS sharding.get_sharded_schema_name(INTEGER);
    SQL
  end
end
