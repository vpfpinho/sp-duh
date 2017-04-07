class CreateCommonGetTablespaceNameFunction < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION common.get_tablespace_name(
        IN a_schema_name TEXT
      )
      RETURNS TEXT AS $BODY$
      DECLARE
        _tablespace_name TEXT;
      BEGIN
        SELECT regexp_replace(a_schema_name, '^pt\d{6}(\d{3}).*', 'tablespace_\1') AS tablespacename
          INTO _tablespace_name;

        RETURN _tablespace_name;
      END;
      $BODY$ LANGUAGE 'plpgsql' IMMUTABLE;
    SQL
  end

  def down
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS common.get_tablespace_name(TEXT);
    SQL
  end
end
