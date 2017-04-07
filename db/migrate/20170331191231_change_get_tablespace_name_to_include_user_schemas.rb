class ChangeGetTablespaceNameToIncludeUserSchemas < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS common.get_tablespace_name(TEXT);

      CREATE OR REPLACE FUNCTION common.get_tablespace_name(
        IN a_schema_name TEXT
      )
      RETURNS TEXT AS $BODY$
      DECLARE
        _tablespace_name TEXT;
      BEGIN

        IF left(a_schema_name,4) = 'user' THEN
          -- last 3 digits from user id, left padded with zeroes
          _tablespace_name := 'tablespace_' || lpad( split_part(a_schema_name,'_',2), 3, '0');
        ELSIF left(a_schema_name,11) = 'pt999999990' THEN
          -- last 3 digits from id, left padded with zeroes
          _tablespace_name := 'tablespace_' || lpad( regexp_replace(a_schema_name,'^pt\d{9}.*?(\d{1,3})$','\1'), 3, '0');
        ELSE
          -- last 3 digits from tax_registration_number
          _tablespace_name := 'tablespace_' || substr(a_schema_name,9,3);
        END IF;

        RETURN _tablespace_name;
      END;
      $BODY$ LANGUAGE 'plpgsql' STABLE STRICT;
    SQL
  end

  def down
    execute %Q[
      DROP FUNCTION IF EXISTS common.get_tablespace_name(TEXT);
    ]
  end
end

