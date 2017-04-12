class UseSeparateMetaSchemasPerCompanyInTransfer < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.get_meta_schema_name(bigint);
      CREATE OR REPLACE FUNCTION transfer.get_meta_schema_name(
        company_id    bigint
      ) RETURNS text AS $BODY$
      DECLARE
      BEGIN

        -- Can be global (the same for all companies) or (as now) defined as one meta schema per company
        RETURN FORMAT('_meta_c%1$s_', company_id);

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
  end

  def down
    execute <<-'SQL'
      DROP FUNCTION IF EXISTS transfer.get_meta_schema_name(bigint);
      CREATE OR REPLACE FUNCTION transfer.get_meta_schema_name(
        company_id    bigint
      ) RETURNS text AS $BODY$
      DECLARE
      BEGIN

        -- Now is global (the same for all companies), but can be defined as one meta schema per company
        RETURN '_meta_';

      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
  end
end
