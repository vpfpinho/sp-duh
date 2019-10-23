class CleanUpNotNeededShardingFunctions < ActiveRecord::Migration
  def up
  	execute <<-'SQL'
			DROP FUNCTION IF EXISTS sharding.create_safety_triggers_for_sharded_companies();
  	SQL
  end

  def down
  	execute <<-'SQL'
			CREATE OR REPLACE FUNCTION sharding.create_safety_triggers_for_sharded_companies()
			RETURNS VOID AS $BODY$
			DECLARE
			  _table_name TEXT;
			  query TEXT;
			BEGIN

			  FOR _table_name IN (
			    SELECT c.table_name
			    FROM information_schema.columns c
			      JOIN information_schema.tables t
			        ON c.table_schema = t.table_schema
			          AND c.table_name = t.table_name
			          AND t.table_type = 'BASE TABLE'
			    WHERE c.column_name = 'company_id'
			      AND c.table_schema = 'public'
			      AND NOT ( sharding.get_auxiliary_table_information()->'unsharded_tables' ? c.table_name )
			  ) LOOP
			    -- Legacy trigger name
			    query := format('DROP TRIGGER IF EXISTS trg_prevent_insert_or_update_on_sharded_companies ON public.%1$I CASCADE', _table_name);
			    RAISE NOTICE 'query: %', query;
			    EXECUTE query;

			    -- New trigger name
			    query := format('DROP TRIGGER IF EXISTS trg_prevent_changes_on_sharded_tables_for_sharded_companies ON public.%1$I CASCADE', _table_name);
			    RAISE NOTICE 'query: %', query;
			    EXECUTE query;

			    query := format($$
			      CREATE TRIGGER trg_prevent_changes_on_sharded_tables_for_sharded_companies
			        BEFORE INSERT OR UPDATE OR DELETE ON public.%1$I
			        FOR EACH ROW
			        EXECUTE PROCEDURE sharding.trf_prevent_changes_on_sharded_tables_for_sharded_companies();
			    $$, _table_name);
			    RAISE NOTICE 'query: %', query;
			    EXECUTE query;
			  END LOOP;
			END;
			$BODY$ LANGUAGE 'plpgsql';
  	SQL
  end
end
