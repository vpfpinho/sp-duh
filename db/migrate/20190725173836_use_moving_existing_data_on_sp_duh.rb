class UseMovingExistingDataOnSpDuh < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_prevent_changes_on_sharded_tables_for_sharded_companies()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        _company_id    integer;
        _comp          record; 
        _current_cluster integer;
      BEGIN

        IF sharding.moving_existing_data() THEN
          RETURN CASE TG_OP WHEN 'DELETE' THEN OLD ELSE NEW END;
        END IF;

        EXECUTE 'SELECT ($1).company_id::integer' INTO _company_id USING (CASE TG_OP WHEN 'DELETE' THEN OLD ELSE NEW END);

        SELECT use_sharded_company,cluster FROM public.companies WHERE id = _company_id INTO _comp;
        IF _comp.use_sharded_company THEN
          RAISE restrict_violation
            USING MESSAGE = format('Company %1$L has already been sharded, can''t INSERT or UPDATE or DELETE records on unsharded tables' , _company_id),
                  TABLE = TG_TABLE_NAME;
        END IF;

        SHOW cloudware.cluster INTO _current_cluster;
        IF _comp.cluster != _current_cluster THEN
          RAISE restrict_violation
            USING MESSAGE = format('Company %1$L is on another cluster, can''t INSERT or UPDATE or DELETE records from cluster %2$s' , _company_id, _comp.cluster),
                  TABLE = TG_TABLE_NAME;
        END IF;

        RETURN CASE TG_OP WHEN 'DELETE' THEN OLD ELSE NEW END;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
  end

  def down
    execute <<-'SQL'
      CREATE OR REPLACE FUNCTION sharding.trf_prevent_changes_on_sharded_tables_for_sharded_companies()
      RETURNS TRIGGER AS $BODY$
      DECLARE
        _stack         text;
        _company_id    integer;
        _comp          record; 
        _current_cluster integer;
      BEGIN

        GET DIAGNOSTICS _stack = PG_CONTEXT;
        IF _stack ~ 'sharding\.trf_shard_existing_data()' THEN
          RETURN CASE TG_OP WHEN 'DELETE' THEN OLD ELSE NEW END;
        END IF;

        EXECUTE 'SELECT ($1).company_id::integer' INTO _company_id USING (CASE TG_OP WHEN 'DELETE' THEN OLD ELSE NEW END);

        SELECT use_sharded_company,cluster FROM public.companies WHERE id = _company_id INTO _comp;
        IF _comp.use_sharded_company THEN
          RAISE restrict_violation
            USING MESSAGE = format('Company %1$L has already been sharded, can''t INSERT or UPDATE or DELETE records on unsharded tables' , _company_id),
                  TABLE = TG_TABLE_NAME;
        END IF;

        SHOW cloudware.cluster INTO _current_cluster;
        IF _comp.cluster != _current_cluster THEN
          RAISE restrict_violation
            USING MESSAGE = format('Company %1$L is on another cluster, can''t INSERT or UPDATE or DELETE records from cluster %2$s' , _company_id, _comp.cluster),
                  TABLE = TG_TABLE_NAME;
        END IF;

        RETURN CASE TG_OP WHEN 'DELETE' THEN OLD ELSE NEW END;
      END;
      $BODY$ LANGUAGE 'plpgsql';
    SQL
  end
end
