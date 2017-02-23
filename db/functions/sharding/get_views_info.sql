DROP FUNCTION IF EXISTS sharding.get_views_info(TEXT, TEXT);

CREATE OR REPLACE FUNCTION sharding.get_views_info(
  schema_name             TEXT DEFAULT 'public',
  prefix                  TEXT DEFAULT ''
)
RETURNS TABLE (
  object_name             TEXT,
  qualified_object_name   TEXT,
  independent             BOOLEAN,
  definition              TEXT
) AS $BODY$
DECLARE
  all_objects_data        JSONB;
BEGIN

  EXECUTE FORMAT('
    SELECT json_object(array_agg(dependent_view), array_agg(depends_on))::JSONB
    FROM (
      SELECT
        dependent_view.relname::TEXT AS dependent_view,
        array_agg(source_view.relname)::TEXT AS depends_on
      FROM pg_depend
        JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
        JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
        JOIN pg_class as source_view ON pg_depend.refobjid = source_view.oid
        JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
        JOIN pg_namespace source_ns ON source_ns.oid = source_view.relnamespace
      WHERE source_ns.nspname = %1$L
        AND dependent_ns.nspname = %1$L
        AND source_view.relname != dependent_view.relname
        AND source_view.relname ILIKE ''%2$s%%''
        AND dependent_view.relname ILIKE ''%2$s%%''
        AND source_view.relkind = ''v''
      GROUP by dependent_view.relname
    ) views_dependencies;
  ', schema_name, prefix)
  INTO all_objects_data;

  RETURN QUERY EXECUTE FORMAT('
    SELECT
      v.viewname::TEXT AS object_name,
      format(''%%1$I.%%2$I'', v.schemaname, v.viewname) AS qualified_object_name,
      CASE WHEN NOT %3$L ? v.viewname THEN true ELSE false END AS independent,
      pg_catalog.pg_get_viewdef(c.oid) AS definition
    FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_catalog.pg_views v ON c.oid = (v.schemaname || ''.'' || v.viewname)::regclass::oid
    WHERE n.nspname = %1$L
      AND v.viewname ILIKE ''%2$s%%''
  ', schema_name, prefix, all_objects_data);

END;
$BODY$ LANGUAGE 'plpgsql';
