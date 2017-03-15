DROP FUNCTION IF EXISTS sharding.get_tables_info(TEXT, TEXT);

CREATE OR REPLACE FUNCTION sharding.get_tables_info(
  schema_name             TEXT DEFAULT 'public',
  prefix                  TEXT DEFAULT ''
)
RETURNS TABLE (
  object_name             TEXT,
  qualified_object_name   TEXT,
  columns                 JSONB,
  indexes                 JSONB,
  foreign_keys            JSONB,
  constraints             JSONB,
  triggers                JSONB
) AS $BODY$
DECLARE
BEGIN

  RETURN QUERY EXECUTE FORMAT('
    WITH table_columns AS (
      SELECT
        t.tablename::TEXT AS object_name,
        format(''%%1$I.%%2$I'', t.schemaname, t.tablename) AS qualified_object_name,
        (t.schemaname || ''.'' || t.tablename)::regclass::oid AS table_oid,
        json_agg(json_build_object(
          ''name'', a.attname,
          ''type'', pg_catalog.format_type(a.atttypid, a.atttypmod),
          ''default_value'', (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
          ''is_not_null'', a.attnotnull
        ) ORDER BY a.attnum)::JSONB AS columns
      FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_tables t ON c.oid = (t.schemaname || ''.'' || t.tablename)::regclass::oid
      WHERE a.attnum > 0
        AND NOT a.attisdropped
        AND n.nspname = %1$L
        AND t.tablename ILIKE ''%2$s%%''
      GROUP BY t.schemaname, t.tablename
    ),
    table_indexes AS (
      SELECT
        format(''%%1$I.%%2$I'', t.schemaname, t.tablename) AS qualified_object_name,
        (t.schemaname || ''.'' || t.tablename)::regclass::oid AS table_oid,
        json_agg(json_build_object(
          ''name'', c2.relname,
          ''is_primary'', i.indisprimary,
          ''is_unique'', i.indisunique,
          ''definition'', pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
          ''constraint_definition'', pg_catalog.pg_get_constraintdef(con.oid, true)
        )::JSONB)::JSONB AS indexes
      FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_index i ON c.oid = i.indrelid
        JOIN pg_catalog.pg_class c2 ON i.indexrelid = c2.oid
        LEFT JOIN pg_catalog.pg_constraint con ON (con.conrelid = i.indrelid AND con.conindid = i.indexrelid AND con.contype IN (''p'',''u'',''x''))
        JOIN pg_catalog.pg_tables t ON c.oid = (t.schemaname || ''.'' || t.tablename)::regclass::oid
      WHERE t.schemaname = %1$L
        AND t.tablename ILIKE ''%2$s%%''
      GROUP BY t.schemaname, t.tablename
    ),
    table_foreign_keys AS (
      SELECT
        format(''%%1$I.%%2$I'', t.schemaname, t.tablename) AS qualified_object_name,
        (t.schemaname || ''.'' || t.tablename)::regclass::oid AS table_oid,
        json_agg(json_build_object(
          ''name'', c.conname,
          ''update_action'', c.confupdtype,
          ''delete_action'', c.confdeltype,
          ''definition'', pg_catalog.pg_get_constraintdef(c.oid, true)
        )::JSONB)::JSONB AS foreign_keys
      FROM pg_catalog.pg_constraint c
        LEFT JOIN pg_catalog.pg_tables t ON c.conrelid = (t.schemaname || ''.'' || t.tablename)::regclass::oid
      WHERE c.contype = ''f''
        AND t.schemaname = %1$L
        AND t.tablename ILIKE ''%2$s%%''
      GROUP BY t.schemaname, t.tablename
    ),
    table_constraints AS (
      SELECT
        format(''%%1$I.%%2$I'', t.schemaname, t.tablename) AS qualified_object_name,
        (t.schemaname || ''.'' || t.tablename)::regclass::oid AS table_oid,
        json_agg(json_build_object(
          ''name'', c.conname,
          ''definition'', pg_catalog.pg_get_constraintdef(c.oid, true)
        )::JSONB)::JSONB AS constraints
      FROM pg_catalog.pg_constraint c
        LEFT JOIN pg_catalog.pg_tables t ON c.conrelid = (t.schemaname || ''.'' || t.tablename)::regclass::oid
      WHERE c.contype = ''c''
        AND t.schemaname = %1$L
        AND t.tablename ILIKE ''%2$s%%''
      GROUP BY t.schemaname, t.tablename
    ),
    table_triggers AS (
      SELECT
        format(''%%1$I.%%2$I'', ta.schemaname, ta.tablename) AS qualified_object_name,
        (ta.schemaname || ''.'' || ta.tablename)::regclass::oid AS table_oid,
        json_agg(json_build_object(
          ''name'', t.tgname,
          ''definition'', pg_catalog.pg_get_triggerdef(t.oid, true)
        )::JSONB)::JSONB AS triggers
      FROM pg_catalog.pg_trigger t
        LEFT JOIN pg_catalog.pg_tables ta ON t.tgrelid = (ta.schemaname || ''.'' || ta.tablename)::regclass::oid
      WHERE ta.schemaname = %1$L
        AND ta.tablename ILIKE ''%2$s%%''
        AND (NOT t.tgisinternal OR (t.tgisinternal AND t.tgenabled = ''D''))
        AND t.tgname != ''trg_prevent_insert_or_update_on_sharded_companies'' -- Do not copy the prevent trigger for sharded companies
        -- AND t.tgname !~* ''^trg_vfk(?:i|p)?'' -- Do not copy the virtual foreign key triggers
      GROUP BY ta.schemaname, ta.tablename
    )
    SELECT
      c.object_name,
      c.qualified_object_name,
      c.columns,
      i.indexes,
      fk.foreign_keys,
      ct.constraints,
      trg.triggers
    FROM table_columns c
      LEFT JOIN table_indexes i ON c.table_oid = i.table_oid
      LEFT JOIN table_foreign_keys fk ON c.table_oid = fk.table_oid
      LEFT JOIN table_constraints ct ON c.table_oid = ct.table_oid
      LEFT JOIN table_triggers trg ON c.table_oid = trg.table_oid
  ', schema_name, prefix);

END;
$BODY$ LANGUAGE 'plpgsql';
