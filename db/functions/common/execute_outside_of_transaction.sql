-- DROP FUNCTION IF EXISTS common.execute_outside_of_transaction(TEXT);

CREATE OR REPLACE FUNCTION common.execute_outside_of_transaction(query TEXT)
RETURNS VOID AS
$BODY$
DECLARE
    port INTEGER;
    dbname TEXT;
    server_connection TEXT;
    _query TEXT;
BEGIN
    port := (SELECT setting FROM pg_settings WHERE name = 'port');
    dbname := current_database();

    server_connection := format('port=%1$s dbname=%2$s', port, dbname);

    _query := format($Q$
      DO $$
      BEGIN
        %1$s
      END;
      $$;
    $Q$
    , query
    );

    PERFORM public.dblink_exec(server_connection, _query);

    RETURN;
END;
$BODY$ language plpgsql;