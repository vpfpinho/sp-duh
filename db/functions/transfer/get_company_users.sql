DROP FUNCTION IF EXISTS transfer.get_company_users(bigint);
CREATE OR REPLACE FUNCTION transfer.get_company_users(
  company_id    bigint
) RETURNS TABLE (
  user_id       bigint,
  full_name     text,
  type          text
) AS $BODY$
DECLARE
  query         text;
BEGIN

  query = FORMAT('

    SELECT u.id::bigint, u.full_name::text, u.type::text
      FROM public.users u
      WHERE
        u.company_id = %1$L
    UNION
    SELECT u.id::bigint, u.full_name::text, u.type::text
      FROM public.users u
      JOIN public.companies c
        ON u.id = c.accountant_id
      WHERE
        c.id = %1$L

  ', company_id);
  RETURN QUERY EXECUTE query;

END;
$BODY$ LANGUAGE 'plpgsql';