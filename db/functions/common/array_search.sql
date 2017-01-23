DROP FUNCTION IF EXISTS common.array_search(ANYELEMENT, ANYARRAY);

CREATE FUNCTION common.array_search(needle ANYELEMENT, haystack ANYARRAY)
RETURNS INT AS $$
    SELECT i
      FROM generate_subscripts($2, 1) AS i
     WHERE $2[i] = $1
  ORDER BY i
$$ LANGUAGE sql STABLE;