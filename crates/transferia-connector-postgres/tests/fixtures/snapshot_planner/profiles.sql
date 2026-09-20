-- Tokens are expanded only by the evaluator: schema is a quoted identifier,
-- rows is a validated positive integer divisible by 1000. No DROP or TRUNCATE.
CREATE SCHEMA {{schema}};
CREATE TABLE {{schema}}.uniform_numeric (
  row_id bigint PRIMARY KEY, n1 bigint, n2 bigint, n3 bigint,
  n4 bigint, n5 bigint, n6 bigint, n7 bigint
);
INSERT INTO {{schema}}.uniform_numeric
SELECT g, g # 1, g # 2, g # 3, g # 4, g # 5, g # 6, g # 7
FROM generate_series(0::bigint, {{rows}} - 1) AS g;

CREATE TABLE {{schema}}.nullable_logs (
  row_id bigint NOT NULL, tenant integer NOT NULL, at timestamptz NOT NULL,
  message text, host text, lag double precision,
  n01 bigint, n02 bigint, n03 bigint, n04 bigint, n05 bigint,
  n06 bigint, n07 bigint, n08 bigint, n09 bigint, n10 bigint,
  n11 bigint, n12 bigint, n13 bigint, n14 bigint, n15 bigint,
  n16 bigint, n17 bigint, n18 bigint, n19 bigint, n20 bigint,
  PRIMARY KEY (tenant, row_id, at)
);
INSERT INTO {{schema}}.nullable_logs
SELECT g, (g % 17)::integer, timestamptz '2020-01-01 00:00:00+00' + g * interval '1 microsecond',
  CASE WHEN g % 7 = 0 THEN NULL ELSE repeat('log ' || (g % 97)::text || ' ', 12) END,
  CASE WHEN g % 11 = 0 THEN NULL ELSE 'host-' || (g % 13)::text END,
  CASE WHEN g % 5 = 0 THEN NULL ELSE (g % 123)::double precision / 10 END,
  g, g+1, g+2, g+3, g+4, g+5, g+6, g+7, g+8, g+9,
  g+10, g+11, g+12, g+13, g+14, g+15, g+16, g+17, g+18, g+19
FROM generate_series(0::bigint, {{rows}} - 1) AS g;

-- Multiplication by 104729 permutes the integer domain when rows is a power
-- of ten. For arbitrary accepted sizes, the complete PK still includes row_id.
DO $fixture$
DECLARE columns_sql text; values_sql text;
BEGIN
  SELECT string_agg(format('c%s bigint', n), ', ' ORDER BY n),
         string_agg(format('(g # %s)', n), ', ' ORDER BY n)
  INTO columns_sql, values_sql FROM generate_series(1, 101) AS n;
  EXECUTE 'CREATE TABLE {{schema}}.wide_permuted (row_id bigint NOT NULL, watch_id bigint NOT NULL, at timestamptz NOT NULL, payload text, '
    || columns_sql || ', PRIMARY KEY (watch_id, row_id, at))';
  EXECUTE 'INSERT INTO {{schema}}.wide_permuted SELECT g, (g * 104729) % {{rows}}, timestamptz ''2020-01-01 00:00:00+00'' + g * interval ''1 microsecond'', repeat(''wide'', 8), '
    || values_sql || ' FROM generate_series(0::bigint, {{rows}} - 1) AS g';
END
$fixture$;

CREATE TABLE {{schema}}.key_skew (LIKE {{schema}}.uniform_numeric INCLUDING ALL);
INSERT INTO {{schema}}.key_skew
SELECT CASE WHEN row_id < {{rows}} * 9 / 10 THEN row_id
            ELSE {{rows}} * 100 + (row_id - {{rows}} * 9 / 10) * 1000 END,
       n1, n2, n3, n4, n5, n6, n7
FROM {{schema}}.uniform_numeric ORDER BY row_id;

CREATE TABLE {{schema}}.empty_prefix (LIKE {{schema}}.uniform_numeric INCLUDING ALL);
INSERT INTO {{schema}}.empty_prefix SELECT * FROM {{schema}}.uniform_numeric ORDER BY row_id;
DELETE FROM {{schema}}.empty_prefix WHERE row_id < {{rows}} * 9 / 10;

CREATE TABLE {{schema}}.toast_tail (row_id bigint PRIMARY KEY, payload text NOT NULL);
ALTER TABLE {{schema}}.toast_tail ALTER COLUMN payload SET STORAGE EXTERNAL;
INSERT INTO {{schema}}.toast_tail
SELECT g, repeat('x', CASE WHEN g >= {{rows}} * 99 / 100 THEN 65536 ELSE 32 END)
FROM generate_series(0::bigint, {{rows}} - 1) AS g;
-- VACUUM must be a separate protocol command outside a transaction, supplied
-- by the fixture runner after this script. ANALYZE follows it separately.
