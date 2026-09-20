-- Uses the schema already created by profiles.sql. Small exactness corpus.
CREATE TABLE {{schema}}.empty_rows (row_id bigint PRIMARY KEY, payload text);
CREATE TABLE {{schema}}.one_row (row_id bigint PRIMARY KEY, payload text);
INSERT INTO {{schema}}.one_row VALUES (0, E'null is not NULL\t\\N');
CREATE TABLE {{schema}}.extreme_keys (row_id bigint PRIMARY KEY, payload bytea);
INSERT INTO {{schema}}.extreme_keys VALUES
  (-9223372036854775808, decode('00ff','hex')), (-1, NULL), (0, ''::bytea),
  (1, decode('ff00','hex')), (9223372036854775807, decode('0000','hex'));

CREATE TABLE {{schema}}.nullable_composite (
  row_id bigint NOT NULL, tenant text COLLATE "C" NOT NULL,
  a integer, b text, payload text, PRIMARY KEY (tenant, row_id)
);
INSERT INTO {{schema}}.nullable_composite
SELECT g, CASE g % 3 WHEN 0 THEN 'a' WHEN 1 THEN 'A' ELSE 'я' END,
  CASE WHEN g % 2 = 0 THEN NULL ELSE (g % 5)::integer END,
  CASE WHEN g % 4 < 2 THEN NULL ELSE (g % 7)::text END,
  CASE WHEN g % 9 = 0 THEN NULL ELSE 'value-' || g END
FROM generate_series(0, 255) AS g;

CREATE TABLE {{schema}}.special_values (
  row_id bigint PRIMARY KEY, uuid_value uuid, bytes bytea, exact_value numeric,
  floating double precision, day date, moment timestamptz, plain timestamp,
  document jsonb, string_value text
);
INSERT INTO {{schema}}.special_values VALUES
  (1, '00000000-0000-0000-0000-000000000000', decode('00ff00','hex'),
   9007199254740993.12345678901234567890, 'NaN', 'infinity', 'infinity', '-infinity', '{"null":null}', E'\t\n\\N'),
  (2, 'ffffffff-ffff-ffff-ffff-ffffffffffff', ''::bytea,
   -999999999999999999999.00000000000000000001, 'Infinity', '-infinity', '-infinity', 'infinity', '[]', ''),
  (3, '12345678-1234-1234-1234-123456789abc', NULL,
   0.00000000000000000000000000000000000001, '-Infinity', '0001-01-01 BC', '2020-10-25 02:30:00.123456+01', '2020-01-01 00:00:00.999999', 'true', 'я'),
  (4, NULL, decode('5c4e','hex'), '-0.000', '-0', '2000-02-29', '2020-10-25 02:30:00.123456+02', NULL, '0', NULL),
  (5, NULL, NULL, NULL, '0', NULL, NULL, NULL, NULL, NULL);

CREATE TYPE {{schema}}.declared_order AS ENUM ('zebra', 'alpha', 'middle');
CREATE TABLE {{schema}}.ordered_keys (
  row_id bigint PRIMARY KEY, text_key text COLLATE "C" NOT NULL,
  declared {{schema}}.declared_order, array_key integer[], long_key text
);
INSERT INTO {{schema}}.ordered_keys VALUES
  (1, 'a', 'zebra', ARRAY[1,NULL,3], repeat('a', 1500) || '1'),
  (2, 'A', 'alpha', '[0:2]={1,NULL,3}'::integer[], repeat('a', 1500) || '2'),
  (3, 'я', 'middle', '{}'::integer[], repeat('b', 1500) || '3'),
  (4, '', NULL, NULL, NULL);

CREATE TABLE {{schema}}.mcv_keys (row_id bigint PRIMARY KEY, split_key integer, all_null text);
INSERT INTO {{schema}}.mcv_keys
SELECT g, CASE WHEN g < 900 THEN 0 ELSE g END, NULL FROM generate_series(0,999) AS g;
CREATE TABLE {{schema}}.missing_statistics (row_id bigint PRIMARY KEY, payload text);
ALTER TABLE {{schema}}.missing_statistics ALTER COLUMN row_id SET STATISTICS 0;
INSERT INTO {{schema}}.missing_statistics SELECT g, g::text FROM generate_series(1,1000) AS g;
CREATE TABLE {{schema}}.stale_statistics (row_id bigint PRIMARY KEY, payload text);
INSERT INTO {{schema}}.stale_statistics SELECT g, g::text FROM generate_series(1,1000) AS g;
ANALYZE {{schema}}.stale_statistics;
INSERT INTO {{schema}}.stale_statistics SELECT g, g::text FROM generate_series(2001,2500) AS g;
UPDATE {{schema}}.stale_statistics SET row_id = -row_id WHERE row_id <= 200;

CREATE TABLE {{schema}}.partition_parent (row_id bigint, bucket integer, payload text,
  PRIMARY KEY (bucket, row_id)) PARTITION BY RANGE (bucket);
CREATE TABLE {{schema}}.partition_a PARTITION OF {{schema}}.partition_parent FOR VALUES FROM (0) TO (1);
CREATE TABLE {{schema}}.partition_b PARTITION OF {{schema}}.partition_parent FOR VALUES FROM (1) TO (2);
INSERT INTO {{schema}}.partition_parent
SELECT g, g % 2, 'partition-' || g FROM generate_series(0,199) AS g;
