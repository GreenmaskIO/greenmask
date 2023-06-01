create table test_bool
(
    id  SERIAL PRIMARY KEY,
    at1 BOOLEAN          DEFAULT TRUE,
    at2 BOOLEAN NOT NULL DEFAULT FALSE
);

INSERT INTO test_bool
SELECT
FROM generate_series(1, 100000);
