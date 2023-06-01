create schema typetest;

create table typetest.test_bool
(
    id  SERIAL PRIMARY KEY,
    at1 BOOLEAN          DEFAULT TRUE,
    at2 BOOLEAN NOT NULL DEFAULT FALSE
);

INSERT INTO typetest.test_bool
SELECT
FROM generate_series(1, 100000);


create table typetest.test_int
(
    id  SERIAL PRIMARY KEY,
    at1 INT2          DEFAULT 1,
    at2 INT4          DEFAULT 2,
    at3 INT8          DEFAULT 3,
    at4 INT2 NOT NULL DEFAULT 4,
    at5 INT4 NOT NULL DEFAULT 5,
    at6 INT8 NOT NULL DEFAULT 6
);

INSERT INTO typetest.test_int
SELECT
FROM generate_series(1, 100000);
