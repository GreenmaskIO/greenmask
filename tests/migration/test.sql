-- Copyright 2023 Greenmask
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

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


CREATE DOMAIN us_postal_code AS TEXT
    CHECK (
        VALUE ~ '^\d{5}$'
    OR VALUE ~ '^\d{5}-\d{4}$'
    );
CREATE DOMAIN us_postal_code_v2 AS us_postal_code;


ALTER TABLE bookings.flights ADD COLUMN post_code us_postal_code_v2 DEFAULT '12345' NOT NULL ;

CREATE DOMAIN int_dom AS INT
    CHECK ( VALUE > 10 AND VALUE < 100);

CREATE DOMAIN int_dom_v2 AS int_dom;

ALTER TABLE bookings.flights ADD COLUMN test_dom int_dom_v2 DEFAULT 11 NOT NULL ;

select * from bookings.flights;


DROP DATABASE demo_restore;
CREATE DATABASE demo_restore;
