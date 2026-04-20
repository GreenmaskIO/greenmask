-- Copyright 2025 Greenmask
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

-- =============================================================================
-- E2E test schema for greenmask MySQL dump / restore tests.
--
-- Covers all three dump sections:
--   pre-data  — CREATE TABLE definitions
--   data      — INSERT seed rows
--   post-data — triggers and stored routines
-- =============================================================================

-- -----------------------------------------------------------------------------
-- testdb (default database, created by the MySQL container env var)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS test_table (
  id   INT          NOT NULL AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS excluded_table (
  id   INT          NOT NULL AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS data_excluded_table (
  id   INT          NOT NULL AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------------------------------
-- other_db
-- -----------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS other_db;

CREATE TABLE IF NOT EXISTS other_db.other_table (
  id   INT          NOT NULL AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------------------------------
-- Seed data
-- -----------------------------------------------------------------------------

INSERT INTO test_table          (name) VALUES ('test1'), ('test2');
INSERT INTO excluded_table      (name) VALUES ('ex1');
INSERT INTO data_excluded_table (name) VALUES ('dex1');
INSERT INTO other_db.other_table (name) VALUES ('other1');

-- -----------------------------------------------------------------------------
-- Post-data: triggers and stored routines (testdb)
-- -----------------------------------------------------------------------------

DROP TRIGGER IF EXISTS trg_test_table_before_insert;

DELIMITER ;;

CREATE TRIGGER trg_test_table_before_insert
  BEFORE INSERT ON test_table
  FOR EACH ROW
  BEGIN
    IF NEW.name = '' THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'name must not be empty';
    END IF;
  END;;

DELIMITER ;

DROP PROCEDURE IF EXISTS get_test_table_count;

DELIMITER ;;

CREATE PROCEDURE get_test_table_count(OUT row_count INT)
  BEGIN
    SELECT COUNT(*) INTO row_count FROM test_table;
  END;;

DELIMITER ;
