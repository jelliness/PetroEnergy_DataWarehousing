/*
===============================================================================
DDL Script: Create Silver Tables for HR demograpics, parental leave, tenure, 
training, and safety values
===============================================================================
Script Purpose:
    This script creates a table in silver layer for hr values per year.
    Each table will be dropped, then recreates a new table.
===============================================================================
*/

CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.hr_safety;
DROP TABLE IF EXISTS silver.hr_training;
DROP TABLE IF EXISTS silver.hr_tenure;
DROP TABLE IF EXISTS silver.hr_parental_leave;
DROP TABLE IF EXISTS silver.hr_demographics;
DROP TABLE IF EXISTS silver.hr_position;

CREATE TABLE silver.hr_demographics (
    employee_id VARCHAR(20) PRIMARY KEY,
    gender VARCHAR(1),
    birthdate TIMESTAMP,
	--age INT, -- DERIVED VALUES FOR CALCULATING AGE
    position_id VARCHAR(2),
	--position_name VARCHAR(20),
    p_np VARCHAR(2),
    company_id VARCHAR(6),
	employment_status VARCHAR(20),
	date_created TIMESTAMP,
	date_updated TIMESTAMP
);

CREATE TABLE silver.hr_position (
	position_id VARCHAR(2) PRIMARY KEY,
	position_name VARCHAR(20)
);

CREATE TABLE silver.hr_parental_leave (
    employee_id VARCHAR(20),
    type_of_leave VARCHAR(12),
    date TIMESTAMP,
    days INT,
	end_date TIMESTAMP, -- DERIVED BY ADDING DAYS TO DATE
	months_availed INT, -- DERIVED BY CALCULATING MONTHS AVAILED
	date_created TIMESTAMP,
	date_updated TIMESTAMP,
	PRIMARY KEY (date, employee_id)
);


CREATE TABLE silver.hr_tenure (
    employee_id VARCHAR(20) PRIMARY KEY,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
	-- is_active BOOLEAN, -- DERIVED, CHECK IF END DATE IS NULL
	tenure_length NUMERIC(5,2), -- derived by subtracting end date from start date/ used in gold layer to calculate average tenure
	date_created TIMESTAMP,
	date_updated TIMESTAMP
);


CREATE TABLE silver.hr_training (
    employee_id VARCHAR(20) PRIMARY KEY,
    hours INT,
    month_start INT,
    year_start INT,
    position_id VARCHAR(2),
	date_created TIMESTAMP,
	date_updated TIMESTAMP
);

CREATE TABLE silver.hr_safety (
    employee_id VARCHAR(20) PRIMARY KEY,
    company_id VARCHAR(8),
    date TIMESTAMP,
    type_of_accident VARCHAR(50),
    safety_man_hours INT,
	date_created TIMESTAMP,
	date_updated TIMESTAMP
);

ALTER TABLE silver.hr_parental_leave ADD FOREIGN KEY (employee_id) REFERENCES silver.hr_demographics(employee_id);
ALTER TABLE silver.hr_tenure ADD FOREIGN KEY (employee_id) REFERENCES silver.hr_demographics(employee_id);
ALTER TABLE silver.hr_training ADD FOREIGN KEY (employee_id) REFERENCES silver.hr_demographics(employee_id);
ALTER TABLE silver.hr_safety ADD FOREIGN KEY (employee_id) REFERENCES silver.hr_demographics(employee_id);