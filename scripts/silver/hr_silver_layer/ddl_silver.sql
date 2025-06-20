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

DROP VIEW IF EXISTS gold.dim_employee_descriptions CASCADE;
DROP VIEW IF EXISTS gold.dim_employee_training_description;
DROP VIEW IF EXISTS gold.dim_employee_safety_manhours_description;
DROP VIEW IF EXISTS gold.dim_occupational_safety_health;
DROP VIEW IF EXISTS gold.dim_employee_parental_leave_description;

DROP FUNCTION IF EXISTS gold.func_employee_summary_yearly;
DROP FUNCTION IF EXISTS gold.func_hr_rate_summary_yearly;
DROP FUNCTION IF EXISTS gold.func_training_summary;
DROP FUNCTION IF EXISTS gold.func_safety_workdata_summary;
DROP FUNCTION IF EXISTS gold.func_occupational_safety_health_summary;
DROP FUNCTION IF EXISTS gold.func_parental_leave_summary_yearly;

DROP FUNCTION IF EXISTS gold.func_employee_summary_monthly;
DROP FUNCTION IF EXISTS gold.func_hr_rate_summary_monthly;
DROP FUNCTION IF EXISTS gold.func_parental_leave_summary_monthly;

DROP TABLE IF EXISTS silver.hr_safety_workdata;
DROP TABLE IF EXISTS silver.hr_occupational_safety_health;
DROP TABLE IF EXISTS silver.hr_training;
DROP TABLE IF EXISTS silver.hr_tenure;
DROP TABLE IF EXISTS silver.hr_parental_leave;
DROP TABLE IF EXISTS silver.hr_demographics;

CREATE TABLE silver.hr_demographics (
    employee_id VARCHAR(20) PRIMARY KEY,
    gender VARCHAR(1),
    birthdate TIMESTAMP,
    position_id VARCHAR(2),
    p_np VARCHAR(2),
    company_id VARCHAR(6),
	employment_status VARCHAR(20),
	date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	date_updated TIMESTAMP
);

CREATE TABLE silver.hr_parental_leave (
    parental_leave_id VARCHAR(20) PRIMARY KEY,
    employee_id VARCHAR(20),
    type_of_leave VARCHAR(12),
    date TIMESTAMP,
    days INT,
	end_date TIMESTAMP, -- DERIVED BY ADDING DAYS TO DATE
	months_availed INT, -- DERIVED BY CALCULATING MONTHS AVAILED
	date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	date_updated TIMESTAMP
);


CREATE TABLE silver.hr_tenure (
    employee_id VARCHAR(20),
    start_date TIMESTAMP,
    end_date TIMESTAMP,
	tenure_length NUMERIC(5,2), -- derived by subtracting end date from start date/ used in gold layer to calculate average tenure
	date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	date_updated TIMESTAMP,
	PRIMARY KEY (start_date, employee_id)
);

CREATE TABLE silver.hr_training (
    training_id VARCHAR(20) PRIMARY KEY,
    company_id VARCHAR(10),
    training_title TEXT,
    date TIMESTAMP,
    training_hours INT,
    number_of_participants INT,
    total_training_hours INT, -- derived by multiplying training_hours by number_of_participants
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	date_updated TIMESTAMP
);

CREATE TABLE silver.hr_safety_workdata (
    safety_workdata_id VARCHAR(20) PRIMARY KEY,
    company_id VARCHAR(10),
    contractor TEXT,
    date TIMESTAMP,
    manpower INT,
    manhours INT,
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	date_updated TIMESTAMP
);

CREATE TABLE silver.hr_occupational_safety_health (
    osh_id VARCHAR(20) PRIMARY KEY,
    company_id VARCHAR(10),
    workforce_type TEXT,
    lost_time BOOLEAN,
    date TIMESTAMP,
    incident_type TEXT,
    incident_title TEXT,
    incident_count INT,
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	date_updated TIMESTAMP
);

ALTER TABLE silver.hr_parental_leave ADD FOREIGN KEY (employee_id) REFERENCES silver.hr_demographics(employee_id);
ALTER TABLE silver.hr_tenure ADD FOREIGN KEY (employee_id) REFERENCES silver.hr_demographics(employee_id);
ALTER TABLE silver.hr_training ADD FOREIGN KEY (company_id) REFERENCES ref.company_main(company_id);
ALTER TABLE silver.hr_safety_workdata ADD FOREIGN KEY (company_id) REFERENCES ref.company_main(company_id);
ALTER TABLE silver.hr_occupational_safety_health ADD FOREIGN KEY (company_id) REFERENCES ref.company_main(company_id);