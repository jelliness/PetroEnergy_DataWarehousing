/*
===============================================================================
DDL Script: Create Bronze Tables for HR demograpics, parental leave, tenure, 
training, and safety values
===============================================================================
Script Purpose:
    This script creates a table in bronze layer for hr values per year.
    Each table will be dropped, then recreates a new table.
===============================================================================
*/

CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.hr_safety_workdata;
DROP TABLE IF EXISTS bronze.hr_occupational_safety_health;
DROP TABLE IF EXISTS bronze.hr_training;
DROP TABLE IF EXISTS bronze.hr_tenure;
DROP TABLE IF EXISTS bronze.hr_parental_leave;
DROP TABLE IF EXISTS bronze.hr_demographics;

DROP TABLE IF EXISTS bronze.hr_safety_workdata_staging;
DROP TABLE IF EXISTS bronze.hr_occupational_safety_health_staging;
DROP TABLE IF EXISTS bronze.hr_training_staging;
DROP TABLE IF EXISTS bronze.hr_parental_leave_staging;


CREATE TABLE bronze.hr_demographics (
    employee_id VARCHAR(20) PRIMARY KEY,
    gender VARCHAR(1),
    birthdate TIMESTAMP,
    position_id VARCHAR(2),
    p_np VARCHAR(2),
    company_id VARCHAR(6),
	employment_status VARCHAR(20)
);

CREATE TABLE bronze.hr_parental_leave_staging (
    employee_id VARCHAR(20),
    type_of_leave VARCHAR(12),
    date TIMESTAMP,
    days INT
);


CREATE TABLE bronze.hr_parental_leave (
    parental_leave_id VARCHAR(20) PRIMARY KEY,
    employee_id VARCHAR(20),
    type_of_leave VARCHAR(12),
    date TIMESTAMP,
    days INT
);


CREATE TABLE bronze.hr_tenure (
    employee_id VARCHAR(20),
    start_date TIMESTAMP,
    end_date TIMESTAMP
);


CREATE TABLE bronze.hr_training_staging (
    company_id VARCHAR(10),
    training_title TEXT,
    date TIMESTAMP,
    training_hours INT,
    number_of_participants INT
);


CREATE TABLE bronze.hr_training (
    training_id VARCHAR(20) PRIMARY KEY,
    company_id VARCHAR(10),
    training_title TEXT,
    date TIMESTAMP,
    training_hours INT,
    number_of_participants INT
);

CREATE TABLE bronze.hr_safety_workdata_staging (
    company_id VARCHAR(10),
    contractor TEXT,
    date TIMESTAMP,
    manpower INT,
    manhours INT
);

CREATE TABLE bronze.hr_safety_workdata (
    safety_workdata_id VARCHAR(20) PRIMARY KEY,
    company_id VARCHAR(10),
    contractor TEXT,
    date TIMESTAMP,
    manpower INT,
    manhours INT
);

CREATE TABLE bronze.hr_occupational_safety_health_staging (
    company_id VARCHAR(10),
    workforce_type TEXT,
    lost_time BOOLEAN,
    date TIMESTAMP,
    incident_type TEXT,
    incident_title TEXT,
    incident_count INT
);

CREATE TABLE bronze.hr_occupational_safety_health (
    osh_id VARCHAR(20) PRIMARY KEY,
    company_id VARCHAR(10),
    workforce_type TEXT,
    lost_time BOOLEAN,
    date TIMESTAMP,
    incident_type TEXT,
    incident_title TEXT,
    incident_count INT
);