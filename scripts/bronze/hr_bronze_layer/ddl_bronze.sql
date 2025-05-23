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

DROP TABLE IF EXISTS bronze.hr_safety;
DROP TABLE IF EXISTS bronze.hr_training;
DROP TABLE IF EXISTS bronze.hr_tenure;
DROP TABLE IF EXISTS bronze.hr_parental_leave;
DROP TABLE IF EXISTS bronze.hr_position;
DROP TABLE IF EXISTS bronze.hr_demographics;


CREATE TABLE bronze.hr_demographics (
    employee_id VARCHAR(20) PRIMARY KEY,
    gender VARCHAR(1),
    birthdate TIMESTAMP,
    position_id VARCHAR(2),
    p_np VARCHAR(2),
    company_id VARCHAR(6),
	employment_status VARCHAR(20)
);

CREATE TABLE bronze.hr_parental_leave (
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


CREATE TABLE bronze.hr_training (
    employee_id VARCHAR(20),
    hours INT,
    date TIMESTAMP,
    position_id VARCHAR(2)
);

CREATE TABLE bronze.hr_safety (
    employee_id VARCHAR(20),
    company_id VARCHAR(10),
    date TIMESTAMP,
    type_of_accident VARCHAR(50),
    safety_man_hours INT
);