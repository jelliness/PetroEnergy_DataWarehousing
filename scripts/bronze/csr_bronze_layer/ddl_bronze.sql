/*
===============================================================================
DDL Script: Create Bronze Tables for CSR Company and Accmomplishments
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
      Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
DROP TABLE IF EXISTS bronze.csr_programs;
CREATE TABLE bronze.csr_programs (
	program_id VARCHAR(5),
	program_name VARCHAR(20)
);

DROP TABLE IF EXISTS bronze.csr_projects;
CREATE TABLE bronze.csr_projects (
	project_id VARCHAR(20),
	program_id VARCHAR(20),
	project_name VARCHAR(50),
	project_metrics TEXT
);

DROP TABLE IF EXISTS bronze.csr_activity;
CREATE TABLE bronze.csr_activity (
    csr_id VARCHAR(10),
    company_id VARCHAR(20),
	project_id VARCHAR(20),
    project_year SMALLINT,
    csr_report NUMERIC,
	project_expenses NUMERIC
);