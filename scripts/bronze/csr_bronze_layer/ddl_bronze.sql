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

DROP TABLE IF EXISTS bronze.csr_company;
CREATE TABLE bronze.csr_company (
    company_id VARCHAR(20),
    company_name VARCHAR(100),
    resources VARCHAR(20)
);

DROP TABLE IF EXISTS bronze.csr_programs;
CREATE TABLE bronze.csr_programs (
	program_id VARCHAR(20),
	program_name VARCHAR(20)
);

DROP TABLE IF EXISTS bronze.csr_projects;
CREATE TABLE bronze.csr_projects (
	project_id VARCHAR(20),
	program_id VARCHAR(20),
	project_name VARCHAR(50),
	project_metrics VARCHAR(100)
);

DROP TABLE IF EXISTS bronze.csr_per_company;
CREATE TABLE bronze.csr_per_company (
	inv_id VARCHAR(20),
	company_id VARCHAR(20),
	program_id VARCHAR(20),
	program_investment NUMERIC
);

DROP TABLE IF EXISTS bronze.csr_activity;
CREATE TABLE bronze.csr_activity (
    csr_id VARCHAR(20),
    company_id VARCHAR(20),
	project_id VARCHAR(20),
    ac_year VARCHAR(4),
    csr_report NUMERIC
);