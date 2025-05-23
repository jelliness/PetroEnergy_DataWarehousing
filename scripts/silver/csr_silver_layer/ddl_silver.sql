-- ===============================================================================
-- DDL Script: Create Silver Tables
-- ===============================================================================
-- Script Purpose:
--     This script creates tables in the 'silver' schema, dropping existing tables 
--     if they already exist.
--     Run this script to re-define the DDL structure of 'silver' Tables
-- ===============================================================================


DROP TABLE IF EXISTS silver.csr_programs;
CREATE TABLE silver.csr_programs (
    program_id VARCHAR(5) NOT NULL PRIMARY KEY,
    program_name VARCHAR(20),
    date_created TIMESTAMP DEFAULT NOW(),
    date_updated TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS silver.csr_projects;
CREATE TABLE silver.csr_projects (
    project_id VARCHAR(20) NOT NULL PRIMARY KEY,
    program_id VARCHAR(20) NOT NULL,
    project_name VARCHAR(50) NOT NULL,
    project_metrics TEXT NOT NULL DEFAULT 'None',
    date_created TIMESTAMP DEFAULT NOW(),
    date_updated TIMESTAMP DEFAULT NOW(),
	CONSTRAINT fk_program_name 
		FOREIGN KEY (program_id)
		REFERENCES silver.csr_programs (program_id) ON DELETE CASCADE
);

DROP TABLE IF EXISTS silver.csr_activity;
CREATE TABLE silver.csr_activity (
    csr_id VARCHAR(10) NOT NULL NOT NULL PRIMARY KEY,
    company_id VARCHAR(20) NOT NULL,
    project_id VARCHAR(20) NOT NULL,
    project_year SMALLINT,
    csr_report NUMERIC,
    project_expenses NUMERIC,
    date_created TIMESTAMP DEFAULT NOW(),
    date_updated TIMESTAMP DEFAULT NOW(),
	CONSTRAINT fk_company_name 
		FOREIGN KEY (company_id)
		REFERENCES ref.company_main (company_id) ON DELETE CASCADE,
	CONSTRAINT fk_company_project 
		FOREIGN KEY (project_id) 
		REFERENCES silver.csr_projects (project_id) ON DELETE SET NULL
);