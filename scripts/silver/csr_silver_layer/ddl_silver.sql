-- ===============================================================================
-- DDL Script: Create Silver Tables
-- ===============================================================================
-- Script Purpose:
--     This script creates tables in the 'silver' schema, dropping existing tables 
--     if they already exist.
--     Run this script to re-define the DDL structure of 'silver' Tables
-- ===============================================================================

DROP TABLE IF EXISTS silver.csr_company;
CREATE TABLE silver.csr_company (
    company_id TEXT NOT NULL,
    company_name TEXT,
    resources TEXT,
    date_created TIMESTAMP DEFAULT NOW(),
    date_updated TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS silver.csr_programs;
CREATE TABLE silver.csr_programs (
    program_id TEXT NOT NULL,
    program_name TEXT,
    date_created TIMESTAMP DEFAULT NOW(),
    date_updated TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS silver.csr_projects;
CREATE TABLE silver.csr_projects (
    project_id TEXT NOT NULL,
    program_id TEXT,
    project_name TEXT,
    project_metrics TEXT,
    date_created TIMESTAMP DEFAULT NOW(),
    date_updated TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS silver.csr_per_company;
CREATE TABLE silver.csr_per_company (
    inv_id TEXT NOT NULL,
    company_id TEXT,
    program_id TEXT,
    program_investment TEXT,
    date_created TIMESTAMP DEFAULT NOW(),
    date_updated TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS silver.csr_activity;
CREATE TABLE silver.csr_activity (
    csr_id TEXT NOT NULL,
    company_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    ac_year TEXT NOT NULL,
    csr_report TEXT,
    date_created TIMESTAMP DEFAULT NOW(),
    date_updated TIMESTAMP DEFAULT NOW()
);
