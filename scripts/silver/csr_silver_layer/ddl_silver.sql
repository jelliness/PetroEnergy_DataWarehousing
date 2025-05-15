-- ===============================================================================
-- DDL Script: Create Silver Tables
-- ===============================================================================
-- Script Purpose:
--     This script creates tables in the 'silver' schema, dropping existing tables 
--     if they already exist.
--     Run this script to re-define the DDL structure of 'silver' Tables
-- ===============================================================================

DROP TABLE IF EXISTS silver.csr_accomplishments;

CREATE TABLE silver.csr_accomplishments (
    ac_id TEXT NOT NULL,
    company_id TEXT NOT NULL,
    ac_year TEXT NOT NULL,      
    csr_program TEXT NOT NULL,
    csr_report TEXT,            
    date_created TIMESTAMP DEFAULT NOW(),
    date_updated TIMESTAMP DEFAULT NOW()
);  
