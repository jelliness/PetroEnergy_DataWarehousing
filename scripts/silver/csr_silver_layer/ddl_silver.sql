-- ===============================================================================
-- DDL Script: Create Silver Tables
-- ===============================================================================
-- Script Purpose:
--     This script creates tables in the 'silver' schema, dropping existing tables 
--     if they already exist.
--     Run this script to re-define the DDL structure of 'bronze' Tables
-- ===============================================================================

DROP TABLE IF EXISTS silver.csr_accomplishments;

CREATE TABLE silver.csr_accomplishments (
    ac_id TEXT NOT NULL,
    company_id TEXT NOT NULL,
    ac_year INTEGER NOT NULL,
    program TEXT NOT NULL,
    csr_report INTEGER
);
