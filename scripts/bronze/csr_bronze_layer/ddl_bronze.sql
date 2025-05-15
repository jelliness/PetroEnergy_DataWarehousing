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

DROP TABLE IF EXISTS bronze.csr_accomplishments;
CREATE TABLE bronze.csr_accomplishments (
    ac_id VARCHAR(20),
    company_id VARCHAR(20),
    ac_year VARCHAR(4),
    csr_program VARCHAR(100),
    csr_report NUMERIC
);