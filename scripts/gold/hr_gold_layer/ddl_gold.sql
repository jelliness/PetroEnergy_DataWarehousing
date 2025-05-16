/*
===============================================================================
DDL Script: Create Gold Views for HR Data
===============================================================================



===============================================================================
*/
CREATE SCHEMA IF NOT EXISTS gold;

DROP VIEW IF EXISTS gold.vw_active_employees;

CREATE VIEW gold.vw_active_employees AS
SELECT COUNT(*) AS Active_Employees
FROM silver.hr_tenure
WHERE end_date IS NULL;
