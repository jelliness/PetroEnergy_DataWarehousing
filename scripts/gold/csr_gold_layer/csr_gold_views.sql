/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_program_descriptions
-- =============================================================================
CREATE OR REPLACE VIEW gold.dim_program_descriptions
AS 
SELECT
	cprog.program_name AS "Category",
	project_name AS "Project",
	project_metrics AS "Metric"
FROM silver.csr_projects AS cproj
LEFT JOIN silver.csr_programs AS cprog
ON cproj.program_id = cprog.program_id;

-- =============================================================================
-- Create Dimension: gold.dim_csr_numbers
-- =============================================================================
CREATE OR REPLACE VIEW gold.dim_csr_numbers
AS
SELECT
	company_name AS "Company",
	program_name AS "Program Name",
	project_year AS "Year",
	project_name AS "Project",
	csr_report AS "Report",
	project_metrics AS "Metrics",
	project_expenses AS "Expenses"
FROM silver.csr_activity AS act
LEFT JOIN silver.csr_projects AS cproj
ON cproj.project_id = act.project_id
LEFT JOIN silver.csr_programs AS cprog
ON cproj.program_id = cprog.program_id
LEFT JOIN ref.company_main AS comp
ON comp.company_id = act.company_id
GROUP BY company_name, program_name, project_year, project_name, csr_report, project_metrics, act.company_id, project_expenses
ORDER BY act.company_id;

-- =============================================================================
-- Create Fact: gold.fact_perc_report
-- =============================================================================
CREATE OR REPLACE VIEW gold.fact_perc_report
AS
SELECT
	program_name AS "Program",
	project_name AS "Project",
	project_year AS "Year",
	SUM(csr_report::NUMERIC) AS "Total"
FROM silver.csr_activity AS act
LEFT JOIN silver.csr_projects AS cproj
ON act.project_id = cproj.project_id
LEFT JOIN silver.csr_programs AS cprog
ON cproj.program_id = cprog.program_id
GROUP BY (cprog.program_id, cproj.program_id, program_name, project_name, project_year)
ORDER BY cproj.program_id;

-- =============================================================================
-- Create Fact: gold.fact_investment_per_company
-- =============================================================================
CREATE OR REPLACE VIEW gold.fact_investment_per_company
AS
SELECT 
	company_name AS "Company",
	cprog.program_name AS "Program",
	SUM(project_expenses) AS "Investments"
FROM silver.csr_activity AS cact
LEFT JOIN silver.csr_projects AS cproj
ON cact.project_id = cproj.project_id
LEFT JOIN silver.csr_programs AS cprog
ON cproj.program_id = cprog.program_id
LEFT JOIN ref.company_main AS ccomp
ON cact.company_id = ccomp.company_id
GROUP BY company_name, cprog.program_name
ORDER BY company_name, "Investments";

-- =============================================================================
-- Create Fact: gold.fact_perc_investment
-- =============================================================================
CREATE OR REPLACE VIEW gold.fact_perc_investment
AS
SELECT 
	cprog.program_name,
	SUM(project_expenses) AS "Investments"
FROM silver.csr_activity AS cact
LEFT JOIN silver.csr_projects AS cproj
ON cact.project_id = cproj.project_id
LEFT JOIN silver.csr_programs AS cprog
ON cproj.program_id = cprog.program_id
GROUP BY (cprog.program_name)
ORDER BY "Investments"