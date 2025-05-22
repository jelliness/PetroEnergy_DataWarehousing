/*
===============================================================================
DDL Script: Create Gold Views for HR Data
===============================================================================



===============================================================================
*/
CREATE SCHEMA IF NOT EXISTS gold;

DROP VIEW IF EXISTS gold.dim_employee_descriptions CASCADE;

/*
===============================================================================
							EMPLOYEE DESCRIPTION
===============================================================================
*/
CREATE OR REPLACE VIEW gold.dim_employee_descriptions AS
	SELECT
	    demo.employee_id,
	    demo.gender,
	    demo.birthdate,
	    -- demo.age,
	    demo.p_np,
	    demo.position_id,
	    -- pos.position_name,
	    comp.company_id,
	    comp.company_name,
	    ten.start_date,
	    ten.end_date,
		ten.tenure_length
	FROM silver.hr_demographics AS demo
	
	-- LEFT JOIN silver.hr_position AS pos ON demo.position_id = pos.position_id
	LEFT JOIN silver.csr_company AS comp ON demo.company_id = comp.company_id
	LEFT JOIN silver.hr_tenure AS ten ON demo.employee_id = ten.employee_id;
