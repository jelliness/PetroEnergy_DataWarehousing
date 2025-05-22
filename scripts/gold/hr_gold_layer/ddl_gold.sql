/*
===============================================================================
DDL Script: Create Gold Views for HR Data
===============================================================================
*/
CREATE SCHEMA IF NOT EXISTS gold;

DROP VIEW IF EXISTS gold.dim_employee_descriptions CASCADE;
DROP VIEW IF EXISTS gold.dim_employee_training_description;
DROP VIEW IF EXISTS gold.dim_employee_safety_description;

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
	    demo.p_np,
	    demo.position_id,
	    pos.position_name,
	    comp.company_id,
	    comp.company_name,
	    ten.start_date,
	    ten.end_date,
		ten.tenure_length
	FROM silver.hr_demographics AS demo
	LEFT JOIN silver.hr_position AS pos ON demo.position_id = pos.position_id
	LEFT JOIN ref.company_main AS comp ON demo.company_id = comp.company_id
	LEFT JOIN silver.hr_tenure AS ten ON demo.employee_id = ten.employee_id;

/*
===============================================================================
							TRAINING DESCRIPTION
===============================================================================
*/
CREATE OR REPLACE VIEW gold.dim_employee_training_description AS
	SELECT
		dd.employee_id,
		dd.company_id,
		dd.company_name,
		dd.gender,
		dd.position_id,
		tr.hours,
		tr.date
	
	FROM silver.hr_training tr
	LEFT JOIN gold.dim_employee_descriptions dd ON tr.employee_id = dd.employee_id
	ORDER BY dd.employee_id;
/*
===============================================================================
							SAFETY DESCRIPTION
===============================================================================
*/
CREATE OR REPLACE VIEW gold.dim_employee_safety_description AS
	SELECT
		dd.employee_id,
		dd.company_id,
		dd.company_name,
		dd.gender,
		dd.position_id,
		
		sft.date,
		sft.type_of_accident,
		sft.safety_man_hours
		
	FROM silver.hr_safety sft
	LEFT JOIN gold.dim_employee_descriptions dd ON sft.employee_id = dd.employee_id
	ORDER BY sft.employee_id;