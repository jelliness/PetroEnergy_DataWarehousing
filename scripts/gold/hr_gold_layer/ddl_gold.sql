/*
===============================================================================
DDL Script: Create Gold Views for HR Data
===============================================================================
*/
CREATE SCHEMA IF NOT EXISTS gold;

DROP VIEW IF EXISTS gold.dim_employee_descriptions CASCADE;
DROP VIEW IF EXISTS gold.dim_employee_training_description;
DROP VIEW IF EXISTS gold.dim_employee_safety_description;
DROP VIEW IF EXISTS gold.dim_employee_parental_leave_description;

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
		demo.employment_status,
		
	    ten.start_date,
	    ten.end_date,
		ten.tenure_length
	FROM silver.hr_demographics AS demo
	LEFT JOIN ref.hr_position AS pos ON demo.position_id = pos.position_id
	LEFT JOIN ref.company_main AS comp ON demo.company_id = comp.company_id
	LEFT JOIN silver.hr_tenure AS ten ON demo.employee_id = ten.employee_id;

/*
===============================================================================
							TRAINING DESCRIPTION
===============================================================================
*/
CREATE OR REPLACE VIEW gold.dim_employee_training_description AS
	SELECT
		tr.company_id,
		comp.company_name,
		tr.date,
		tr.training_title,
		tr.training_hours,
		tr.number_of_participants,
		tr.total_training_hours
	
	FROM silver.hr_training tr
	LEFT JOIN ref.company_main AS comp ON tr.company_id = comp.company_id
	ORDER BY tr.date;
/*
===============================================================================
							SAFETY MANHOURS DESCRIPTION
===============================================================================
*/

CREATE OR REPLACE VIEW gold.dim_employee_safety_manhours_description AS
	SELECT
		sft.company_id,
		comp.company_name,
		sft.contractor,
		sft.date,
		sft.manpower,
		sft.manhours
		
	FROM silver.hr_safety_workdata sft
	LEFT JOIN ref.company_main AS comp ON sft.company_id = comp.company_id
	ORDER BY sft.date;
/*
===============================================================================
					OCCUPATIONAL SAFETY HEALTH DESCRIPTION
===============================================================================
*/
CREATE OR REPLACE VIEW gold.dim_occupational_safety_health AS
	SELECT
		osh.company_id,
		comp.company_name,
		osh.workforce_type,
		osh.lost_time,
		osh.date,
		osh.incident_type,
		osh.incident_title,
		osh.incident_count
		
	FROM silver.hr_occupational_safety_health osh
	LEFT JOIN ref.company_main AS comp ON osh.company_id = comp.company_id
	ORDER BY osh.date;

/*
===============================================================================
						PARENTAL LEAVE DESCRIPTION
===============================================================================
*/
CREATE OR REPLACE VIEW gold.dim_employee_parental_leave_description AS
SELECT
    d.employee_id,
    d.company_id,
    d.company_name,
    d.gender,
    d.position_id,
    pl.date,
    pl.days,
	pl.end_date,
    pl.months_availed,
    pl.type_of_leave
FROM silver.hr_parental_leave pl
LEFT JOIN gold.dim_employee_descriptions d ON pl.employee_id = d.employee_id
ORDER BY pl.employee_id;
