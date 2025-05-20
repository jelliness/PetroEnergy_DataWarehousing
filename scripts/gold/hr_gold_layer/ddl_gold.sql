/*
===============================================================================
DDL Script: Create Gold Views for HR Data
===============================================================================



===============================================================================
*/
CREATE SCHEMA IF NOT EXISTS gold;
/*
DROP VIEW IF EXISTS gold.vw_active_employees;
DROP VIEW IF EXISTS gold.vw_active_per_gender;
DROP VIEW IF EXISTS gold.vw_headcount_per_year;
DROP VIEW IF EXISTS gold.vw_gender_headcount_per_year;
DROP VIEW IF EXISTS gold.vw_position_headcount;
DROP VIEW IF EXISTS gold.vw_attrition_rate;
DROP VIEW IF EXISTS gold.vw_avg_tenure;
DROP VIEW IF EXISTS gold.vw_training_participants_per_year;
DROP VIEW IF EXISTS gold.vw_training_hours_per_year;
*/

DROP VIEW IF EXISTS gold.dim_employee_details;

CREATE VIEW gold.dim_employee_details AS
	SELECT 
    d.employee_id,
    d.gender,
    d.position_id,
    t.start_date,
    t.end_date,
    t.tenure_length,
    p.type_of_leave,
    p.date AS leave_start_date,
    p.end_date AS leave_end_date,
    tr.hours AS training_hours,
    tr.year_start
	FROM silver.hr_demographics d
		LEFT JOIN silver.hr_tenure t 
    		ON d.employee_id = t.employee_id
		LEFT JOIN silver.hr_parental_leave p 
    		ON d.employee_id = p.employee_id
		LEFT JOIN silver.hr_training tr 
    		ON d.employee_id = tr.employee_id;


/*
CREATE VIEW gold.vw_hr_fact AS
	SELECT 
    d.employee_id,
    d.gender,
    d.position_id,
    t.start_date,
    t.end_date,
    t.tenure_length,
    p.type_of_leave,
    p.date AS leave_start_date,
    p.end_date AS leave_end_date,
    tr.hours AS training_hours,
    tr.year_start
	FROM silver.hr_demographics d
		LEFT JOIN silver.hr_tenure t 
    		ON d.employee_id = t.employee_id
		LEFT JOIN silver.hr_parental_leave p 
    		ON d.employee_id = p.employee_id
		LEFT JOIN silver.hr_training tr 
    		ON d.employee_id = tr.employee_id;
*/

/*
--ACTIVE EMPLOYEES
CREATE VIEW gold.vw_active_employees AS
	SELECT 
		COUNT(*) AS Active_Employees
	FROM gold.vw_hr_fact
	WHERE end_date IS NULL;

-- ACTIVE PER GENDER
CREATE VIEW gold.vw_active_per_gender AS
	SELECT 
    	COUNT(*) AS total_employees,
    	COUNT(CASE WHEN gender = 'M' THEN 1 END) AS male,
    	COUNT(CASE WHEN gender = 'F' THEN 1 END) AS female
	FROM silver.hr_demographics d
	JOIN silver.hr_tenure t ON d.employee_id = t.employee_id
	WHERE t.end_date IS NULL;

-- ACTIVE EMPLOYEES PER YEAR
CREATE VIEW gold.vw_headcount_per_year AS
	SELECT

-- ACTIVE GENDER HEADCOUNT PER YEAR
CREATE VIEW gold.vw_gender_headcount_per_year AS
	SELECT
    	EXTRACT(YEAR FROM year)::SMALLINT AS year, -- THIS CONVERTS DATETIME TO INT
    	gender,
    	COUNT(DISTINCT employee_id) AS headcount
	FROM (
    	SELECT
        	d.employee_id,
        	d.gender,
        	generate_series(
            	DATE_TRUNC('year', t.start_date),
            	COALESCE(t.end_date, CURRENT_DATE),
            	INTERVAL '1 year'
        	)::DATE AS year -- THIS CONVERTS DATETIME TO YEAR
    	FROM silver.hr_demographics d
    	JOIN silver.hr_tenure t ON d.employee_id = t.employee_id
	) sub
	GROUP BY year, gender
	ORDER BY year, gender;

-- TOTAL ACTIVE PER POSITION ID
CREATE VIEW gold.vw_position_headcount AS
	SELECT 
    	d.position_id,
    	COUNT(*) AS active_headcount
	FROM silver.hr_demographics d
	JOIN silver.hr_tenure t ON d.employee_id = t.employee_id
	WHERE t.end_date IS NULL
	GROUP BY d.position_id, d.position_name
	ORDER BY active_headcount DESC;

-- ATTRITION RATE
CREATE VIEW gold.vw_attrition_rate AS
	SELECT
    	COUNT(*) FILTER (WHERE end_date IS NOT NULL) AS resignations, -- TOTAL RESIGNATION
    	COUNT(*) AS total_employees, -- TOTAL EMPLOYEES
    	ROUND(
        	COUNT(*) FILTER (WHERE end_date IS NOT NULL) * 100.0 / 
			NULLIF(COUNT(*), 0), 2 -- SAFETY MECHANISM TO AVOID DIVISION BY ZERO
    	) AS attrition_rate_percent -- TOTAL RESIGNATION / TOTAL EMPLOYEES * 100
	FROM silver.hr_tenure;

-- TENURE RATE
CREATE VIEW gold.vw_avg_tenure AS
	SELECT
    	ROUND(AVG(tenure_length), 2) AS average_years_of_service
	FROM silver.hr_tenure;

-- TRAINING PARTICIPANT PER YEAR
CREATE VIEW gold.vw_training_participants_per_year AS
	SELECT
    	year_start,
    	COUNT(DISTINCT employee_id) AS participants
	FROM silver.hr_training
	GROUP BY year_start
	ORDER BY year_start;

-- TRAINING HOURS PER YEAR
CREATE VIEW gold.vw_training_hours_per_year AS
	SELECT
    	year_start,
    	SUM(hours) AS total_training_hours
	FROM silver.hr_training
	GROUP BY year_start
	ORDER BY year_start;
*/