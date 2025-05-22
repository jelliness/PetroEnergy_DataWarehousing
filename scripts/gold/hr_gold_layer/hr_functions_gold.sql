/*
===============================================================================
							HR FUNCTION FACT TABLE
===============================================================================
*/
DROP FUNCTION IF EXISTS gold.func_fact_summary;
DROP FUNCTION IF EXISTS gold.func_hr_rate_summary;

CREATE OR REPLACE FUNCTION gold.func_fact_summary (
	p_employee_id VARCHAR(20) DEFAULT NULL,
	p_gender VARCHAR(1) DEFAULT NULL,
	p_position_id VARCHAR(2)[] DEFAULT NULL,
	p_company_id VARCHAR(6)[] DEFAULT NULL,
	p_year INT[] DEFAULT NULL
)
RETURNS TABLE (
	year INT,
	employee_id VARCHAR(20),
	gender VARCHAR(1),
	position_id VARCHAR(2),
	company_id VARCHAR(6),
	company_name TEXT,
	start_date TIMESTAMP,
	end_date TIMESTAMP
) AS $$
BEGIN
	RETURN QUERY
	SELECT
		y.year,
		d.employee_id,
		d.gender,
		d.position_id,
		d.company_id,
		d.company_name,
		d.start_date,
		d.end_date
	FROM gold.dim_employee_descriptions d,
		unnest(COALESCE(p_year, ARRAY[EXTRACT(YEAR FROM CURRENT_DATE)::INT])) AS y(year)
	WHERE
		d.start_date <= make_date(y.year, 12, 31)
		AND (d.end_date IS NULL OR d.end_date >= make_date(y.year, 1, 1))
		AND (p_employee_id IS NULL OR d.employee_id = p_employee_id)
		AND (p_gender IS NULL OR d.gender = p_gender)
		AND (p_position_id IS NULL OR d.position_id = ANY(p_position_id))
		AND (p_company_id IS NULL OR d.company_id = ANY(p_company_id));
END;
$$ LANGUAGE plpgsql;

/*
===============================================================================
							HR FUNCTION RATE TABLE
===============================================================================
*/
CREATE OR REPLACE FUNCTION gold.func_hr_rate_summary (
    p_gender VARCHAR(1) DEFAULT NULL,
    p_position_id VARCHAR(2)[] DEFAULT NULL,
    p_company_id VARCHAR(6)[] DEFAULT NULL,
    p_year INT[] DEFAULT NULL
)
RETURNS TABLE (
    year INT,
    company_id VARCHAR(6),
    company_name TEXT,
    total_employees INT,
    avg_tenure NUMERIC(5,2),
    resigned_count INT,
    attrition_rate_percent NUMERIC(5,2)
) AS $$
BEGIN
    RETURN QUERY
    WITH years AS (
        SELECT UNNEST(COALESCE(p_year, ARRAY[EXTRACT(YEAR FROM CURRENT_DATE)::INT])) AS year
    ),
    employees_filtered AS (
        SELECT *
        FROM gold.dim_employee_descriptions d
        WHERE
            (p_gender IS NULL OR d.gender = p_gender) AND 
            (p_position_id IS NULL OR d.position_id = ANY(p_position_id)) AND 
            (p_company_id IS NULL OR d.company_id = ANY(p_company_id))
    ),
    active_employees AS (
        SELECT
            e.employee_id,
            e.company_id,
            e.company_name,
            e.tenure_length,
            y.year
        FROM employees_filtered e
        CROSS JOIN years y
        WHERE
            e.start_date <= make_date(y.year, 12, 31) AND 
            (e.end_date IS NULL OR e.end_date >= make_date(y.year, 1, 1))
    ),
    resignations AS (
        SELECT
            d.company_id,
            EXTRACT(YEAR FROM d.end_date)::INT AS year,
            COUNT(DISTINCT d.employee_id) AS resigned_count
        FROM employees_filtered d
        WHERE d.end_date IS NOT NULL
            AND EXTRACT(YEAR FROM d.end_date) = ANY(COALESCE(p_year, ARRAY[EXTRACT(YEAR FROM CURRENT_DATE)::INT]))
        GROUP BY d.company_id, year
    ),
    total_active AS (
        SELECT
            a.year,
            a.company_id,
            a.company_name,
            COUNT(DISTINCT a.employee_id)::INT AS total_employees,
            ROUND(AVG(a.tenure_length), 2) AS avg_tenure
        FROM active_employees a
        GROUP BY a.year, a.company_id, a.company_name
    )
    SELECT
        t.year,
        t.company_id,
        t.company_name,
        t.total_employees,
        t.avg_tenure,
        COALESCE(r.resigned_count::INT, 0) AS resigned_count,
        CASE
            WHEN t.total_employees > 0 THEN ROUND((COALESCE(r.resigned_count, 0)::NUMERIC / t.total_employees) * 100, 2)
            ELSE NULL
        END AS attrition_rate_percent
    FROM total_active t
    LEFT JOIN resignations r ON t.company_id = r.company_id AND t.year = r.year
    ORDER BY t.year, t.company_id;
END;
$$ LANGUAGE plpgsql;