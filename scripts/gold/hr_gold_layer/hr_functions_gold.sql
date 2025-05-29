
DROP FUNCTION IF EXISTS gold.func_employee_summary_yearly;
DROP FUNCTION IF EXISTS gold.func_hr_rate_summary_yearly;
DROP FUNCTION IF EXISTS gold.func_training_summary;
DROP FUNCTION IF EXISTS gold.func_safety_workdata_summary;
DROP FUNCTION IF EXISTS gold.func_occupational_safety_health_summary;
DROP FUNCTION IF EXISTS gold.func_parental_leave_summary_yearly;

DROP FUNCTION IF EXISTS gold.func_employee_summary_monthly;
DROP FUNCTION IF EXISTS gold.func_hr_rate_summary_monthly;
--DROP FUNCTION IF EXISTS gold.func_training_summary_monthly;
--DROP FUNCTION IF EXISTS gold.func_safety_summary_monthly;
DROP FUNCTION IF EXISTS gold.func_parental_leave_summary_monthly;

/*
===============================================================================
							HR EMPLOYEE FACT TABLE
===============================================================================
*/
CREATE OR REPLACE FUNCTION gold.func_employee_summary_yearly (
    p_employee_id VARCHAR(20) DEFAULT NULL,
    p_gender VARCHAR(1) DEFAULT NULL,
    p_position_id VARCHAR(2)[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_year INT[] DEFAULT NULL
)
RETURNS TABLE (
    year INT,
    employee_id VARCHAR(20),
    gender VARCHAR(1),
    position_id VARCHAR(2),
	position_name VARCHAR(20),
    company_id VARCHAR(10),
    company_name VARCHAR(255),
    start_date TIMESTAMP,
    end_date TIMESTAMP
)
AS $$
BEGIN
	RETURN QUERY
		SELECT
			dd.year,
			d.employee_id,
			d.gender,
			d.position_id,
			d.position_name,
			d.company_id,
			d.company_name,
			d.start_date,
			d.end_date
		FROM gold.dim_employee_descriptions d
		JOIN gold.dim_date dd ON dd.date_id BETWEEN d.start_date::date AND COALESCE(d.end_date::date, dd.date_id)
		WHERE
			(p_employee_id IS NULL OR d.employee_id = p_employee_id)
			AND (p_gender IS NULL OR d.gender = p_gender)
			AND (p_position_id IS NULL OR d.position_id = ANY(p_position_id))
			AND (p_company_id IS NULL OR d.company_id = ANY(p_company_id))
			AND (p_year IS NULL OR dd.year = ANY(p_year))
		GROUP BY
			dd.year,
			d.employee_id,
			d.gender,
			d.position_id,
			d.position_name,
			d.company_id,
			d.company_name,
			d.start_date,
			d.end_date
		ORDER BY
			dd.year,
			d.employee_id;
END;
$$ LANGUAGE plpgsql;


/*
===============================================================================
							HR FUNCTION RATE SUMMARY
===============================================================================
*/
CREATE OR REPLACE FUNCTION gold.func_hr_rate_summary_yearly (
	p_gender VARCHAR(1) DEFAULT NULL,
	p_position_id VARCHAR(2)[] DEFAULT NULL,
	p_company_id VARCHAR(10)[] DEFAULT NULL,
	p_year INT[] DEFAULT NULL
	)
RETURNS TABLE (
	year INT,
	company_id VARCHAR(10),
	company_name VARCHAR(255),
	total_employees INT,
	avg_tenure NUMERIC(5,2),
	resigned_count INT
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
		WHERE 
			d.end_date IS NOT NULL
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
		GROUP BY 
			a.year, 
			a.company_id, 
			a.company_name
	)
	SELECT
		t.year,
		t.company_id,
		t.company_name,
		t.total_employees,
		t.avg_tenure,
		COALESCE(r.resigned_count::INT, 0) AS resigned_count
	FROM total_active t
	LEFT JOIN resignations r ON t.company_id = r.company_id AND t.year = r.year
	ORDER BY t.year, t.company_id;
END;
$$ LANGUAGE plpgsql;

/*
===============================================================================
					HR FUNCTION TRAINING SUMMARY
===============================================================================
*/
CREATE OR REPLACE FUNCTION gold.func_training_summary(
    p_year INT[] DEFAULT NULL,
    p_quarter TEXT[] DEFAULT NULL,
    p_month INT[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_training_title TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    company_name VARCHAR(255),
    training_title TEXT,
    month_value INT,
    month_name TEXT,
    year INT,
    quarter TEXT,
    training_hours INT,
    number_of_participants INT,
    total_training_hours INT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        tr.company_name,
        tr.training_title,
        dd.month AS month_value,
        TRIM(dd.month_name) AS month_name,
        dd.year,
        CONCAT('Q', dd.quarter::TEXT) AS quarter,
        SUM(tr.training_hours)::INT AS training_hours,
        SUM(tr.number_of_participants)::INT AS number_of_participants,
        SUM(tr.total_training_hours)::INT AS total_training_hours
    FROM gold.dim_employee_training_description tr
    JOIN gold.dim_date dd ON dd.date_id = tr.date::DATE
    WHERE 
        (p_year IS NULL OR dd.year = ANY(p_year)) AND
        (p_quarter IS NULL OR CONCAT('Q', dd.quarter::TEXT) = ANY(p_quarter)) AND
        (p_month IS NULL OR dd.month = ANY(p_month)) AND
        (p_company_id IS NULL OR tr.company_id = ANY(p_company_id)) AND
        (p_training_title IS NULL OR tr.training_title = ANY(p_training_title))
    GROUP BY
        tr.company_name,
        tr.training_title,
        dd.month,
        dd.month_name,
        dd.year,
        dd.quarter
    ORDER BY
        company_name, year, month_value;
END;
$$ LANGUAGE plpgsql;

/*
===============================================================================
							HR FUNCTION SAFETY TABLE
===============================================================================
*/
CREATE OR REPLACE FUNCTION gold.func_safety_workdata_summary(
    p_year INT[] DEFAULT NULL,
    p_quarter TEXT[] DEFAULT NULL,
    p_month INT[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_contractor TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    company_name VARCHAR(255),
    contractor TEXT,
    month_value INT,
    month_name TEXT,
    year INT,
    quarter TEXT,
    manpower INT,
    manhours INT,
    previous_manhours BIGINT,
    cumulative_manhours BIGINT
)
AS $$
BEGIN
    RETURN QUERY
    WITH filtered AS (
        SELECT
            sft.company_name,
            sft.contractor,
            dd.month AS month_value,
            TRIM(dd.month_name) AS month_name,
            dd.year,
            CONCAT('Q', dd.quarter::TEXT) AS quarter,
            sft.manpower,
            sft.manhours,
            sft.date
        FROM gold.dim_employee_safety_manhours_description sft
        JOIN gold.dim_date dd ON dd.date_id = sft.date::DATE
        WHERE
            (p_year IS NULL OR dd.year = ANY(p_year)) AND
            (p_quarter IS NULL OR CONCAT('Q', dd.quarter::TEXT) = ANY(p_quarter)) AND
            (p_month IS NULL OR dd.month = ANY(p_month)) AND
            (p_company_id IS NULL OR sft.company_id = ANY(p_company_id)) AND
            (p_contractor IS NULL OR sft.contractor = ANY(p_contractor))
    )
    SELECT
        filtered.company_name,
        filtered.contractor,
        filtered.month_value,
        filtered.month_name,
        filtered.year,
        filtered.quarter,
        filtered.manpower,
        filtered.manhours,
        COALESCE(
            SUM(filtered.manhours) OVER (
                PARTITION BY filtered.company_name, filtered.contractor
                ORDER BY filtered.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0
        ) AS previous_manhours,
        SUM(filtered.manhours) OVER (
            PARTITION BY filtered.company_name, filtered.contractor
            ORDER BY filtered.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_manhours
    FROM filtered
    ORDER BY filtered.company_name, filtered.year, filtered.month_value, filtered.contractor, filtered.date;
END;
$$ LANGUAGE plpgsql;

/*
===============================================================================
					HR OCCUPATIONAL SAFETY HEALTH TABLE
===============================================================================
*/
CREATE OR REPLACE FUNCTION gold.func_occupational_safety_health_summary(
    p_year INT[] DEFAULT NULL,
    p_quarter TEXT[] DEFAULT NULL,
    p_month INT[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_workforce_type TEXT[] DEFAULT NULL,
    p_lost_time BOOLEAN DEFAULT NULL,
    p_incident_type TEXT[] DEFAULT NULL,
    p_incident_title TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    company_name VARCHAR(255),
    workforce_type TEXT,
    month_value INT,
    month_name TEXT,
    year INT,
    quarter TEXT,
    lost_time BOOLEAN,
    incident_type TEXT,
    incident_title TEXT,
    incident_count INT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        osh.company_name,
        osh.workforce_type,
        dd.month AS month_value,
        TRIM(dd.month_name) AS month_name,
        dd.year,
        CONCAT('Q', dd.quarter::TEXT) AS quarter,
        osh.lost_time,
        osh.incident_type,
        osh.incident_title,
        osh.incident_count
    FROM gold.dim_occupational_safety_health osh
    JOIN gold.dim_date dd ON dd.date_id = osh.date::DATE
    WHERE
        (p_year IS NULL OR dd.year = ANY(p_year)) AND
        (p_quarter IS NULL OR CONCAT('Q', dd.quarter::TEXT) = ANY(p_quarter)) AND
        (p_month IS NULL OR dd.month = ANY(p_month)) AND
        (p_company_id IS NULL OR osh.company_id = ANY(p_company_id)) AND
        (p_workforce_type IS NULL OR osh.workforce_type = ANY(p_workforce_type)) AND
        (p_lost_time IS NULL OR osh.lost_time = p_lost_time) AND
        (p_incident_type IS NULL OR osh.incident_type = ANY(p_incident_type)) AND
        (p_incident_title IS NULL OR osh.incident_title = ANY(p_incident_title))
    ORDER BY
        company_name, year, month_value, workforce_type;
END;
$$ LANGUAGE plpgsql;

/*
===============================================================================
						HR FUNCTION PARENTAL LEAVE TABLE
===============================================================================
*/
CREATE OR REPLACE FUNCTION gold.func_parental_leave_summary_yearly (
    p_employee_id VARCHAR DEFAULT NULL,
    p_gender VARCHAR DEFAULT NULL,
    p_position_id VARCHAR[] DEFAULT NULL,
    p_company_id VARCHAR[] DEFAULT NULL,
    p_year INT[] DEFAULT NULL
)
RETURNS TABLE (
    year INT,
    employee_id VARCHAR,
    company_id VARCHAR,
    company_name VARCHAR(255),
    gender VARCHAR,
    position_id VARCHAR,
	type_of_leave VARCHAR,
    total_days INT,
    total_months INT,
    leave_count INT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        dd.year,
        pl.employee_id,
        pl.company_id,
        pl.company_name,
        pl.gender,
        pl.position_id,
		pl.type_of_leave,
        SUM(pl.days)::INT AS total_days,
        SUM(pl.months_availed)::INT AS total_months,
        COUNT(*)::INT AS leave_count
    FROM gold.dim_employee_parental_leave_description pl
    JOIN gold.dim_date dd ON dd.date_id = pl.date::date
    WHERE
        (p_employee_id IS NULL OR pl.employee_id = p_employee_id)
        AND (p_gender IS NULL OR pl.gender = p_gender)
        AND (p_position_id IS NULL OR pl.position_id = ANY(p_position_id))
        AND (p_company_id IS NULL OR pl.company_id = ANY(p_company_id))
        AND (p_year IS NULL OR dd.year = ANY(p_year))
    GROUP BY
        dd.year,
        pl.employee_id,
        pl.company_id,
        pl.company_name,
        pl.gender,
        pl.position_id,
		pl.type_of_leave
    ORDER BY
        dd.year, pl.employee_id;
END;
$$ LANGUAGE plpgsql;

/*
===============================================================================
						HR FUNCTION EMPLOYEE SUMMARY TABLE
===============================================================================
*/
CREATE OR REPLACE FUNCTION gold.func_employee_summary_monthly(
    p_year INT[] DEFAULT NULL,
    p_quarter TEXT[] DEFAULT NULL,
    p_month INT[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_position_id VARCHAR(10)[] DEFAULT NULL,
    p_gender VARCHAR(1) DEFAULT NULL,
    p_age_category TEXT[] DEFAULT NULL,
    p_employment_status TEXT[] DEFAULT NULL,
    p_pnp TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    year INT,
    month_value INT,
    month_name TEXT,
    quarter TEXT,
    gender VARCHAR(1),
    position_id VARCHAR(2),
    company_id VARCHAR(6),
    age_category TEXT,
    p_np VARCHAR(2),
    employment_status VARCHAR(20),
    employee_count BIGINT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.year,
        d.month AS month_value,
        TRIM(d.month_name) AS month_name,
        d.quarter::TEXT,
        demo.gender,
        demo.position_id,
        demo.company_id,
        CASE
            WHEN AGE(demo.birthdate) < INTERVAL '30 years' THEN 'Under 30'
            WHEN AGE(demo.birthdate) BETWEEN INTERVAL '30 years' AND INTERVAL '50 years' THEN '30 to 50'
            ELSE 'Over 50'
        END AS age_category,
        demo.p_np,
        demo.employment_status,
        COUNT(*)::BIGINT AS employee_count
    FROM gold.dim_employee_descriptions demo
    LEFT JOIN gold.dim_date d ON d.date_id = DATE(demo.start_date)
    WHERE
        d.date_id IS NOT NULL
        AND (p_year IS NULL OR d.year = ANY(p_year))
        AND (p_quarter IS NULL OR d.quarter::TEXT = ANY(p_quarter))
        AND (p_month IS NULL OR d.month = ANY(p_month))
        AND (p_company_id IS NULL OR demo.company_id = ANY(p_company_id))
        AND (p_position_id IS NULL OR demo.position_id = ANY(p_position_id))
        AND (p_gender IS NULL OR demo.gender = p_gender)
        AND (
            p_age_category IS NULL OR 
            CASE
                WHEN AGE(demo.birthdate) < INTERVAL '30 years' THEN 'Under 30'
                WHEN AGE(demo.birthdate) BETWEEN INTERVAL '30 years' AND INTERVAL '50 years' THEN '30 to 50'
                ELSE 'Over 50'
            END = ANY(p_age_category)
        )
        AND (p_employment_status IS NULL OR demo.employment_status = ANY(p_employment_status))
        AND (p_pnp IS NULL OR demo.p_np = ANY(p_pnp))
    GROUP BY
        d.year,
        d.month,
        d.month_name,
        d.quarter,
        demo.gender,
        demo.position_id,
        demo.company_id,
        age_category,
        demo.p_np,
        demo.employment_status
    ORDER BY
        d.year DESC,
        d.quarter DESC,
        d.month DESC,
        gender,
        position_id,
        company_id,
        age_category,
        p_np,
        employment_status;
END;
$$ LANGUAGE plpgsql;
/*
===============================================================================
						HR FUNCTION PARENTAL SUMMARY TABLE
===============================================================================
*/
CREATE OR REPLACE FUNCTION gold.func_parental_leave_summary_monthly(
    p_year INT[] DEFAULT NULL,
    p_quarter TEXT[] DEFAULT NULL,
    p_month INT[] DEFAULT NULL,
    p_company_id VARCHAR(6)[] DEFAULT NULL,
    p_position_id VARCHAR(2)[] DEFAULT NULL,
    p_gender VARCHAR(1) DEFAULT NULL,
    p_age_category TEXT[] DEFAULT NULL,
    p_employment_status TEXT[] DEFAULT NULL,
    p_pnp TEXT[] DEFAULT NULL,
    p_type_of_leave TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    year INT,
    month_value INT,
    month_name TEXT,
    quarter TEXT,
    type_of_leave TEXT,
    gender VARCHAR(1),
    position_id VARCHAR(2),
    company_id VARCHAR(6),
    age_category TEXT,
    p_np VARCHAR(2),
    employment_status VARCHAR(20),
    leave_count BIGINT,
    total_days INT
)
AS $$
BEGIN
    RETURN QUERY
    WITH leave_months AS (
        SELECT 
            pl.employee_id,
            d.gender,
            d.position_id,
            d.company_id,
            d.p_np,
            d.employment_status,
            d.birthdate,
            pl.type_of_leave,
            pl.date AS leave_start_date,
            pl.end_date AS leave_end_date,
            gs.month,
            LEAST(pl.end_date, (date_trunc('month', gs.month) + interval '1 month - 1 day')) 
                - GREATEST(pl.date, date_trunc('month', gs.month)) AS leave_interval,
            EXTRACT(day FROM (
                LEAST(pl.end_date, (date_trunc('month', gs.month) + interval '1 month - 1 day')) 
                - GREATEST(pl.date, date_trunc('month', gs.month))
            ))::int + 1 AS leave_days_in_month
        FROM gold.dim_employee_parental_leave_description pl
        JOIN gold.dim_employee_descriptions d ON pl.employee_id = d.employee_id
        CROSS JOIN LATERAL generate_series(
            date_trunc('month', pl.date),
            date_trunc('month', pl.end_date),
            interval '1 month'
        ) AS gs(month)
    ),
    categorized_leaves AS (
        SELECT 
            lm.employee_id,
            lm.gender,
            lm.position_id,
            lm.company_id,
            lm.p_np,
            lm.employment_status,
            lm.birthdate,
            lm.type_of_leave,
            lm.leave_start_date,
            lm.leave_end_date,
            lm.month,
            EXTRACT(YEAR FROM lm.month)::INT AS year,
            EXTRACT(MONTH FROM lm.month)::INT AS month_value,
            TO_CHAR(lm.month, 'Month') AS month_name,
            'Q' || EXTRACT(QUARTER FROM lm.month)::TEXT AS quarter,
            EXTRACT(YEAR FROM age(lm.month, lm.birthdate)) AS age,
            CASE 
                WHEN EXTRACT(YEAR FROM age(lm.month, lm.birthdate)) < 30 THEN 'Under 30'
                WHEN EXTRACT(YEAR FROM age(lm.month, lm.birthdate)) BETWEEN 30 AND 50 THEN '30-50'
                ELSE 'Over 50'
            END AS age_category,
            lm.leave_days_in_month
        FROM leave_months lm
    )
    SELECT 
    cl.year,
    cl.month_value,
    TRIM(cl.month_name),
    cl.quarter,
    cl.type_of_leave::TEXT,  -- Explicit cast here
    cl.gender,
    cl.position_id,
    cl.company_id,
    cl.age_category,
    cl.p_np,
    cl.employment_status,
    COUNT(DISTINCT cl.employee_id) AS leave_count,
    SUM(cl.leave_days_in_month)::INT AS total_days
FROM categorized_leaves cl
    WHERE 
        (p_year IS NULL OR cl.year = ANY(p_year)) AND
        (p_quarter IS NULL OR cl.quarter = ANY(p_quarter)) AND
        (p_month IS NULL OR cl.month_value = ANY(p_month)) AND
        (p_company_id IS NULL OR cl.company_id = ANY(p_company_id)) AND
        (p_position_id IS NULL OR cl.position_id = ANY(p_position_id)) AND
        (p_gender IS NULL OR cl.gender = p_gender) AND
        (p_age_category IS NULL OR cl.age_category = ANY(p_age_category)) AND
        (p_employment_status IS NULL OR cl.employment_status = ANY(p_employment_status)) AND
        (p_pnp IS NULL OR cl.p_np = ANY(p_pnp)) AND
        (p_type_of_leave IS NULL OR cl.type_of_leave = ANY(p_type_of_leave))
    GROUP BY 
        cl.year, cl.month_value, cl.month_name, cl.quarter, cl.type_of_leave,
        cl.gender, cl.position_id, cl.company_id, cl.age_category, cl.p_np, cl.employment_status
    ORDER BY 
        cl.year DESC, cl.month_value DESC, cl.type_of_leave;

END;
$$ LANGUAGE plpgsql;