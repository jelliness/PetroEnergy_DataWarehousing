/*
===============================================================================
							HR EMPLOYEE FACT TABLE
===============================================================================
*/
DROP FUNCTION IF EXISTS gold.func_employee_summary_yearly;
DROP FUNCTION IF EXISTS gold.func_hr_rate_summary_yearly;
DROP FUNCTION IF EXISTS gold.func_training_summary_yearly;
DROP FUNCTION IF EXISTS gold.func_safety_summary_yearly;
DROP FUNCTION IF EXISTS gold.func_parental_leave_summary_yearly;

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
CREATE OR REPLACE FUNCTION gold.func_training_summary_yearly (
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
    total_hours INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        dd.year,
        tr.employee_id,
        tr.company_id,
        tr.company_name,
        tr.gender,
        tr.position_id,
        SUM(tr.hours)::INT AS total_hours
    FROM gold.dim_employee_training_description tr
    JOIN gold.dim_date dd ON dd.date_id = tr.date
    WHERE 
        (p_employee_id IS NULL OR tr.employee_id = p_employee_id)
        AND (p_gender IS NULL OR tr.gender = p_gender)
        AND (p_position_id IS NULL OR tr.position_id = ANY(p_position_id))
        AND (p_company_id IS NULL OR tr.company_id = ANY(p_company_id))
        AND (p_year IS NULL OR dd.year = ANY(p_year))
    GROUP BY
        dd.year,
        tr.employee_id,
        tr.company_id,
        tr.company_name,
        tr.gender,
        tr.position_id
    ORDER BY
        dd.year, tr.employee_id;
END;
$$;
/*
===============================================================================
							HR FUNCTION SAFETY TABLE
===============================================================================
*/
CREATE OR REPLACE FUNCTION gold.func_safety_summary_yearly (
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
    total_accidents INT,
    total_safety_man_hours INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        dd.year,
        sft.employee_id,
        sft.company_id,
        sft.company_name,
        sft.gender,
        sft.position_id,
        COUNT(*)::INT AS total_accidents,
        COALESCE(SUM(sft.safety_man_hours), 0)::INT AS total_safety_man_hours
    FROM gold.dim_employee_safety_description sft
    JOIN gold.dim_date dd ON dd.date_id = sft.date::date
    WHERE
        (p_employee_id IS NULL OR sft.employee_id = p_employee_id)
        AND (p_gender IS NULL OR sft.gender = p_gender)
        AND (p_position_id IS NULL OR sft.position_id = ANY(p_position_id))
        AND (p_company_id IS NULL OR sft.company_id = ANY(p_company_id))
        AND (p_year IS NULL OR dd.year = ANY(p_year))
    GROUP BY
        dd.year,
        sft.employee_id,
        sft.company_id,
        sft.company_name,
        sft.gender,
        sft.position_id
    ORDER BY
        dd.year,
        sft.employee_id;
END;
$$;
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
LANGUAGE plpgsql
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
$$;



/*
===============================================================================
						HR FUNCTION EMPLOYEE SUMMARY TABLE
===============================================================================
*/
DROP FUNCTION IF EXISTS gold.func_employee_summary_monthly;

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
            WHEN AGE(demo.birthdate) < INTERVAL '30 years' THEN 'Young'
            WHEN AGE(demo.birthdate) BETWEEN INTERVAL '30 years' AND INTERVAL '50 years' THEN 'Mid'
            ELSE 'Senior'
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
                WHEN AGE(demo.birthdate) < INTERVAL '30 years' THEN 'Young'
                WHEN AGE(demo.birthdate) BETWEEN INTERVAL '30 years' AND INTERVAL '50 years' THEN 'Mid'
                ELSE 'Senior'
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

DROP FUNCTION IF EXISTS gold.func_parental_leave_summary_monthly;

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

DROP FUNCTION IF EXISTS gold.func_training_summary_monthly;

CREATE OR REPLACE FUNCTION gold.func_training_summary_monthly(
    p_year INT[] DEFAULT NULL,
    p_quarter TEXT[] DEFAULT NULL,
    p_month INT[] DEFAULT NULL,
    p_company_id VARCHAR(6)[] DEFAULT NULL,
    p_position_id VARCHAR(2)[] DEFAULT NULL,
    p_gender VARCHAR(1) DEFAULT NULL
)
RETURNS TABLE (
    hours INT,
    position_id VARCHAR(2),
    month_value INT,
    month_name TEXT,
    year INT,
    quarter TEXT,
    company_id VARCHAR(6),
    p_np VARCHAR(2),
    demo_position_id VARCHAR(2),
    gender VARCHAR(1),
    training_count BIGINT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
      tr.hours,
      tr.position_id,

      dd.month AS month_value,
      TRIM(dd.month_name) AS month_name,
      dd.year,
      dd.quarter::TEXT AS quarter,

      demo.company_id,
      demo.p_np,
      demo.position_id AS demo_position_id,
      demo.gender,

      COUNT(*)::BIGINT AS training_count
    FROM gold.dim_employee_training_description tr
    LEFT JOIN gold.dim_employee_descriptions demo 
      ON demo.employee_id = tr.employee_id
    LEFT JOIN gold.dim_date dd 
      ON dd.date_id = DATE(tr.date)
    WHERE
      (p_year IS NULL OR dd.year = ANY(p_year)) AND
      (p_quarter IS NULL OR dd.quarter::TEXT = ANY(p_quarter)) AND
      (p_month IS NULL OR dd.month = ANY(p_month)) AND
      (p_company_id IS NULL OR demo.company_id = ANY(p_company_id)) AND
      (p_position_id IS NULL OR tr.position_id = ANY(p_position_id)) AND
      (p_gender IS NULL OR demo.gender = p_gender)
    GROUP BY 
      tr.hours,
      dd.year,
      dd.month,
      dd.month_name,
      dd.quarter,
      tr.position_id,
      demo.company_id,
      demo.p_np,
      demo.position_id,
      demo.gender
    ORDER BY 
      dd.year DESC,
      dd.month DESC,
      month_name,
      demo.company_id,
      demo.p_np,
      demo.position_id,
      demo.gender;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS gold.func_safety_summary_monthly;


CREATE OR REPLACE FUNCTION gold.func_safety_summary_monthly(
    p_year INT[] DEFAULT NULL,
    p_quarter TEXT[] DEFAULT NULL,
    p_month INT[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_position_id VARCHAR(2)[] DEFAULT NULL,
    p_gender VARCHAR(1) DEFAULT NULL
)
RETURNS TABLE (
    safety_man_hours INT,
    month INT,
    month_name TEXT,
    year INT,
    quarter TEXT,
    company_id VARCHAR(10),
    p_np VARCHAR(2),
    position_id VARCHAR(2),
    gender VARCHAR(1),
    safety_count BIGINT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
      sft.safety_man_hours,
      dd.month,
      TRIM(dd.month_name),
      dd.year,
      dd.quarter::TEXT,
      demo.company_id,
      demo.p_np,
      demo.position_id,
      demo.gender,
      COUNT(*)::BIGINT AS safety_count
    FROM gold.dim_employee_safety_description sft
    LEFT JOIN gold.dim_employee_descriptions demo ON demo.employee_id = sft.employee_id
    LEFT JOIN gold.dim_date dd ON dd.date_id = DATE(sft.date)
    WHERE
      (p_year IS NULL OR dd.year = ANY(p_year)) AND
      (p_quarter IS NULL OR dd.quarter::TEXT = ANY(p_quarter)) AND
      (p_month IS NULL OR dd.month = ANY(p_month)) AND
      (p_company_id IS NULL OR demo.company_id = ANY(p_company_id)) AND
      (p_position_id IS NULL OR demo.position_id = ANY(p_position_id)) AND
      (p_gender IS NULL OR demo.gender = p_gender)
    GROUP BY
      sft.safety_man_hours,
      dd.year,
      dd.month,
      dd.month_name,
      dd.quarter,
      demo.company_id,
      demo.p_np,
      demo.position_id,
      demo.gender
    ORDER BY
      dd.year DESC,
      dd.month DESC,
      dd.month_name,
      demo.company_id,
      demo.p_np,
      demo.position_id,
      demo.gender;
END;
$$ LANGUAGE plpgsql;