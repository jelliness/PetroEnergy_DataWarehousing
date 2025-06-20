
DROP FUNCTION IF EXISTS gold.func_employee_summary;
DROP FUNCTION IF EXISTS gold.func_training_summary;
DROP FUNCTION IF EXISTS gold.func_safety_workdata_summary;
DROP FUNCTION IF EXISTS gold.func_occupational_safety_health_summary;
DROP FUNCTION IF EXISTS gold.func_hr_rate_summary;
DROP FUNCTION IF EXISTS gold.func_parental_leave_summary;

/*
===============================================================================
						HR FUNCTION EMPLOYEE SUMMARY TABLE
===============================================================================
*/
CREATE OR REPLACE FUNCTION gold.func_employee_summary(
    p_employee_id VARCHAR(20) DEFAULT NULL,
    p_gender VARCHAR(1) DEFAULT NULL,
    p_position_id VARCHAR(2)[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
RETURNS TABLE (
    year INT,
    employee_id VARCHAR(20),
    gender VARCHAR(1),
    position_id VARCHAR(2),
    position_name VARCHAR(100),
    company_id VARCHAR(10),
    company_name VARCHAR(255),
    start_date DATE,
    end_date DATE,
    age_category TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        EXTRACT(YEAR FROM e.start_date)::INT AS year,
        e.employee_id,
        e.gender,
        e.position_id,
        e.position_name,
        e.company_id,
        e.company_name,
        e.start_date::DATE,
        e.end_date::DATE,
        CASE
            WHEN AGE(e.start_date, e.birthdate) < INTERVAL '30 years' THEN 'Under 30'
            WHEN AGE(e.start_date, e.birthdate) BETWEEN INTERVAL '30 years' AND INTERVAL '50 years' THEN '30 to 50'
            ELSE 'Over 50'
        END AS age_category
    FROM gold.dim_employee_descriptions e
    WHERE
        (p_employee_id IS NULL OR e.employee_id = p_employee_id)
        AND (p_gender IS NULL OR e.gender = p_gender)
        AND (p_position_id IS NULL OR e.position_id = ANY(p_position_id))
        AND (p_company_id IS NULL OR e.company_id = ANY(p_company_id))

        -- Include employees who started within the date range
        AND (
            p_start_date IS NULL OR p_end_date IS NULL OR
            e.start_date BETWEEN p_start_date AND p_end_date
        )

        -- Exclude employees who ended within the date range
        AND (
            e.end_date IS NULL OR
            (p_end_date IS NOT NULL AND e.end_date > p_end_date)
        )

    ORDER BY year, employee_id;
END;
$$ LANGUAGE plpgsql;


-- BACKUP
-- CREATE OR REPLACE FUNCTION gold.func_hr_rate_summary (
--     p_year INT[] DEFAULT NULL,
--     p_quarter TEXT[] DEFAULT NULL,
--     p_month INT[] DEFAULT NULL,
--     p_gender VARCHAR(1) DEFAULT NULL,
--     p_position_id VARCHAR(2)[] DEFAULT NULL,
--     p_company_id VARCHAR(10)[] DEFAULT NULL
-- )
-- RETURNS TABLE (
--     year INT,
--     month_value INT,
--     month_name TEXT,
--     quarter TEXT,
--     company_id VARCHAR(10),
--     company_name VARCHAR(255),
--     total_employees INT,
--     avg_tenure NUMERIC(5,2),
--     resigned_count BIGINT
-- ) AS $$
-- BEGIN
-- RETURN QUERY
--     WITH date_filter AS (
--         SELECT *
--         FROM gold.dim_date df
--         WHERE
--             (p_year IS NULL OR df.year = ANY(p_year)) AND
--             (p_month IS NULL OR df.month = ANY(p_month)) AND
--             (p_quarter IS NULL OR CONCAT('Q', df.quarter::TEXT) = ANY(p_quarter))
-- ),
--     employees_filtered AS (
--         SELECT *
--         FROM gold.dim_employee_descriptions d
--         WHERE
--             (p_gender IS NULL OR d.gender = p_gender) AND 
--             (p_position_id IS NULL OR d.position_id = ANY(p_position_id)) AND 
--             (p_company_id IS NULL OR d.company_id = ANY(p_company_id))
--     ),
--     active_employees AS (
--         SELECT
--             d.employee_id,
--             d.company_id,
--             d.company_name,
--             d.tenure_length,
--             df.year,
--             df.month AS month_value,
--             TRIM(df.month_name) AS month_name,
--             CONCAT('Q', df.quarter::TEXT) AS quarter
--         FROM employees_filtered d
--         JOIN date_filter df ON
--             d.start_date <= df.date_id AND
--             (d.end_date IS NULL OR d.end_date >= df.date_id)
--         GROUP BY
--             d.employee_id, d.company_id, d.company_name, d.tenure_length,
--             df.year, df.month, df.month_name, df.quarter, df.date_id
--     ),
--     resignations AS (
--         SELECT
--             d.company_id,
--             df.year,
--             df.month AS month_value,
--             COUNT(DISTINCT d.employee_id) AS resigned_count
--         FROM employees_filtered d
--         JOIN date_filter df ON df.date_id = d.end_date::DATE
--         WHERE d.end_date IS NOT NULL
--         GROUP BY d.company_id, df.year, df.month
--     ),
--     total_active AS (
--         SELECT
--             a.year,
--             a.month_value,
--             a.month_name,
--             a.quarter,
--             a.company_id,
--             a.company_name,
--             COUNT(DISTINCT a.employee_id)::INT AS total_employees,
--             ROUND(AVG(a.tenure_length), 2) AS avg_tenure
--         FROM active_employees a
--         GROUP BY
--             a.year, a.month_value, a.month_name, a.quarter,
--             a.company_id, a.company_name
--     )
--     SELECT
--         t.year,
--         t.month_value,
--         t.month_name,
--         t.quarter,
--         t.company_id,
--         t.company_name,
--         t.total_employees,
--         t.avg_tenure,
--         COALESCE(r.resigned_count, 0) AS resigned_count
--     FROM total_active t
--     LEFT JOIN resignations r ON
--         t.company_id = r.company_id AND
--         t.year = r.year AND
--         t.month_value = r.month_value
--     ORDER BY t.year, t.month_value, t.company_id;
-- END;
-- $$ LANGUAGE plpgsql;

/*
===============================================================================
					HR FUNCTION TRAINING SUMMARY
===============================================================================
*/
CREATE OR REPLACE FUNCTION gold.func_training_summary(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_training_title TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
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
        tr.company_id,
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
        (p_start_date IS NULL OR tr.date >= p_start_date) AND
        (p_end_date IS NULL OR tr.date <= p_end_date) AND
        (p_company_id IS NULL OR tr.company_id = ANY(p_company_id)) AND
        (p_training_title IS NULL OR tr.training_title = ANY(p_training_title))
    GROUP BY
        tr.company_id,
        tr.company_name,
        tr.training_title,
        dd.month,
        dd.month_name,
        dd.year,
        dd.quarter
    ORDER BY
        company_id, company_name, year, month_value;
END;
$$ LANGUAGE plpgsql;

-- BACKUP
-- CREATE OR REPLACE FUNCTION gold.func_training_summary(
--     p_year INT[] DEFAULT NULL,
--     p_quarter TEXT[] DEFAULT NULL,
--     p_month INT[] DEFAULT NULL,
--     p_company_id VARCHAR(10)[] DEFAULT NULL,
--     p_training_title TEXT[] DEFAULT NULL
-- )
-- RETURNS TABLE (
--     company_name VARCHAR(255),
--     training_title TEXT,
--     month_value INT,
--     month_name TEXT,
--     year INT,
--     quarter TEXT,
--     training_hours INT,
--     number_of_participants INT,
--     total_training_hours INT
-- )
-- AS $$
-- BEGIN
--     RETURN QUERY
--     SELECT
--         tr.company_name,
--         tr.training_title,
--         dd.month AS month_value,
--         TRIM(dd.month_name) AS month_name,
--         dd.year,
--         CONCAT('Q', dd.quarter::TEXT) AS quarter,
--         SUM(tr.training_hours)::INT AS training_hours,
--         SUM(tr.number_of_participants)::INT AS number_of_participants,
--         SUM(tr.total_training_hours)::INT AS total_training_hours
--     FROM gold.dim_employee_training_description tr
--     JOIN gold.dim_date dd ON dd.date_id = tr.date::DATE
--     WHERE 
--         (p_year IS NULL OR dd.year = ANY(p_year)) AND
--         (p_quarter IS NULL OR CONCAT('Q', dd.quarter::TEXT) = ANY(p_quarter)) AND
--         (p_month IS NULL OR dd.month = ANY(p_month)) AND
--         (p_company_id IS NULL OR tr.company_id = ANY(p_company_id)) AND
--         (p_training_title IS NULL OR tr.training_title = ANY(p_training_title))
--     GROUP BY
--         tr.company_name,
--         tr.training_title,
--         dd.month,
--         dd.month_name,
--         dd.year,
--         dd.quarter
--     ORDER BY
--         company_name, year, month_value;
-- END;
-- $$ LANGUAGE plpgsql;

/*
===============================================================================
							HR FUNCTION SAFETY TABLE
===============================================================================
*/
CREATE OR REPLACE FUNCTION gold.func_safety_workdata_summary(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_contractor TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
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
            sft.company_id,
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
            (p_start_date IS NULL OR sft.date >= p_start_date) AND
            (p_end_date IS NULL OR sft.date <= p_end_date) AND
            (p_company_id IS NULL OR sft.company_id = ANY(p_company_id)) AND
            (p_contractor IS NULL OR sft.contractor = ANY(p_contractor))
    )
    SELECT
        filtered.company_id,
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
                PARTITION BY filtered.company_id, filtered.company_name, filtered.contractor
                ORDER BY filtered.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0
        ) AS previous_manhours,
        SUM(filtered.manhours) OVER (
            PARTITION BY filtered.company_id, filtered.company_name, filtered.contractor
            ORDER BY filtered.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_manhours
    FROM filtered
    ORDER BY filtered.company_id, filtered.company_name, filtered.year, filtered.month_value, filtered.contractor, filtered.date;
END;
$$ LANGUAGE plpgsql;

-- BACKUP
-- CREATE OR REPLACE FUNCTION gold.func_safety_workdata_summary(
--     p_year INT[] DEFAULT NULL,
--     p_quarter TEXT[] DEFAULT NULL,
--     p_month INT[] DEFAULT NULL,
--     p_company_id VARCHAR(10)[] DEFAULT NULL,
--     p_contractor TEXT[] DEFAULT NULL
-- )
-- RETURNS TABLE (
--     company_name VARCHAR(255),
--     contractor TEXT,
--     month_value INT,
--     month_name TEXT,
--     year INT,
--     quarter TEXT,
--     manpower INT,
--     manhours INT,
--     previous_manhours BIGINT,
--     cumulative_manhours BIGINT
-- )
-- AS $$
-- BEGIN
--     RETURN QUERY
--     WITH filtered AS (
--         SELECT
--             sft.company_name,
--             sft.contractor,
--             dd.month AS month_value,
--             TRIM(dd.month_name) AS month_name,
--             dd.year,
--             CONCAT('Q', dd.quarter::TEXT) AS quarter,
--             sft.manpower,
--             sft.manhours,
--             sft.date
--         FROM gold.dim_employee_safety_manhours_description sft
--         JOIN gold.dim_date dd ON dd.date_id = sft.date::DATE
--         WHERE
--             (p_year IS NULL OR dd.year = ANY(p_year)) AND
--             (p_quarter IS NULL OR CONCAT('Q', dd.quarter::TEXT) = ANY(p_quarter)) AND
--             (p_month IS NULL OR dd.month = ANY(p_month)) AND
--             (p_company_id IS NULL OR sft.company_id = ANY(p_company_id)) AND
--             (p_contractor IS NULL OR sft.contractor = ANY(p_contractor))
--     )
--     SELECT
--         filtered.company_name,
--         filtered.contractor,
--         filtered.month_value,
--         filtered.month_name,
--         filtered.year,
--         filtered.quarter,
--         filtered.manpower,
--         filtered.manhours,
--         COALESCE(
--             SUM(filtered.manhours) OVER (
--                 PARTITION BY filtered.company_name, filtered.contractor
--                 ORDER BY filtered.date
--                 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
--             ), 0
--         ) AS previous_manhours,
--         SUM(filtered.manhours) OVER (
--             PARTITION BY filtered.company_name, filtered.contractor
--             ORDER BY filtered.date
--             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
--         ) AS cumulative_manhours
--     FROM filtered
--     ORDER BY filtered.company_name, filtered.year, filtered.month_value, filtered.contractor, filtered.date;
-- END;
-- $$ LANGUAGE plpgsql;

/*
===============================================================================
					HR OCCUPATIONAL SAFETY HEALTH TABLE
===============================================================================
*/
CREATE OR REPLACE FUNCTION gold.func_occupational_safety_health_summary(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_workforce_type TEXT[] DEFAULT NULL,
    p_lost_time BOOLEAN DEFAULT NULL,
    p_incident_type TEXT[] DEFAULT NULL,
    p_incident_title TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
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
        osh.company_id,
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
        (p_start_date IS NULL OR osh.date >= p_start_date) AND
        (p_end_date IS NULL OR osh.date <= p_end_date) AND
        (p_company_id IS NULL OR osh.company_id = ANY(p_company_id)) AND
        (p_workforce_type IS NULL OR osh.workforce_type = ANY(p_workforce_type)) AND
        (p_lost_time IS NULL OR osh.lost_time = p_lost_time) AND
        (p_incident_type IS NULL OR osh.incident_type = ANY(p_incident_type)) AND
        (p_incident_title IS NULL OR osh.incident_title = ANY(p_incident_title))
    ORDER BY
        company_id, company_name, year, month_value, workforce_type;
END;
$$ LANGUAGE plpgsql;

-- BACKUP
-- CREATE OR REPLACE FUNCTION gold.func_occupational_safety_health_summary(
--     p_year INT[] DEFAULT NULL,
--     p_quarter TEXT[] DEFAULT NULL,
--     p_month INT[] DEFAULT NULL,
--     p_company_id VARCHAR(10)[] DEFAULT NULL,
--     p_workforce_type TEXT[] DEFAULT NULL,
--     p_lost_time BOOLEAN DEFAULT NULL,
--     p_incident_type TEXT[] DEFAULT NULL,
--     p_incident_title TEXT[] DEFAULT NULL
-- )
-- RETURNS TABLE (
--     company_name VARCHAR(255),
--     workforce_type TEXT,
--     month_value INT,
--     month_name TEXT,
--     year INT,
--     quarter TEXT,
--     lost_time BOOLEAN,
--     incident_type TEXT,
--     incident_title TEXT,
--     incident_count INT
-- )
-- AS $$
-- BEGIN
--     RETURN QUERY
--     SELECT
--         osh.company_name,
--         osh.workforce_type,
--         dd.month AS month_value,
--         TRIM(dd.month_name) AS month_name,
--         dd.year,
--         CONCAT('Q', dd.quarter::TEXT) AS quarter,
--         osh.lost_time,
--         osh.incident_type,
--         osh.incident_title,
--         osh.incident_count
--     FROM gold.dim_occupational_safety_health osh
--     JOIN gold.dim_date dd ON dd.date_id = osh.date::DATE
--     WHERE
--         (p_year IS NULL OR dd.year = ANY(p_year)) AND
--         (p_quarter IS NULL OR CONCAT('Q', dd.quarter::TEXT) = ANY(p_quarter)) AND
--         (p_month IS NULL OR dd.month = ANY(p_month)) AND
--         (p_company_id IS NULL OR osh.company_id = ANY(p_company_id)) AND
--         (p_workforce_type IS NULL OR osh.workforce_type = ANY(p_workforce_type)) AND
--         (p_lost_time IS NULL OR osh.lost_time = p_lost_time) AND
--         (p_incident_type IS NULL OR osh.incident_type = ANY(p_incident_type)) AND
--         (p_incident_title IS NULL OR osh.incident_title = ANY(p_incident_title))
--     ORDER BY
--         company_name, year, month_value, workforce_type;
-- END;
-- $$ LANGUAGE plpgsql;

/*
===============================================================================
						HR FUNCTION PARENTAL LEAVE TABLE
===============================================================================
*/
CREATE OR REPLACE FUNCTION gold.func_parental_leave_summary (
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL,
    p_employee_id VARCHAR DEFAULT NULL,
    p_gender VARCHAR DEFAULT NULL,
    p_position_id VARCHAR[] DEFAULT NULL,
    p_company_id VARCHAR[] DEFAULT NULL
)
RETURNS TABLE (
    year INT,
    month_value INT,
    month_name TEXT,
    quarter TEXT,
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
        dd.month AS month_value,
        TRIM(dd.month_name) AS month_name,
        CONCAT('Q', dd.quarter::TEXT) AS quarter,
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
    JOIN gold.dim_date dd ON dd.date_id = pl.date::DATE
    WHERE
        (p_start_date IS NULL OR pl.date >= p_start_date)
        AND (p_end_date IS NULL OR pl.date <= p_end_date)
        AND (p_employee_id IS NULL OR pl.employee_id = p_employee_id)
        AND (p_gender IS NULL OR pl.gender = p_gender)
        AND (p_position_id IS NULL OR pl.position_id = ANY(p_position_id))
        AND (p_company_id IS NULL OR pl.company_id = ANY(p_company_id))
    GROUP BY
        dd.year,
        dd.month,
        dd.month_name,
        dd.quarter,
        pl.employee_id,
        pl.company_id,
        pl.company_name,
        pl.gender,
        pl.position_id,
        pl.type_of_leave
    ORDER BY
        dd.year, dd.month, pl.employee_id;
END;
$$ LANGUAGE plpgsql;


-- BACKUP
-- CREATE OR REPLACE FUNCTION gold.func_parental_leave_summary (
--     p_year INT[] DEFAULT NULL,
--     p_quarter TEXT[] DEFAULT NULL,
--     p_month INT[] DEFAULT NULL,
--     p_employee_id VARCHAR DEFAULT NULL,
--     p_gender VARCHAR DEFAULT NULL,
--     p_position_id VARCHAR[] DEFAULT NULL,
--     p_company_id VARCHAR[] DEFAULT NULL
-- )
-- RETURNS TABLE (
--     year INT,
--     month_value INT,
--     month_name TEXT,
--     quarter TEXT,
--     employee_id VARCHAR,
--     company_id VARCHAR,
--     company_name VARCHAR(255),
--     gender VARCHAR,
--     position_id VARCHAR,
-- 	type_of_leave VARCHAR,
--     total_days INT,
--     total_months INT,
--     leave_count INT
-- )
-- AS $$
-- BEGIN
--     RETURN QUERY
--     SELECT
--         dd.year,
--         dd.month AS month_value,
--         TRIM(dd.month_name) AS month_name,
--         CONCAT('Q', dd.quarter::TEXT) AS quarter,
--         pl.employee_id,
--         pl.company_id,
--         pl.company_name,
--         pl.gender,
--         pl.position_id,
-- 		pl.type_of_leave,
--         SUM(pl.days)::INT AS total_days,
--         SUM(pl.months_availed)::INT AS total_months,
--         COUNT(*)::INT AS leave_count
--     FROM gold.dim_employee_parental_leave_description pl
--     JOIN gold.dim_date dd ON dd.date_id = pl.date::date
--     WHERE
--         (p_employee_id IS NULL OR pl.employee_id = p_employee_id)
--         AND (p_gender IS NULL OR pl.gender = p_gender)
--         AND (p_position_id IS NULL OR pl.position_id = ANY(p_position_id))
--         AND (p_company_id IS NULL OR pl.company_id = ANY(p_company_id))
--         AND (p_year IS NULL OR dd.year = ANY(p_year))
--     GROUP BY
--         dd.year,
--         dd.month,
--         dd.month_name,
--         dd.quarter,
--         pl.employee_id,
--         pl.company_id,
--         pl.company_name,
--         pl.gender,
--         pl.position_id,
-- 		pl.type_of_leave
--     ORDER BY
--         dd.year, dd.month, pl.employee_id;
-- END;
-- $$ LANGUAGE plpgsql;