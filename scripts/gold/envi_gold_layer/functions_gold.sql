/*
===============================================================================
Function Script: Create Gold Functions for Environment Data
===============================================================================
*/
-- =============================================================================
-- NOTE: Execute the create functions first, then execute the sample queries for checking
-- =============================================================================

-- =============================================================================
-- Create Functions for gold.func_environment_water_abstraction
-- =============================================================================
-- DROP FUNCTION IF EXISTS gold.func_environment_water_abstraction_by_year;
DROP FUNCTION IF EXISTS gold.func_environment_water_abstraction_by_year;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_water_abstraction_by_year(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    year SMALLINT,
    total_volume NUMERIC(10,2),  -- updated data type for 2-decimal precision
    unit VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ewa.company_id,
        ewa.year,
        CAST(SUM(ewa.volume) AS NUMERIC(10,2)) AS total_volume,
        ewa.unit
    FROM gold.vw_environment_water_abstraction ewa
    WHERE (p_company_id IS NULL OR ewa.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR ewa.year = ANY(p_year))
      AND (p_quarter IS NULL OR ewa.quarter = ANY(p_quarter))
      AND TRIM(LOWER(ewa.status_name)) = 'approved'
    GROUP BY 
        ewa.company_id, 
        ewa.year,
        ewa.unit
    ORDER BY 
        ewa.company_id, 
        ewa.year, 
        ewa.unit;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_water_abstraction_by_quarter;
DROP FUNCTION IF EXISTS gold.func_environment_water_abstraction_by_quarter;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_water_abstraction_by_quarter(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    year SMALLINT,
	quarter VARCHAR(2),
    total_volume NUMERIC(10,2),  -- updated data type for 2-decimal precision
    unit VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ewa.company_id,
        ewa.year,
		ewa.quarter,
        CAST(SUM(ewa.volume) AS NUMERIC(10,2)) AS total_volume,
        ewa.unit
    FROM gold.vw_environment_water_abstraction ewa
    WHERE (p_company_id IS NULL OR ewa.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR ewa.year = ANY(p_year))
      AND (p_quarter IS NULL OR ewa.quarter = ANY(p_quarter))
      AND TRIM(LOWER(ewa.status_name)) = 'approved'
    GROUP BY 
        ewa.company_id, 
        ewa.year,
		ewa.quarter,
        ewa.unit
    ORDER BY 
        ewa.company_id, 
        ewa.year, 
		ewa.quarter,
        ewa.unit;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_water_abstraction_by_perc_lvl;
DROP FUNCTION IF EXISTS gold.func_environment_water_abstraction_by_perc_lvl;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_water_abstraction_by_perc_lvl(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    total_volume NUMERIC(10,2),  -- updated data type for 2-decimal precision
    unit VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ewa.company_id,
        CAST(SUM(ewa.volume) AS NUMERIC(10,2)) AS total_volume,
        ewa.unit
    FROM gold.vw_environment_water_abstraction ewa
    WHERE (p_company_id IS NULL OR ewa.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR ewa.year = ANY(p_year))
      AND (p_quarter IS NULL OR ewa.quarter = ANY(p_quarter))
      AND TRIM(LOWER(ewa.status_name)) = 'approved'
    GROUP BY 
        ewa.company_id, 
        ewa.unit
    ORDER BY 
        ewa.company_id, 
        ewa.unit;
END;
$$ LANGUAGE plpgsql;


-- =============================================================================
-- Create Functions for gold.func_environment_water_discharge
-- =============================================================================
-- DROP FUNCTION IF EXISTS gold.func_environment_water_discharge_by_year;
DROP FUNCTION IF EXISTS gold.func_environment_water_discharge_by_year;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_water_discharge_by_year(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    year SMALLINT,
    total_volume NUMERIC(10,2),
    unit VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ewd.company_id,
        ewd.year,
        CAST(SUM(ewd.volume) AS NUMERIC(10,2)) AS total_volume,
        ewd.unit
    FROM gold.vw_environment_water_discharge ewd
    WHERE (p_company_id IS NULL OR ewd.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR ewd.year = ANY(p_year))
      AND (p_quarter IS NULL OR ewd.quarter = ANY(p_quarter))
      AND TRIM(LOWER(ewd.status_name)) = 'approved'
    GROUP BY 
        ewd.company_id, 
        ewd.year,
        ewd.unit
    ORDER BY 
        ewd.company_id, 
        ewd.year, 
        ewd.unit;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_water_discharge_by_quarter;
DROP FUNCTION IF EXISTS gold.func_environment_water_discharge_by_quarter;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_water_discharge_by_quarter(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    year SMALLINT,
	quarter VARCHAR(2),
    total_volume NUMERIC(10,2),  -- updated data type for 2-decimal precision
    unit VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ewd.company_id,
        ewd.year,
		ewd.quarter,
        CAST(SUM(ewd.volume) AS NUMERIC(10,2)) AS total_volume,
        ewd.unit
    FROM gold.vw_environment_water_discharge ewd
    WHERE (p_company_id IS NULL OR ewd.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR ewd.year = ANY(p_year))
      AND (p_quarter IS NULL OR ewd.quarter = ANY(p_quarter))
      AND TRIM(LOWER(ewd.status_name)) = 'approved'
    GROUP BY 
        ewd.company_id, 
        ewd.year,
		ewd.quarter,
        ewd.unit
    ORDER BY 
        ewd.company_id, 
        ewd.year, 
		ewd.quarter,
        ewd.unit;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_water_discharge_by_perc_lvl;
DROP FUNCTION IF EXISTS gold.func_environment_water_discharge_by_perc_lvl;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_water_discharge_by_perc_lvl(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    total_volume NUMERIC(10,2),  -- updated data type for 2-decimal precision
    unit VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ewd.company_id,
        CAST(SUM(ewd.volume) AS NUMERIC(10,2)) AS total_volume,
        ewd.unit
    FROM gold.vw_environment_water_discharge ewd
    WHERE (p_company_id IS NULL OR ewd.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR ewd.year = ANY(p_year))
      AND (p_quarter IS NULL OR ewd.quarter = ANY(p_quarter))
      AND TRIM(LOWER(ewd.status_name)) = 'approved'
    GROUP BY 
        ewd.company_id, 
        ewd.unit
    ORDER BY 
        ewd.company_id, 
        ewd.unit;
END;
$$ LANGUAGE plpgsql;


-- =============================================================================
-- Create Functions for gold.func_environment_water_consumption
-- =============================================================================
-- DROP FUNCTION IF EXISTS gold.func_environment_water_consumption_by_year;
DROP FUNCTION IF EXISTS gold.func_environment_water_consumption_by_year;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_water_consumption_by_year(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    year SMALLINT,
    total_volume NUMERIC(10,2),  -- updated data type for 2-decimal precision
    unit VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ewc.company_id,
        ewc.year,
        CAST(SUM(ewc.volume) AS NUMERIC(10,2)) AS total_volume,
        ewc.unit
    FROM gold.vw_environment_water_consumption ewc
    WHERE (p_company_id IS NULL OR ewc.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR ewc.year = ANY(p_year))
      AND (p_quarter IS NULL OR ewc.quarter = ANY(p_quarter))
      AND TRIM(LOWER(ewc.status_name)) = 'approved'
    GROUP BY 
        ewc.company_id, 
        ewc.year,
        ewc.unit
    ORDER BY 
        ewc.company_id, 
        ewc.year, 
        ewc.unit;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_water_consumption_by_quarter;
DROP FUNCTION IF EXISTS gold.func_environment_water_consumption_by_quarter;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_water_consumption_by_quarter(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    year SMALLINT,
	quarter VARCHAR(2),
    total_volume NUMERIC(10,2),  -- updated data type for 2-decimal precision
    unit VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ewc.company_id,
        ewc.year,
		ewc.quarter,
        CAST(SUM(ewc.volume) AS NUMERIC(10,2)) AS total_volume,
        ewc.unit
    FROM gold.vw_environment_water_consumption ewc
    WHERE (p_company_id IS NULL OR ewc.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR ewc.year = ANY(p_year))
      AND (p_quarter IS NULL OR ewc.quarter = ANY(p_quarter))
      AND TRIM(LOWER(ewc.status_name)) = 'approved'
    GROUP BY 
        ewc.company_id, 
        ewc.year,
		ewc.quarter,
        ewc.unit
    ORDER BY 
        ewc.company_id, 
        ewc.year, 
		ewc.quarter,
        ewc.unit;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_water_consumption_by_perc_lvl;
DROP FUNCTION IF EXISTS gold.func_environment_water_consumption_by_perc_lvl;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_water_consumption_by_perc_lvl(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    total_volume NUMERIC(10,2),  -- updated data type for 2-decimal precision
    unit VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ewc.company_id,
        CAST(SUM(ewc.volume) AS NUMERIC(10,2)) AS total_volume,
        ewc.unit
    FROM gold.vw_environment_water_consumption ewc
    WHERE (p_company_id IS NULL OR ewc.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR ewc.year = ANY(p_year))
      AND (p_quarter IS NULL OR ewc.quarter = ANY(p_quarter))
      AND TRIM(LOWER(ewc.status_name)) = 'approved'
    GROUP BY 
        ewc.company_id, 
        ewc.unit
    ORDER BY 
        ewc.company_id, 
        ewc.unit;
END;
$$ LANGUAGE plpgsql;


-- =============================================================================
-- Create Functions for gold.func_environment_water_summary
-- =============================================================================
-- DROP FUNCTION IF EXISTS gold.func_environment_water_summary;
DROP FUNCTION IF EXISTS gold.func_environment_water_summary;

CREATE OR REPLACE FUNCTION gold.func_environment_water_summary(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    year SMALLINT,
    quarter VARCHAR(2),
    total_abstracted_volume NUMERIC(10,2),
    total_discharged_volume NUMERIC(10,2),
    total_consumption_volume NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    WITH abstraction_summary AS (
        SELECT 
            ewa.company_id,
            ewa.year,
            ewa.quarter,
            SUM(ewa.volume) as total_abstracted_volume
        FROM gold.vw_environment_water_abstraction ewa
        WHERE TRIM(LOWER(ewa.status_name)) = 'approved'
        GROUP BY ewa.company_id, ewa.year, ewa.quarter
    ),
    discharge_summary AS (
        SELECT 
            ewd.company_id,
            ewd.year,
            ewd.quarter,
            SUM(ewd.volume) as total_discharged_volume
        FROM gold.vw_environment_water_discharge ewd
        WHERE TRIM(LOWER(ewd.status_name)) = 'approved'
        GROUP BY ewd.company_id, ewd.year, ewd.quarter
    ),
    consumption_summary AS (
        SELECT 
            ewc.company_id,
            ewc.year,
            ewc.quarter,
            SUM(ewc.volume) as total_consumption_volume
        FROM gold.vw_environment_water_consumption ewc
        WHERE TRIM(LOWER(ewc.status_name)) = 'approved'
        GROUP BY ewc.company_id, ewc.year, ewc.quarter
    ),
    all_combinations AS (
        SELECT abs_s.company_id, abs_s.year, abs_s.quarter FROM abstraction_summary abs_s
        UNION
        SELECT dis_s.company_id, dis_s.year, dis_s.quarter FROM discharge_summary dis_s
        UNION
        SELECT con_s.company_id, con_s.year, con_s.quarter FROM consumption_summary con_s
    )
    SELECT
        ac.company_id,
        ac.year,
        ac.quarter,
        CAST(COALESCE(abs.total_abstracted_volume, 0) AS NUMERIC(10,2)) AS total_abstracted_volume,
        CAST(COALESCE(dis.total_discharged_volume, 0) AS NUMERIC(10,2)) AS total_discharged_volume,
        CAST(COALESCE(con.total_consumption_volume, 0) AS NUMERIC(10,2)) AS total_consumption_volume
    FROM all_combinations ac
    LEFT JOIN abstraction_summary abs ON abs.company_id = ac.company_id AND abs.year = ac.year AND abs.quarter = ac.quarter
    LEFT JOIN discharge_summary dis ON dis.company_id = ac.company_id AND dis.year = ac.year AND dis.quarter = ac.quarter
    LEFT JOIN consumption_summary con ON con.company_id = ac.company_id AND con.year = ac.year AND con.quarter = ac.quarter
    WHERE (p_company_id IS NULL OR ac.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR ac.year = ANY(p_year))
      AND (p_quarter IS NULL OR ac.quarter = ANY(p_quarter))
    ORDER BY 
        ac.company_id, 
        ac.year,
        ac.quarter;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Create Function: gold.func_environment_diesel_consumption
-- =============================================================================
-- DROP FUNCTION IF EXISTS gold.func_environment_diesel_consumption_by_year;
DROP FUNCTION IF EXISTS gold.func_environment_diesel_consumption_by_year;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_diesel_consumption_by_year(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
	p_company_property_name VARCHAR(30)[] DEFAULT NULL,
	p_company_property_type VARCHAR(15)[] DEFAULT NULL,
    p_month VARCHAR(10)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
	year SMALLINT,
    company_property_name VARCHAR(30),
	company_property_type VARCHAR(15),
    total_consumption NUMERIC(10,2),
	unit_of_measurement VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        edc.company_id,
		edc.year,
        edc.company_property_name,
		edc.company_property_type,
        CAST(SUM(edc.consumption) AS NUMERIC(10,2)) AS total_consumption,
		edc.unit_of_measurement
    FROM gold.vw_environment_diesel_consumption edc
    WHERE (p_company_id IS NULL OR edc.company_id = ANY(p_company_id))
      AND (p_company_property_name IS NULL OR edc.company_property_name = ANY(p_company_property_name))
	  AND (p_company_property_type IS NULL OR edc.company_property_type = ANY(p_company_property_type))
      AND (p_year IS NULL OR edc.year = ANY(p_year))
      AND (p_quarter IS NULL OR edc.quarter = ANY(p_quarter))
      AND (p_month IS NULL OR edc.month = ANY(p_month))
      AND TRIM(LOWER(edc.status_name)) = 'approved'
    GROUP BY 
        edc.company_id, 
		edc.year,
        edc.company_property_name,
		edc.company_property_type,
		edc.unit_of_measurement
    ORDER BY 
		edc.company_id, 
		edc.year,
		edc.company_property_name, 
		edc.company_property_type, 
		edc.unit_of_measurement;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_diesel_consumption_by_quarter;
DROP FUNCTION IF EXISTS gold.func_environment_diesel_consumption_by_quarter;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_diesel_consumption_by_quarter(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
	p_company_property_name VARCHAR(30)[] DEFAULT NULL,
	p_company_property_type VARCHAR(15)[] DEFAULT NULL,
    p_month VARCHAR(10)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
	year SMALLINT,
	quarter VARCHAR(2),
    company_property_name VARCHAR(30),
	company_property_type VARCHAR(15),
    total_consumption NUMERIC(10,2),
	unit_of_measurement VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        edc.company_id,
		edc.year,
		edc.quarter,
        edc.company_property_name,
		edc.company_property_type,
        CAST(SUM(edc.consumption) AS NUMERIC(10,2)) AS total_consumption,
		edc.unit_of_measurement
    FROM gold.vw_environment_diesel_consumption edc
    WHERE (p_company_id IS NULL OR edc.company_id = ANY(p_company_id))
      AND (p_company_property_name IS NULL OR edc.company_property_name = ANY(p_company_property_name))
	  AND (p_company_property_type IS NULL OR edc.company_property_type = ANY(p_company_property_type))
      AND (p_year IS NULL OR edc.year = ANY(p_year))
      AND (p_quarter IS NULL OR edc.quarter = ANY(p_quarter))
      AND (p_month IS NULL OR edc.month = ANY(p_month))
      AND TRIM(LOWER(edc.status_name)) = 'approved'
    GROUP BY 
        edc.company_id, 
		edc.year,
		edc.quarter,
        edc.company_property_name,
		edc.company_property_type,
		edc.unit_of_measurement
    ORDER BY 
		edc.company_id, 
		edc.year,
		edc.quarter,
		edc.company_property_name, 
		edc.company_property_type, 
		edc.unit_of_measurement;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_diesel_consumption_by_month;
DROP FUNCTION IF EXISTS gold.func_environment_diesel_consumption_by_month;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_diesel_consumption_by_month(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
	p_company_property_name VARCHAR(30)[] DEFAULT NULL,
	p_company_property_type VARCHAR(15)[] DEFAULT NULL,
    p_month VARCHAR(10)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
	year SMALLINT,
	month VARCHAR(10),
    company_property_name VARCHAR(30),
	company_property_type VARCHAR(15),
    total_consumption NUMERIC(10,2),
	unit_of_measurement VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        edc.company_id,
		edc.year,
		edc.month,
        edc.company_property_name,
		edc.company_property_type,
        CAST(SUM(edc.consumption) AS NUMERIC(10,2)) AS total_consumption,
		edc.unit_of_measurement
    FROM gold.vw_environment_diesel_consumption edc
    WHERE (p_company_id IS NULL OR edc.company_id = ANY(p_company_id))
      AND (p_company_property_name IS NULL OR edc.company_property_name = ANY(p_company_property_name))
	  AND (p_company_property_type IS NULL OR edc.company_property_type = ANY(p_company_property_type))
      AND (p_year IS NULL OR edc.year = ANY(p_year))
      AND (p_quarter IS NULL OR edc.quarter = ANY(p_quarter))
      AND (p_month IS NULL OR edc.month = ANY(p_month))
      AND TRIM(LOWER(edc.status_name)) = 'approved'
    GROUP BY 
        edc.company_id, 
		edc.year,
		edc.month,
        edc.company_property_name,
		edc.company_property_type,
		edc.unit_of_measurement
    ORDER BY 
		edc.company_id, 
		edc.year,
		edc.month,
		edc.company_property_name, 
		edc.company_property_type, 
		edc.unit_of_measurement;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_diesel_consumption_by_cp_name;
DROP FUNCTION IF EXISTS gold.func_environment_diesel_consumption_by_cp_name;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_diesel_consumption_by_cp_name(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
	p_company_property_name VARCHAR(30)[] DEFAULT NULL,
	p_company_property_type VARCHAR(15)[] DEFAULT NULL,
    p_month VARCHAR(10)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    company_property_name VARCHAR(30),
    total_consumption NUMERIC(10,2),
	unit_of_measurement VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        edc.company_id,
        edc.company_property_name,
        CAST(SUM(edc.consumption) AS NUMERIC(10,2)) AS total_consumption,
		edc.unit_of_measurement
    FROM gold.vw_environment_diesel_consumption edc
    WHERE (p_company_id IS NULL OR edc.company_id = ANY(p_company_id))
      AND (p_company_property_name IS NULL OR edc.company_property_name = ANY(p_company_property_name))
	  AND (p_company_property_type IS NULL OR edc.company_property_type = ANY(p_company_property_type))
      AND (p_year IS NULL OR edc.year = ANY(p_year))
      AND (p_quarter IS NULL OR edc.quarter = ANY(p_quarter))
      AND (p_month IS NULL OR edc.month = ANY(p_month))
      AND TRIM(LOWER(edc.status_name)) = 'approved'
    GROUP BY 
        edc.company_id, 
        edc.company_property_name,
		edc.unit_of_measurement
    ORDER BY 
		edc.company_id, 
		edc.company_property_name, 
		edc.unit_of_measurement;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_diesel_consumption_by_cp_type;
DROP FUNCTION IF EXISTS gold.func_environment_diesel_consumption_by_cp_type;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_diesel_consumption_by_cp_type(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
	p_company_property_name VARCHAR(30)[] DEFAULT NULL,
	p_company_property_type VARCHAR(15)[] DEFAULT NULL,
    p_month VARCHAR(10)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
	company_property_type VARCHAR(15),
    total_consumption NUMERIC(10,2),
	unit_of_measurement VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        edc.company_id,
		edc.company_property_type,
        CAST(SUM(edc.consumption) AS NUMERIC(10,2)) AS total_consumption,
		edc.unit_of_measurement
    FROM gold.vw_environment_diesel_consumption edc
    WHERE (p_company_id IS NULL OR edc.company_id = ANY(p_company_id))
      AND (p_company_property_name IS NULL OR edc.company_property_name = ANY(p_company_property_name))
	  AND (p_company_property_type IS NULL OR edc.company_property_type = ANY(p_company_property_type))
      AND (p_year IS NULL OR edc.year = ANY(p_year))
      AND (p_quarter IS NULL OR edc.quarter = ANY(p_quarter))
      AND (p_month IS NULL OR edc.month = ANY(p_month))
      AND TRIM(LOWER(edc.status_name)) = 'approved'
    GROUP BY 
        edc.company_id, 
		edc.company_property_type,
		edc.unit_of_measurement
    ORDER BY 
		edc.company_id, 
		edc.company_property_type, 
		edc.unit_of_measurement;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_diesel_consumption_by_perc_lvl;
DROP FUNCTION IF EXISTS gold.func_environment_diesel_consumption_by_perc_lvl;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_diesel_consumption_by_perc_lvl(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
	p_company_property_name VARCHAR(30)[] DEFAULT NULL,
	p_company_property_type VARCHAR(15)[] DEFAULT NULL,
    p_month VARCHAR(10)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    total_consumption NUMERIC(10,2),
	unit_of_measurement VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        edc.company_id,
        CAST(SUM(edc.consumption) AS NUMERIC(10,2)) AS total_consumption,
		edc.unit_of_measurement
    FROM gold.vw_environment_diesel_consumption edc
    WHERE (p_company_id IS NULL OR edc.company_id = ANY(p_company_id))
      AND (p_company_property_name IS NULL OR edc.company_property_name = ANY(p_company_property_name))
	  AND (p_company_property_type IS NULL OR edc.company_property_type = ANY(p_company_property_type))
      AND (p_year IS NULL OR edc.year = ANY(p_year))
      AND (p_quarter IS NULL OR edc.quarter = ANY(p_quarter))
      AND (p_month IS NULL OR edc.month = ANY(p_month))
      AND TRIM(LOWER(edc.status_name)) = 'approved'
    GROUP BY 
        edc.company_id, 
		edc.unit_of_measurement
    ORDER BY 
		edc.company_id, 
		edc.unit_of_measurement;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Create Functions for gold.func_environment_electric_consumption
-- =============================================================================

-- DROP FUNCTION IF EXISTS gold.func_environment_electric_consumption;
DROP FUNCTION IF EXISTS gold.func_environment_electric_consumption_by_year;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_electric_consumption_by_year(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_consumption_source VARCHAR(30)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
	year SMALLINT,
    unit_of_measurement VARCHAR(15),
    total_consumption NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ec.company_id,
		ec.year,
        ec.unit_of_measurement,
        CAST(SUM(ec.consumption) AS NUMERIC(10,2)) AS total_consumption
    FROM gold.vw_environment_electric_consumption ec
    WHERE (p_company_id IS NULL OR ec.company_id = ANY(p_company_id))
	  AND (p_consumption_source IS NULL OR ec.consumption_source = ANY(p_consumption_source))
      AND (p_year IS NULL OR ec.year = ANY(p_year))
      AND (p_quarter IS NULL OR ec.quarter = ANY(p_quarter))
      AND TRIM(LOWER(ec.status_name)) = 'approved'
    GROUP BY ec.company_id, ec.year, ec.unit_of_measurement
    ORDER BY ec.company_id;
END;
$$ LANGUAGE plpgsql;


-- DROP FUNCTION IF EXISTS gold.func_environment_electric_consumption;
DROP FUNCTION IF EXISTS gold.func_environment_electric_consumption_by_quarter;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_electric_consumption_by_quarter(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_consumption_source VARCHAR(30)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
	year SMALLINT,
    quarter VARCHAR(2),
    unit_of_measurement VARCHAR(15),
    total_consumption NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ec.company_id,
		ec.year,
        ec.quarter,
        ec.unit_of_measurement,
        CAST(SUM(ec.consumption) AS NUMERIC(10,2)) AS total_consumption
    FROM gold.vw_environment_electric_consumption ec
    WHERE (p_company_id IS NULL OR ec.company_id = ANY(p_company_id))
	  AND (p_consumption_source IS NULL OR ec.consumption_source = ANY(p_consumption_source))
      AND (p_year IS NULL OR ec.year = ANY(p_year))
      AND (p_quarter IS NULL OR ec.quarter = ANY(p_quarter))
      AND TRIM(LOWER(ec.status_name)) = 'approved'
    GROUP BY ec.company_id, ec.year, ec.quarter, ec.unit_of_measurement
    ORDER BY ec.company_id;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_electric_consumption;
DROP FUNCTION IF EXISTS gold.func_environment_electric_consumption_by_source;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_electric_consumption_by_source(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_consumption_source VARCHAR(30)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    consumption_source VARCHAR(30),
    unit_of_measurement VARCHAR(15),
    total_consumption NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ec.company_id,
        ec.consumption_source,
        ec.unit_of_measurement,
        CAST(SUM(ec.consumption) AS NUMERIC(10,2)) AS total_consumption
    FROM gold.vw_environment_electric_consumption ec
    WHERE (p_company_id IS NULL OR ec.company_id = ANY(p_company_id))
	  AND (p_consumption_source IS NULL OR ec.consumption_source = ANY(p_consumption_source))
      AND (p_year IS NULL OR ec.year = ANY(p_year))
      AND (p_quarter IS NULL OR ec.quarter = ANY(p_quarter))
      AND TRIM(LOWER(ec.status_name)) = 'approved'
    GROUP BY ec.company_id, ec.consumption_source, ec.unit_of_measurement
    ORDER BY ec.company_id;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_electric_consumption;
DROP FUNCTION IF EXISTS gold.func_environment_electric_consumption_by_perc_lvl;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_electric_consumption_by_perc_lvl(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_consumption_source VARCHAR(30)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    unit_of_measurement VARCHAR(15),
    total_consumption NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ec.company_id,
        ec.unit_of_measurement,
        CAST(SUM(ec.consumption) AS NUMERIC(10,2)) AS total_consumption
    FROM gold.vw_environment_electric_consumption ec
    WHERE (p_company_id IS NULL OR ec.company_id = ANY(p_company_id))
	  AND (p_consumption_source IS NULL OR ec.consumption_source = ANY(p_consumption_source))
      AND (p_year IS NULL OR ec.year = ANY(p_year))
      AND (p_quarter IS NULL OR ec.quarter = ANY(p_quarter))
      AND TRIM(LOWER(ec.status_name)) = 'approved'
    GROUP BY ec.company_id, ec.unit_of_measurement
    ORDER BY ec.company_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Create Functions for gold.func_environment_non_hazard_waste
-- =============================================================================

-- DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste;
DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste_by_year;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_non_hazard_waste_by_year(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_metrics VARCHAR(20)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL,
    p_unit_of_measurement VARCHAR(15)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    year SMALLINT,
    metrics VARCHAR(20),
    unit_of_measurement VARCHAR(15),
    total_waste NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        nhw.company_id,
        nhw.year,
        nhw.metrics,
        nhw.unit_of_measurement,
        CAST(SUM(nhw.waste) AS NUMERIC(10,2)) AS total_waste
    FROM gold.vw_environment_non_hazard_waste nhw
    WHERE (p_company_id IS NULL OR nhw.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR nhw.year = ANY(p_year))
      AND (p_quarter IS NULL OR nhw.quarter = ANY(p_quarter))
      AND (p_metrics IS NULL OR nhw.metrics = ANY(p_metrics))
      AND (p_unit_of_measurement IS NULL OR nhw.unit_of_measurement = ANY(p_unit_of_measurement))
      AND TRIM(LOWER(nhw.status_name)) = 'approved'
    GROUP BY nhw.company_id, nhw.year, nhw.metrics, nhw.unit_of_measurement
    ORDER BY nhw.company_id;
END;
$$ LANGUAGE plpgsql;


-- DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste;
DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste_by_quarter;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_non_hazard_waste_by_quarter(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_metrics VARCHAR(20)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL,
    p_unit_of_measurement VARCHAR(15)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    year SMALLINT,
    quarter VARCHAR(2),
    metrics VARCHAR(20),
    unit_of_measurement VARCHAR(15),
    total_waste NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        nhw.company_id,
        nhw.year,
        nhw.quarter,
        nhw.metrics,
        nhw.unit_of_measurement,
        CAST(SUM(nhw.waste) AS NUMERIC(10,2)) AS total_waste
    FROM gold.vw_environment_non_hazard_waste nhw
    WHERE (p_company_id IS NULL OR nhw.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR nhw.year = ANY(p_year))
      AND (p_quarter IS NULL OR nhw.quarter = ANY(p_quarter))
      AND (p_metrics IS NULL OR nhw.metrics = ANY(p_metrics))
      AND (p_unit_of_measurement IS NULL OR nhw.unit_of_measurement = ANY(p_unit_of_measurement))
      AND TRIM(LOWER(nhw.status_name)) = 'approved'
    GROUP BY nhw.company_id, nhw.year, nhw.quarter, nhw.metrics, nhw.unit_of_measurement
    ORDER BY nhw.company_id;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste;
DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste_by_metrics;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_non_hazard_waste_by_metrics(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_metrics VARCHAR(20)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL,
    p_unit_of_measurement VARCHAR(15)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    metrics VARCHAR(20),
    unit_of_measurement VARCHAR(15),
    total_waste NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        nhw.company_id,
        nhw.metrics,  
        nhw.unit_of_measurement,
        CAST(SUM(nhw.waste) AS NUMERIC(10,2)) AS total_waste
    FROM gold.vw_environment_non_hazard_waste nhw
    WHERE (p_company_id IS NULL OR nhw.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR nhw.year = ANY(p_year))
      AND (p_quarter IS NULL OR nhw.quarter = ANY(p_quarter))
      AND (p_metrics IS NULL OR nhw.metrics = ANY(p_metrics))
      AND (p_unit_of_measurement IS NULL OR nhw.unit_of_measurement = ANY(p_unit_of_measurement))
      AND TRIM(LOWER(nhw.status_name)) = 'approved'
    GROUP BY nhw.company_id, nhw.metrics, nhw.unit_of_measurement
    ORDER BY nhw.company_id;
END;
$$ LANGUAGE plpgsql;


-- DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste;
DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste_by_perc_lvl;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_non_hazard_waste_by_perc_lvl(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_metrics VARCHAR(20)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL,
    p_unit_of_measurement VARCHAR(15)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    unit_of_measurement VARCHAR(15),
    total_waste NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        nhw.company_id,
        nhw.unit_of_measurement,
        CAST(SUM(nhw.waste) AS NUMERIC(10,2)) AS total_waste
    FROM gold.vw_environment_non_hazard_waste nhw
    WHERE (p_company_id IS NULL OR nhw.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR nhw.year = ANY(p_year))
      AND (p_quarter IS NULL OR nhw.quarter = ANY(p_quarter))
      AND (p_metrics IS NULL OR nhw.metrics = ANY(p_metrics))
      AND (p_unit_of_measurement IS NULL OR nhw.unit_of_measurement = ANY(p_unit_of_measurement))
      AND TRIM(LOWER(nhw.status_name)) = 'approved'
    GROUP BY nhw.company_id, nhw.unit_of_measurement
    ORDER BY nhw.company_id;
END;
$$ LANGUAGE plpgsql;


-- =============================================================================
-- Create Functions for gold.func_environment_hazard_waste_generated
-- =============================================================================
-- DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_generated_by_year;
DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_generated_by_year;
-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_hazard_waste_generated_by_year(
    p_company_id   VARCHAR(10)[] DEFAULT NULL,
    p_year         SMALLINT[] DEFAULT NULL,
    p_quarter      VARCHAR(2)[] DEFAULT NULL,
    p_waste_type   VARCHAR(15)[] DEFAULT NULL,
    p_unit         VARCHAR(15)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id     VARCHAR(10),
    year           SMALLINT,
    waste_type     VARCHAR(15),
    unit           VARCHAR(15),
    total_generate NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        g.company_id,
        g.year,
        g.waste_type,
        g.unit,
        CAST(SUM(g.waste_generated) AS NUMERIC(10,2)) AS total_generate
    FROM gold.vw_environment_hazard_waste_generated g
    WHERE (p_company_id IS NULL OR g.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR g.year = ANY(p_year))
      AND (p_quarter IS NULL OR g.quarter = ANY(p_quarter))
      AND (p_waste_type IS NULL OR g.waste_type = ANY(p_waste_type))
      AND (p_unit IS NULL OR g.unit = ANY(p_unit))
      AND TRIM(LOWER(g.status_name)) = 'approved'
    GROUP BY g.company_id, g.year,g.waste_type, g.unit
    ORDER BY g.company_id, g.year;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_generated_by_quarter;
DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_generated_by_quarter;
-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_hazard_waste_generated_by_quarter(
    p_company_id   VARCHAR(10)[] DEFAULT NULL,
    p_year         SMALLINT[] DEFAULT NULL,
    p_quarter      VARCHAR(2)[] DEFAULT NULL,
    p_waste_type   VARCHAR(15)[] DEFAULT NULL,
    p_unit         VARCHAR(15)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id     VARCHAR(10),
    year           SMALLINT,
    quarter        VARCHAR(2),
    waste_type     VARCHAR(15),
    unit           VARCHAR(15),
    total_generate NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        g.company_id,
        g.year,
        g.quarter,
        g.waste_type,
        g.unit,
        CAST(SUM(g.waste_generated) AS NUMERIC(10,2)) AS total_generate
    FROM gold.vw_environment_hazard_waste_generated g
    WHERE (p_company_id IS NULL OR g.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR g.year = ANY(p_year))
      AND (p_quarter IS NULL OR g.quarter = ANY(p_quarter))
      AND (p_waste_type IS NULL OR g.waste_type = ANY(p_waste_type))
      AND (p_unit IS NULL OR g.unit = ANY(p_unit))
      AND TRIM(LOWER(g.status_name)) = 'approved'
    GROUP BY g.company_id, g.quarter, g.year,g.waste_type, g.unit
    ORDER BY g.company_id;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_generated_by_waste_type;
DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_generated_by_waste_type;
-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_hazard_waste_generated_by_waste_type(
    p_company_id   VARCHAR(10)[] DEFAULT NULL,
    p_year         SMALLINT[] DEFAULT NULL,
    p_quarter      VARCHAR(2)[] DEFAULT NULL,
    p_waste_type   VARCHAR(15)[] DEFAULT NULL,
    p_unit         VARCHAR(15)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id     VARCHAR(10),
    waste_type     VARCHAR(15),
    unit           VARCHAR(15),
    total_generate NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        g.company_id,
        g.waste_type,
        g.unit,
        CAST(SUM(g.waste_generated) AS NUMERIC(10,2)) AS total_generate
    FROM gold.vw_environment_hazard_waste_generated g
    WHERE (p_company_id IS NULL OR g.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR g.year = ANY(p_year))
      AND (p_quarter IS NULL OR g.quarter = ANY(p_quarter))
      AND (p_waste_type IS NULL OR g.waste_type = ANY(p_waste_type))
      AND (p_unit IS NULL OR g.unit = ANY(p_unit))
      AND TRIM(LOWER(g.status_name)) = 'approved'
    GROUP BY g.company_id,g.waste_type, g.unit
    ORDER BY g.company_id;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_generated_by_perc_lvl;
DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_generated_by_perc_lvl;
-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_hazard_waste_generated_by_perc_lvl(
    p_company_id   VARCHAR(10)[] DEFAULT NULL,
    p_year         SMALLINT[] DEFAULT NULL,
    p_quarter      VARCHAR(2)[] DEFAULT NULL,
    p_waste_type   VARCHAR(15)[] DEFAULT NULL,
    p_unit         VARCHAR(15)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id     VARCHAR(10),
    unit           VARCHAR(15),
    total_generate NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        g.company_id,
        g.unit,
        CAST(SUM(g.waste_generated) AS NUMERIC(10,2)) AS total_generate
    FROM gold.vw_environment_hazard_waste_generated g
    WHERE (p_company_id IS NULL OR g.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR g.year = ANY(p_year))
      AND (p_quarter IS NULL OR g.quarter = ANY(p_quarter))
      AND (p_waste_type IS NULL OR g.waste_type = ANY(p_waste_type))
      AND (p_unit IS NULL OR g.unit = ANY(p_unit))
      AND TRIM(LOWER(g.status_name)) = 'approved'
    GROUP BY g.company_id, g.unit
    ORDER BY g.company_id;
END;
$$ LANGUAGE plpgsql;


-- =============================================================================
-- Create Function: gold.func_environment_hazard_waste_disposed
-- =============================================================================
-- DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_disposed_by_year;
DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_disposed_by_year;
-- FUNCTION disposed
CREATE OR REPLACE FUNCTION gold.func_environment_hazard_waste_disposed_by_year(
    p_company_id   VARCHAR(10)[] DEFAULT NULL,
    p_year         SMALLINT[] DEFAULT NULL,
    p_waste_type   VARCHAR(15)[] DEFAULT NULL,
    p_unit         VARCHAR(15)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id      VARCHAR(10),
    year            SMALLINT,
    waste_type      VARCHAR(15),
    unit            VARCHAR(15),
    total_disposed  NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.company_id,
        d.year,
        d.waste_type,
        d.unit,
        CAST(SUM(d.waste_disposed) AS NUMERIC(10,2)) AS total_disposed
    FROM gold.vw_environment_hazard_waste_disposed d
    WHERE (p_company_id IS NULL OR d.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR d.year = ANY(p_year))
      AND (p_waste_type IS NULL OR d.waste_type = ANY(p_waste_type))
      AND (p_unit IS NULL OR d.unit = ANY(p_unit))
      AND TRIM(LOWER(d.status_name)) = 'approved'
    GROUP BY d.company_id, d.year, d.waste_type, d.unit
    ORDER BY d.company_id, d.year;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_disposed_by_waste_type;

CREATE OR REPLACE FUNCTION gold.func_environment_hazard_waste_disposed_by_waste_type(
    p_company_id   VARCHAR(10)[] DEFAULT NULL,
    p_year         SMALLINT[] DEFAULT NULL,
    p_waste_type   VARCHAR(15)[] DEFAULT NULL,
    p_unit         VARCHAR(15)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id      VARCHAR(10),
    waste_type      VARCHAR(15),
    unit            VARCHAR(15),
    total_disposed  NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.company_id,
        d.waste_type,
        d.unit,
        CAST(SUM(d.waste_disposed) AS NUMERIC(10,2)) AS total_disposed
    FROM gold.vw_environment_hazard_waste_disposed d
    WHERE (p_company_id IS NULL OR d.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR d.year = ANY(p_year))
      AND (p_waste_type IS NULL OR d.waste_type = ANY(p_waste_type))
      AND (p_unit IS NULL OR d.unit = ANY(p_unit))
      AND TRIM(LOWER(d.status_name)) = 'approved'
    GROUP BY d.company_id, d.waste_type, d.unit
    ORDER BY d.company_id;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_disposed_by_perc_lvl;

CREATE OR REPLACE FUNCTION gold.func_environment_hazard_waste_disposed_by_perc_lvl(
    p_company_id   VARCHAR(10)[] DEFAULT NULL,
    p_year         SMALLINT[] DEFAULT NULL,
    p_waste_type   VARCHAR(15)[] DEFAULT NULL,
    p_unit         VARCHAR(15)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id      VARCHAR(10),
    unit            VARCHAR(15),
    total_disposed  NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.company_id,
        d.unit,
        CAST(SUM(d.waste_disposed) AS NUMERIC(10,2)) AS total_disposed
    FROM gold.vw_environment_hazard_waste_disposed d
    WHERE (p_company_id IS NULL OR d.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR d.year = ANY(p_year))
      AND (p_waste_type IS NULL OR d.waste_type = ANY(p_waste_type))
      AND (p_unit IS NULL OR d.unit = ANY(p_unit))
      AND TRIM(LOWER(d.status_name)) = 'approved'
    GROUP BY d.company_id, d.unit
    ORDER BY d.company_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Sample Query: Execute this queries to test the functions
-- =============================================================================

-- SAMPLE QUERIES FOR WATER WITHDRAWAL
SELECT * FROM gold.func_environment_water_abstraction_by_year(
    NULL, 
    NULL,
	NULL
);
SELECT * FROM gold.func_environment_water_abstraction_by_quarter(
    NULL, 
    NULL,
	NULL
);
SELECT * FROM gold.func_environment_water_abstraction_by_perc_lvl(
    NULL, 
    NULL,
	NULL
);
SELECT * FROM gold.func_environment_water_discharge_by_year(
    NULL, 
    NULL,
	NULL
);
SELECT * FROM gold.func_environment_water_discharge_by_quarter(
    NULL, 
    NULL,
	NULL
);
SELECT * FROM gold.func_environment_water_discharge_by_perc_lvl(
    NULL, 
    NULL,
	NULL
);
SELECT * FROM gold.func_environment_water_consumption_by_year(
    NULL, 
    NULL,
	NULL
);
SELECT * FROM gold.func_environment_water_consumption_by_quarter(
    NULL, 
    NULL,
	NULL
);
SELECT * FROM gold.func_environment_water_consumption_by_perc_lvl(
    NULL, 
    NULL,
	NULL
);
SELECT * FROM gold.func_environment_water_summary(
   NULL,
   NULL,
   NULL
);

-- SAMPLE QUERIES FOR DIESEL CONSUMPTION
SELECT * FROM gold.func_environment_diesel_consumption_by_year(
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    NULL
);
SELECT * FROM gold.func_environment_diesel_consumption_by_quarter(
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    NULL
);
SELECT * FROM gold.func_environment_diesel_consumption_by_month(
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    NULL
);
SELECT * FROM gold.func_environment_diesel_consumption_by_cp_name(
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    NULL
);
SELECT * FROM gold.func_environment_diesel_consumption_by_cp_type(
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    NULL
);
SELECT * FROM gold.func_environment_diesel_consumption_by_perc_lvl(
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    NULL
);

-- SAMPLE QUERIES FOR ELECTRIC CONSUMPTION
SELECT * FROM gold.func_environment_electric_consumption_by_year(
    NULL, 
    NULL, 
    NULL
);

SELECT * FROM gold.func_environment_electric_consumption_by_quarter(
    NULL, 
    NULL, 
    NULL
);

SELECT * FROM gold.func_environment_electric_consumption_by_perc_lvl(
    NULL, 
    NULL, 
    NULL
);

-- SAMPLE QUERIES FOR NON HAZARD WASTE GENERATED
SELECT * FROM gold.func_environment_non_hazard_waste_by_year(
    NULL, 
    NULL, 
    NULL, 
    NULL
);

SELECT * FROM gold.func_environment_non_hazard_waste_by_quarter(
    NULL, 
    NULL, 
    NULL, 
    NULL
);

SELECT * FROM gold.func_environment_non_hazard_waste_by_metrics(
    NULL, 
    NULL, 
    NULL, 
    NULL
);

SELECT * FROM gold.func_environment_non_hazard_waste_by_perc_lvl(
    NULL, 
    NULL, 
    NULL, 
    NULL
);

-- SAMPLE QUERIES FOR WASTE GENERATED
SELECT * FROM gold.func_environment_hazard_waste_generated_by_year(
    NULL,
    NULL,
    NULL,
    NULL
);
SELECT * FROM gold.func_environment_hazard_waste_generated_by_quarter(
    NULL,
    NULL,
    NULL,
    NULL
);
SELECT * FROM gold.func_environment_hazard_waste_generated_by_waste_type(
    NULL,
    NULL,
    NULL,
    NULL
);
SELECT * FROM gold.func_environment_hazard_waste_generated_by_perc_lvl(
    NULL,
    NULL,
    NULL,
    NULL
);

-- SAMPLE QUERIES FOR WASTE DISPOSED
SELECT * FROM gold.func_environment_hazard_waste_disposed_by_year(
    NULL,
    NULL,
    NULL
);

SELECT * FROM gold.func_environment_hazard_waste_disposed_by_waste_type(
    NULL,
    NULL,
    NULL
);

SELECT * FROM gold.func_environment_hazard_waste_disposed_by_perc_lvl(
    NULL,
    NULL,
    NULL
);