/*
===============================================================================
Function Script: Create Gold Functions for Environment Data
===============================================================================
*/
-- =============================================================================
-- NOTE: Execute the create functions first, then execute the sample queries for checking
-- =============================================================================

-- =============================================================================
-- Create Functions for gold.func_environment_water_withdrawal
-- =============================================================================
-- DROP FUNCTION IF EXISTS gold.func_environment_water_withdrawal_by_year;
DROP FUNCTION IF EXISTS gold.func_environment_water_withdrawal_by_year;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_water_withdrawal_by_year(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_natural_sources VARCHAR(30)[] DEFAULT NULL,
    p_month VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    year SMALLINT,
    natural_sources VARCHAR(30),
    total_volume NUMERIC(10,2),  -- updated data type for 2-decimal precision
    unit_of_measurement VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        eww.company_id,
        eww.year,
        eww.natural_sources,
        CAST(SUM(eww.water_volume) AS NUMERIC(10,2)) AS total_volume,
        eww.unit_of_measurement
    FROM gold.vw_environment_water_withdrawal eww
    WHERE (p_company_id IS NULL OR eww.company_id = ANY(p_company_id))
      AND (p_natural_sources IS NULL OR eww.natural_sources = ANY(p_natural_sources))
      AND (p_year IS NULL OR eww.year = ANY(p_year))
      AND (p_quarter IS NULL OR eww.quarter = ANY(p_quarter))
      AND (p_month IS NULL OR eww.month = ANY(p_month))
    GROUP BY 
        eww.company_id, 
        eww.year,
        eww.natural_sources,
        eww.unit_of_measurement
    ORDER BY 
        eww.company_id, 
        eww.year, 
        eww.natural_sources, 
        eww.unit_of_measurement;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_water_withdrawal_by_quarter;
DROP FUNCTION IF EXISTS gold.func_environment_water_withdrawal_by_quarter;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_water_withdrawal_by_quarter(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_natural_sources VARCHAR(30)[] DEFAULT NULL,
    p_month VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
	year SMALLINT,
    quarter VARCHAR(2),
	natural_sources VARCHAR(30),
    total_volume NUMERIC(10,2),  -- updated data type for 2-decimal precision
    unit_of_measurement VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        eww.company_id,
		eww.year,
        eww.quarter,
		eww.natural_sources,
        CAST(SUM(eww.water_volume) AS NUMERIC(10,2)) AS total_volume,
        eww.unit_of_measurement
    FROM gold.vw_environment_water_withdrawal eww
    WHERE (p_company_id IS NULL OR eww.company_id = ANY(p_company_id))
      AND (p_natural_sources IS NULL OR eww.natural_sources = ANY(p_natural_sources))
      AND (p_year IS NULL OR eww.year = ANY(p_year))
      AND (p_quarter IS NULL OR eww.quarter = ANY(p_quarter))
      AND (p_month IS NULL OR eww.month = ANY(p_month))
    GROUP BY 
        eww.company_id, 
        eww.year,
        eww.quarter,
		eww.natural_sources,
        eww.unit_of_measurement
    ORDER BY 
        eww.company_id, 
        eww.year, 
       	eww.quarter, 
		   eww.natural_sources,
        eww.unit_of_measurement;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_water_withdrawal_by_month;
DROP FUNCTION IF EXISTS gold.func_environment_water_withdrawal_by_month;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_water_withdrawal_by_month(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_natural_sources VARCHAR(30)[] DEFAULT NULL,
    p_month VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    year SMALLINT,
    month VARCHAR(10),
	natural_sources VARCHAR(30),
    total_volume NUMERIC(10,2),  -- updated data type for 2-decimal precision
    unit_of_measurement VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        eww.company_id,
        eww.year,
        eww.month,
		eww.natural_sources,
        CAST(SUM(eww.water_volume) AS NUMERIC(10,2)) AS total_volume,
        eww.unit_of_measurement
    FROM gold.vw_environment_water_withdrawal eww
    WHERE (p_company_id IS NULL OR eww.company_id = ANY(p_company_id))
      AND (p_natural_sources IS NULL OR eww.natural_sources = ANY(p_natural_sources))
      AND (p_year IS NULL OR eww.year = ANY(p_year))
      AND (p_quarter IS NULL OR eww.quarter = ANY(p_quarter))
      AND (p_month IS NULL OR eww.month = ANY(p_month))
    GROUP BY 
        eww.company_id, 
        eww.year,
        eww.month,
		eww.natural_sources,
        eww.unit_of_measurement
    ORDER BY 
        eww.company_id, 
        eww.year, 
        eww.month, 
		eww.natural_sources,
        eww.unit_of_measurement;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_water_withdrawal_by_natural_sources;
DROP FUNCTION IF EXISTS gold.func_environment_water_withdrawal_by_natural_sources;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_water_withdrawal_by_natural_sources(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_natural_sources VARCHAR(30)[] DEFAULT NULL,
    p_month VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    natural_sources VARCHAR(30),
    total_volume NUMERIC(10,2),  -- updated data type for 2-decimal precision
    unit_of_measurement VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        eww.company_id,
        eww.natural_sources,
        CAST(SUM(eww.water_volume) AS NUMERIC(10,2)) AS total_volume,
        eww.unit_of_measurement
    FROM gold.vw_environment_water_withdrawal eww
    WHERE (p_company_id IS NULL OR eww.company_id = ANY(p_company_id))
      AND (p_natural_sources IS NULL OR eww.natural_sources = ANY(p_natural_sources))
      AND (p_year IS NULL OR eww.year = ANY(p_year))
      AND (p_quarter IS NULL OR eww.quarter = ANY(p_quarter))
      AND (p_month IS NULL OR eww.month = ANY(p_month))
    GROUP BY 
        eww.company_id, 
        eww.natural_sources,
        eww.unit_of_measurement
    ORDER BY 
        eww.company_id, 
        eww.natural_sources, 
        eww.unit_of_measurement;
END;
$$ LANGUAGE plpgsql;

-- DROP FUNCTION IF EXISTS gold.func_environment_water_withdrawal_by_perc_lvl;
DROP FUNCTION IF EXISTS gold.func_environment_water_withdrawal_by_perc_lvl;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_water_withdrawal_by_perc_lvl(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_natural_sources VARCHAR(30)[] DEFAULT NULL,
    p_month VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
    total_volume NUMERIC(10,2),  -- updated data type for 2-decimal precision
    unit_of_measurement VARCHAR(15)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        eww.company_id,
        CAST(SUM(eww.water_volume) AS NUMERIC(10,2)) AS total_volume,
        eww.unit_of_measurement
    FROM gold.vw_environment_water_withdrawal eww
    WHERE (p_company_id IS NULL OR eww.company_id = ANY(p_company_id))
      AND (p_natural_sources IS NULL OR eww.natural_sources = ANY(p_natural_sources))
      AND (p_year IS NULL OR eww.year = ANY(p_year))
      AND (p_quarter IS NULL OR eww.quarter = ANY(p_quarter))
      AND (p_month IS NULL OR eww.month = ANY(p_month))
    GROUP BY 
        eww.company_id, 
        eww.unit_of_measurement
    ORDER BY 
        eww.company_id, 
        eww.unit_of_measurement;
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
      AND (p_year IS NULL OR ec.year = ANY(p_year))
      AND (p_quarter IS NULL OR ec.quarter = ANY(p_quarter))
    GROUP BY ec.company_id, ec.year, ec.unit_of_measurement
    ORDER BY ec.company_id;
END;
$$ LANGUAGE plpgsql;


-- DROP FUNCTION IF EXISTS gold.func_environment_electric_consumption;
DROP FUNCTION IF EXISTS gold.func_environment_electric_consumption_by_quarter;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_electric_consumption_by_quarter(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
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
      AND (p_year IS NULL OR ec.year = ANY(p_year))
      AND (p_quarter IS NULL OR ec.quarter = ANY(p_quarter))
    GROUP BY ec.company_id, ec.year, ec.quarter, ec.unit_of_measurement
    ORDER BY ec.company_id;
END;
$$ LANGUAGE plpgsql;


-- DROP FUNCTION IF EXISTS gold.func_environment_electric_consumption;
DROP FUNCTION IF EXISTS gold.func_environment_electric_consumption_by_perc_lvl;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_electric_consumption_by_perc_lvl(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
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
      AND (p_year IS NULL OR ec.year = ANY(p_year))
      AND (p_quarter IS NULL OR ec.quarter = ANY(p_quarter))
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
	p_waste_source VARCHAR(20)[] DEFAULT NULL,
    p_metrics VARCHAR(20)[] DEFAULT NULL,
    p_month VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
	year SMALLINT,
	waste_source VARCHAR(20),
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
		nhw.waste_source,  
		nhw.metrics,
		nhw.unit_of_measurement,
		CAST(SUM(nhw.waste) AS NUMERIC(10,2)) AS total_waste
    FROM gold.vw_environment_non_hazard_waste nhw
    WHERE (p_company_id IS NULL OR nhw.company_id = ANY(p_company_id))
      AND (p_waste_source IS NULL OR nhw.waste_source = ANY(p_waste_source))
      AND (p_year IS NULL OR nhw.year = ANY(p_year))
      AND (p_quarter IS NULL OR nhw.quarter = ANY(p_quarter))
      AND (p_month IS NULL OR nhw.month = ANY(p_month))
	GROUP BY nhw.company_id, nhw.year, nhw.waste_source, nhw.metrics, nhw.unit_of_measurement
    ORDER BY nhw.company_id;
END;
$$ LANGUAGE plpgsql;


-- DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste;
DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste_by_quarter;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_non_hazard_waste_by_quarter(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
	p_waste_source VARCHAR(20)[] DEFAULT NULL,
    p_metrics VARCHAR(20)[] DEFAULT NULL,
    p_month VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
	year SMALLINT,
    quarter VARCHAR(2),
	waste_source VARCHAR(20),
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
		nhw.waste_source,  
		nhw.metrics,
		nhw.unit_of_measurement,
		CAST(SUM(nhw.waste) AS NUMERIC(10,2)) AS total_waste
    FROM gold.vw_environment_non_hazard_waste nhw
    WHERE (p_company_id IS NULL OR nhw.company_id = ANY(p_company_id))
      AND (p_waste_source IS NULL OR nhw.waste_source = ANY(p_waste_source))
      AND (p_year IS NULL OR nhw.year = ANY(p_year))
      AND (p_quarter IS NULL OR nhw.quarter = ANY(p_quarter))
      AND (p_month IS NULL OR nhw.month = ANY(p_month))
	GROUP BY nhw.company_id, nhw.year, nhw.quarter, nhw.waste_source, nhw.metrics, nhw.unit_of_measurement
    ORDER BY nhw.company_id;
END;
$$ LANGUAGE plpgsql;


-- DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste;
DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste_by_month;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_non_hazard_waste_by_month(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
	p_waste_source VARCHAR(20)[] DEFAULT NULL,
    p_metrics VARCHAR(20)[] DEFAULT NULL,
    p_month VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
	year SMALLINT,
    month VARCHAR(10),
	waste_source VARCHAR(20),
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
        nhw.month,
		nhw.waste_source,  
		nhw.metrics,
		nhw.unit_of_measurement,
		CAST(SUM(nhw.waste) AS NUMERIC(10,2)) AS total_waste
    FROM gold.vw_environment_non_hazard_waste nhw
    WHERE (p_company_id IS NULL OR nhw.company_id = ANY(p_company_id))
      AND (p_waste_source IS NULL OR nhw.waste_source = ANY(p_waste_source))
      AND (p_year IS NULL OR nhw.year = ANY(p_year))
      AND (p_quarter IS NULL OR nhw.quarter = ANY(p_quarter))
      AND (p_month IS NULL OR nhw.month = ANY(p_month))
	GROUP BY nhw.company_id, nhw.year, nhw.month, nhw.waste_source, nhw.metrics, nhw.unit_of_measurement
    ORDER BY nhw.company_id;
END;
$$ LANGUAGE plpgsql;


-- DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste;
DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste_by_waste_source;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_non_hazard_waste_by_waste_source(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
	p_waste_source VARCHAR(20)[] DEFAULT NULL,
    p_metrics VARCHAR(20)[] DEFAULT NULL,
    p_month VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(10),
	waste_source VARCHAR(20),
	unit_of_measurement VARCHAR(15),
	total_waste NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        nhw.company_id,
		nhw.waste_source,  
		nhw.unit_of_measurement,
		CAST(SUM(nhw.waste) AS NUMERIC(10,2)) AS total_waste
    FROM gold.vw_environment_non_hazard_waste nhw
    WHERE (p_company_id IS NULL OR nhw.company_id = ANY(p_company_id))
      AND (p_waste_source IS NULL OR nhw.waste_source = ANY(p_waste_source))
      AND (p_year IS NULL OR nhw.year = ANY(p_year))
      AND (p_quarter IS NULL OR nhw.quarter = ANY(p_quarter))
      AND (p_month IS NULL OR nhw.month = ANY(p_month))
	GROUP BY nhw.company_id, nhw.waste_source, nhw.unit_of_measurement
    ORDER BY nhw.company_id;
END;
$$ LANGUAGE plpgsql;


-- DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste;
DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste_by_metrics;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_non_hazard_waste_by_metrics(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
	p_waste_source VARCHAR(20)[] DEFAULT NULL,
    p_metrics VARCHAR(20)[] DEFAULT NULL,
    p_month VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
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
      AND (p_waste_source IS NULL OR nhw.waste_source = ANY(p_waste_source))
      AND (p_year IS NULL OR nhw.year = ANY(p_year))
      AND (p_quarter IS NULL OR nhw.quarter = ANY(p_quarter))
      AND (p_month IS NULL OR nhw.month = ANY(p_month))
	GROUP BY nhw.company_id, nhw.metrics, nhw.unit_of_measurement
    ORDER BY nhw.company_id;
END;
$$ LANGUAGE plpgsql;


-- DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste;
DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste_by_perc_lvl;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_non_hazard_waste_by_perc_lvl(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
	p_waste_source VARCHAR(20)[] DEFAULT NULL,
    p_metrics VARCHAR(20)[] DEFAULT NULL,
    p_month VARCHAR(10)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
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
      AND (p_waste_source IS NULL OR nhw.waste_source = ANY(p_waste_source))
      AND (p_year IS NULL OR nhw.year = ANY(p_year))
      AND (p_quarter IS NULL OR nhw.quarter = ANY(p_quarter))
      AND (p_month IS NULL OR nhw.month = ANY(p_month))
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
    p_waste_type   VARCHAR(15)[] DEFAULT NULL
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
        CAST(SUM(g.generate) AS NUMERIC(10,2)) AS total_generate
    FROM gold.vw_environment_hazard_waste_generated g
    WHERE (p_company_id IS NULL OR g.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR g.year = ANY(p_year))
      AND (p_quarter IS NULL OR g.quarter = ANY(p_quarter))
      AND (p_waste_type IS NULL OR g.waste_type = ANY(p_waste_type))
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
    p_waste_type   VARCHAR(15)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id     VARCHAR(10),
    year           SMALLINT,
	quarter		   VARCHAR(2),
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
        CAST(SUM(g.generate) AS NUMERIC(10,2)) AS total_generate
    FROM gold.vw_environment_hazard_waste_generated g
    WHERE (p_company_id IS NULL OR g.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR g.year = ANY(p_year))
      AND (p_quarter IS NULL OR g.quarter = ANY(p_quarter))
      AND (p_waste_type IS NULL OR g.waste_type = ANY(p_waste_type))
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
    p_waste_type   VARCHAR(15)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id     VARCHAR(10),
    year           SMALLINT,
	quarter		   VARCHAR(2),
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
        CAST(SUM(g.generate) AS NUMERIC(10,2)) AS total_generate
    FROM gold.vw_environment_hazard_waste_generated g
    WHERE (p_company_id IS NULL OR g.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR g.year = ANY(p_year))
      AND (p_quarter IS NULL OR g.quarter = ANY(p_quarter))
      AND (p_waste_type IS NULL OR g.waste_type = ANY(p_waste_type))
    GROUP BY g.company_id, g.quarter, g.year,g.waste_type, g.unit
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
    p_waste_type   VARCHAR(15)[] DEFAULT NULL
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
        CAST(SUM(g.generate) AS NUMERIC(10,2)) AS total_generate
    FROM gold.vw_environment_hazard_waste_generated g
    WHERE (p_company_id IS NULL OR g.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR g.year = ANY(p_year))
      AND (p_quarter IS NULL OR g.quarter = ANY(p_quarter))
      AND (p_waste_type IS NULL OR g.waste_type = ANY(p_waste_type))
    GROUP BY g.company_id, g.unit
    ORDER BY g.company_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Create Function: gold.func_environment_hazard_waste_disposed
-- =============================================================================
-- DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_disposed_by_year;
DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_disposed_by_year;

-- Now create our new functions
-- FUNCTION disposed
CREATE OR REPLACE FUNCTION gold.func_environment_hazard_waste_disposed_by_year(
    p_company_id   VARCHAR(10)[] DEFAULT NULL,
    p_year         SMALLINT[] DEFAULT NULL,
    p_waste_type   VARCHAR(15)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id      VARCHAR(10),
    waste_type      VARCHAR(15),
    unit            VARCHAR(15),
    total_disposed  NUMERIC(10,2),
    year            SMALLINT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.company_id,
        d.waste_type,
        d.unit,
        CAST(SUM(d.disposed) AS NUMERIC(10,2)) AS total_disposed,
        d.year
    FROM gold.vw_environment_hazard_waste_disposed d
    WHERE (p_company_id IS NULL OR d.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR d.year = ANY(p_year))
      AND (p_waste_type IS NULL OR d.waste_type = ANY(p_waste_type))
    GROUP BY d.company_id, d.waste_type, d.unit, d.year
    ORDER BY d.company_id, d.year;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_disposed_by_waste_type;

CREATE OR REPLACE FUNCTION gold.func_environment_hazard_waste_disposed_by_waste_type(
    p_company_id   VARCHAR(10)[] DEFAULT NULL,
    p_year         SMALLINT[] DEFAULT NULL,
    p_waste_type   VARCHAR(15)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id      VARCHAR(10),
    waste_type      VARCHAR(15),
    unit            VARCHAR(15),
    total_disposed  NUMERIC(10,2),
    year            SMALLINT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.company_id,
        d.waste_type,
        d.unit,
        CAST(SUM(d.disposed) AS NUMERIC(10,2)) AS total_disposed,
        d.year
    FROM gold.vw_environment_hazard_waste_disposed d
    WHERE (p_company_id IS NULL OR d.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR d.year = ANY(p_year))
      AND (p_waste_type IS NULL OR d.waste_type = ANY(p_waste_type))
    GROUP BY d.company_id, d.waste_type, d.unit, d.year
    ORDER BY d.company_id, d.year;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_disposed_by_perc_lvl;

CREATE OR REPLACE FUNCTION gold.func_environment_hazard_waste_disposed_by_perc_lvl(
    p_company_id   VARCHAR(10)[] DEFAULT NULL,
    p_year         SMALLINT[] DEFAULT NULL,
    p_waste_type   VARCHAR(15)[] DEFAULT NULL
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
        CAST(SUM(d.disposed) AS NUMERIC(10,2)) AS total_disposed
    FROM gold.vw_environment_hazard_waste_disposed d
    WHERE (p_company_id IS NULL OR d.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR d.year = ANY(p_year))
      AND (p_waste_type IS NULL OR d.waste_type = ANY(p_waste_type))
    GROUP BY d.company_id, d.waste_type, d.unit, d.year
    ORDER BY d.company_id, d.year;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Sample Query: Execute this queries to test the functions
-- =============================================================================

-- SAMPLE QUERIES FOR WATER WITHDRAWAL
SELECT * FROM gold.func_environment_water_withdrawal_by_year(
    NULL::VARCHAR(10)[], 
    NULL::VARCHAR(30)[], 
    NULL::VARCHAR(10)[], 
    NULL::VARCHAR(2)[], 
    ARRAY['2021', '2022']::SMALLINT[]
);
SELECT * FROM gold.func_environment_water_withdrawal_by_quarter(
    NULL::VARCHAR(10)[], 
    NULL::VARCHAR(30)[], 
    NULL::VARCHAR(10)[], 
    NULL::VARCHAR(2)[], 
    ARRAY['2021', '2022']::SMALLINT[]
);
SELECT * FROM gold.func_environment_water_withdrawal_by_month(
    ARRAY['PSC']::VARCHAR(10)[], 
    NULL::VARCHAR(30)[], 
    NULL::VARCHAR(10)[], 
    NULL::VARCHAR(2)[], 
    NULL::SMALLINT[]
);
SELECT * FROM gold.func_environment_water_withdrawal_by_natural_sources(
    NULL::VARCHAR(10)[], 
    NULL::VARCHAR(30)[], 
    NULL::VARCHAR(10)[], 
    NULL::VARCHAR(2)[], 
    ARRAY['2025']::SMALLINT[]
);
SELECT * FROM gold.func_environment_water_withdrawal_by_perc_lvl(
    NULL::VARCHAR(10)[], 
    NULL::VARCHAR(30)[], 
    NULL::VARCHAR(10)[], 
    NULL::VARCHAR(2)[], 
    NULL::SMALLINT[]
);

-- SAMPLE QUERIES FOR DIESEL CONSUMPTION
SELECT * FROM gold.func_environment_diesel_consumption_by_year(
    NULL::VARCHAR(10)[], 
    NULL::VARCHAR(30)[], 
    NULL::VARCHAR(15)[], 
    NULL::VARCHAR(10)[], 
    ARRAY[2024]::SMALLINT[], 
    NULL::VARCHAR(2)[]
);
SELECT * FROM gold.func_environment_diesel_consumption_by_quarter(
    NULL::VARCHAR(10)[], 
    NULL::VARCHAR(30)[], 
    NULL::VARCHAR(15)[], 
    NULL::VARCHAR(10)[], 
    ARRAY[2024]::SMALLINT[], 
    NULL::VARCHAR(2)[]
);
SELECT * FROM gold.func_environment_diesel_consumption_by_month(
    NULL::VARCHAR(10)[], 
    NULL::VARCHAR(30)[], 
    NULL::VARCHAR(15)[], 
    NULL::VARCHAR(10)[], 
    ARRAY[2024]::SMALLINT[], 
    NULL::VARCHAR(2)[]
);
SELECT * FROM gold.func_environment_diesel_consumption_by_cp_name(
    NULL::VARCHAR(10)[], 
    NULL::VARCHAR(30)[], 
    NULL::VARCHAR(15)[], 
    NULL::VARCHAR(10)[], 
    ARRAY[2024]::SMALLINT[], 
    NULL::VARCHAR(2)[]
);
SELECT * FROM gold.func_environment_diesel_consumption_by_cp_type(
    NULL::VARCHAR(10)[], 
    NULL::VARCHAR(30)[], 
    NULL::VARCHAR(15)[], 
    NULL::VARCHAR(10)[], 
    ARRAY[2024]::SMALLINT[], 
    NULL::VARCHAR(2)[]
);
SELECT * FROM gold.func_environment_diesel_consumption_by_perc_lvl(
    NULL::VARCHAR(10)[], 
    NULL::VARCHAR(30)[], 
    NULL::VARCHAR(15)[], 
    NULL::VARCHAR(10)[], 
    ARRAY[2024]::SMALLINT[], 
    NULL::VARCHAR(2)[]
);

-- SAMPLE QUERIES FOR ELECTRIC CONSUMPTION
SELECT * FROM gold.func_environment_electric_consumption_by_year(
    NULL, 
    NULL, 
    ARRAY[2024]::SMALLINT[]
);

SELECT * FROM gold.func_environment_electric_consumption_by_quarter(
    NULL, 
    ARRAY['Q1'], 
    NULL::SMALLINT[]
);

SELECT * FROM gold.func_environment_electric_consumption_by_perc_lvl(
    NULL, 
    NULL, 
    NULL::SMALLINT[]
);

-- SAMPLE QUERIES FOR NON HAZARD WASTE GENERATED
SELECT * FROM gold.func_environment_non_hazard_waste_by_year(
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    NULL,
	ARRAY[2024]::SMALLINT[]
);

SELECT * FROM gold.func_environment_non_hazard_waste_by_quarter(
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    ARRAY['Q1','Q2'],
	NULL::SMALLINT[]
);

SELECT * FROM gold.func_environment_non_hazard_waste_by_month(
    NULL, 
    NULL, 
    NULL, 
    ARRAY['January','March'], 
    NULL,
	NULL::SMALLINT[]
);

SELECT * FROM gold.func_environment_non_hazard_waste_by_waste_source(
    NULL, 
    ARRAY['Staff House', 'Security', 'Utility'], 
    NULL, 
    NULL, 
    NULL,
	NULL::SMALLINT[]
);

SELECT * FROM gold.func_environment_non_hazard_waste_by_metrics(
    NULL, 
    NULL, 
    ARRAY['Residual'], 
    NULL, 
    NULL,
	NULL
);

SELECT * FROM gold.func_environment_non_hazard_waste_by_perc_lvl(
    NULL, 
    NULL, 
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