/*
===============================================================================
Function Script: Create Gold Functions for Environment Data
===============================================================================
*/
-- =============================================================================
-- NOTE: Execute the create functions first, then execute the sample queries for checking
-- =============================================================================

-- =============================================================================
-- Create Function: gold.func_environment_water_withdrawal
-- =============================================================================
-- DROP FUNCTION IF EXISTS gold.func_environment_water_withdrawal;
DROP FUNCTION IF EXISTS gold.func_environment_water_withdrawal;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_water_withdrawal(
    p_company_id VARCHAR(20)[] DEFAULT NULL,
    p_natural_sources VARCHAR(100)[] DEFAULT NULL,
    p_month VARCHAR(20)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(20),
    year SMALLINT,
    natural_sources VARCHAR(100),
    total_volume DOUBLE PRECISION,
    unit_of_measurement VARCHAR(20)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        eww.company_id,
        eww.year,
        eww.natural_sources,
        SUM(eww.water_volume) AS total_volume,
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


-- =============================================================================
-- Create Function: gold.func_environment_diesel_consumption
-- =============================================================================
-- DROP FUNCTION IF EXISTS gold.func_environment_diesel_consumption;
DROP FUNCTION IF EXISTS gold.func_environment_diesel_consumption;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_diesel_consumption(
    p_company_id VARCHAR(20)[] DEFAULT NULL,
	p_company_property_name VARCHAR(100)[] DEFAULT NULL,
	p_company_property_type VARCHAR(50)[] DEFAULT NULL,
    p_month VARCHAR(20)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(20),
	year SMALLINT,
    company_property_name VARCHAR(100),
	company_property_type VARCHAR(50),
    total_consumption DOUBLE PRECISION,
	unit_of_measurement VARCHAR(20)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        edc.company_id,
		edc.year::SMALLINT,
        edc.company_property_name,
		edc.company_property_type,
        SUM(edc.consumption) AS total_consumption,
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

-- =============================================================================
-- Create Function: gold.func_environment_electric_consumption
-- =============================================================================
-- DROP FUNCTION IF EXISTS gold.func_environment_electric_consumption;
DROP FUNCTION IF EXISTS gold.func_environment_electric_consumption;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_electric_consumption(
    p_company_id VARCHAR(20)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year INT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(20),
	year INT,
    unit_of_measurement VARCHAR(20),
    total_consumption DOUBLE PRECISION
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ec.company_id,
		ec.year,
        ec.unit_of_measurement,
        SUM(ec.consumption) AS total_consumption
    FROM gold.vw_environment_electric_consumption ec
    WHERE (p_company_id IS NULL OR ec.company_id = ANY(p_company_id))
      AND (p_year IS NULL OR ec.year = ANY(p_year))
      AND (p_quarter IS NULL OR ec.quarter = ANY(p_quarter))
    GROUP BY ec.company_id, ec.year, ec.unit_of_measurement
    ORDER BY ec.company_id;
END;
$$ LANGUAGE plpgsql;


-- =============================================================================
-- Create Function: gold.func_environment_non_hazard_waste
-- =============================================================================
-- DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste;
DROP FUNCTION IF EXISTS gold.func_environment_non_hazard_waste;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_non_hazard_waste(
    p_company_id VARCHAR(20)[] DEFAULT NULL,
	p_waste_source VARCHAR(50)[] DEFAULT NULL,
    p_month VARCHAR(20)[] DEFAULT NULL,
    p_quarter VARCHAR(2)[] DEFAULT NULL,
    p_year INT[] DEFAULT NULL
)
RETURNS TABLE (
    company_id VARCHAR(20),
	year INT,
	waste_source VARCHAR(50),
	metrics VARCHAR(20),
	unit_of_measurement VARCHAR(20),
	total_waste DOUBLE PRECISION
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
		SUM(nhw.waste) AS total_waste
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


-- =============================================================================
-- Create Function: gold.func_environment_hazard_waste_generated
-- =============================================================================
-- DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_generated;
DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_generated;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_environment_hazard_waste_generated(
    p_company_id   VARCHAR(20)[] DEFAULT NULL,
    p_year         INT[] DEFAULT NULL,
    p_quarter      VARCHAR(20)[] DEFAULT NULL,
    p_waste_type   VARCHAR(20)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id     VARCHAR(20),
    waste_type     VARCHAR(20),
    unit           VARCHAR(20),
    total_generate DOUBLE PRECISION,
    year           INT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        g.company_name,
        g.waste_type,
        g.unit,
        SUM(g.generate) AS total_generate,
        g.year
    FROM gold.vw_environment_hazard_waste_generated g
    WHERE (p_company_id IS NULL OR g.company_name = ANY(p_company_id))
      AND (p_year IS NULL OR g.year = ANY(p_year))
      AND (p_quarter IS NULL OR g.quarter = ANY(p_quarter))
      AND (p_waste_type IS NULL OR g.waste_type = ANY(p_waste_type))
    GROUP BY g.company_name, g.waste_type, g.unit, g.year
    ORDER BY g.company_name, g.year;
END;
$$ LANGUAGE plpgsql;


-- =============================================================================
-- Create Function: gold.func_environment_hazard_waste_disposed
-- =============================================================================
-- DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_disposed;
DROP FUNCTION IF EXISTS gold.func_environment_hazard_waste_disposed;

-- Now create our new functions
-- FUNCTION disposed
CREATE OR REPLACE FUNCTION gold.func_environment_hazard_waste_disposed(
    p_company_id   VARCHAR(20)[] DEFAULT NULL,
    p_year         INT[] DEFAULT NULL,
    p_quarter      VARCHAR(20)[] DEFAULT NULL,
    p_waste_type   VARCHAR(20)[] DEFAULT NULL
)
RETURNS TABLE (
    company_id      VARCHAR(20),
    waste_type      VARCHAR(20),
    unit            VARCHAR(20),
    total_disposed  DOUBLE PRECISION,
    year            INT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.company_name,
        d.waste_type,
        d.unit,
        SUM(d.disposed) AS total_disposed,
        d.year
    FROM gold.vw_environment_hazard_waste_disposed d
    WHERE (p_company_id IS NULL OR d.company_name = ANY(p_company_id))
      AND (p_year IS NULL OR d.year = ANY(p_year))
      AND (p_waste_type IS NULL OR d.waste_type = ANY(p_waste_type))
    GROUP BY d.company_name, d.waste_type, d.unit, d.year
    ORDER BY d.company_name, d.year;
END;
$$ LANGUAGE plpgsql;


-- =============================================================================
-- Sample Query: Execute this queries to test the functions
-- =============================================================================

-- SAMPLE QUERIES FOR WATER WITHDRAWAL
SELECT * FROM gold.func_environment_water_withdrawal(
    NULL::VARCHAR(20)[], 
    NULL::VARCHAR(100)[], 
    NULL::VARCHAR(20)[], 
    NULL::VARCHAR(2)[], 
    ARRAY['2021', '2022']::SMALLINT[]
);

-- SAMPLE QUERIES FOR DIESEL CONSUMPTION
SELECT * FROM gold.func_environment_diesel_consumption(
    NULL::VARCHAR(20)[], 
    NULL::VARCHAR(100)[], 
    NULL::VARCHAR(50)[], 
    NULL::VARCHAR(20)[], 
    ARRAY[2024]::SMALLINT[], 
    NULL::VARCHAR(2)[]
);

-- SAMPLE QUERIES FOR ELECTRIC CONSUMPTION
SELECT * FROM gold.func_environment_electric_consumption(
    NULL, 
    ARRAY['Q1'], 
    ARRAY[2024]
);

-- SAMPLE QUERIES FOR NON HAZARD WASTE GENERATED
SELECT * FROM gold.func_environment_non_hazard_waste(
    NULL, 
    ARRAY['Staff House'], 
    NULL, 
    NULL, 
    NULL
);

-- SAMPLE QUERIES FOR WASTE GENERATED
SELECT * FROM gold.func_environment_hazard_waste_generated(
    NULL,
    NULL,
    NULL,
    NULL
);

SELECT * FROM gold.func_environment_hazard_waste_generated(
    NULL,
    ARRAY[2023],
    ARRAY['Q3'],
    NULL
);

-- SAMPLE QUERIES FOR WASTE GENERATED
SELECT * FROM gold.func_environment_hazard_waste_disposed(
    NULL,
    NULL,
    NULL,
    NULL
);

SELECT * FROM gold.func_environment_hazard_waste_disposed(
    NULL,
    NULL,
    NULL,
    ARRAY['Battery']
);
