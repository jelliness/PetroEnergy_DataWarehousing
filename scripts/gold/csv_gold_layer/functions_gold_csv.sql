/*
======================================================================================
Function Script: Create Gold Functions for Power Plant Data
======================================================================================

======================================================================================
NOTE: Execute the create functions first, then execute the sample queries for checking
======================================================================================
*/

-- ===================================================================================
-- Create Functions for gold.func_fact_energy (daily)
-- ===================================================================================
DROP FUNCTION IF EXISTS gold.func_fact_energy;

CREATE OR REPLACE FUNCTION gold.func_fact_energy(
    p_power_plant_id VARCHAR(10)[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_generation_source TEXT[] DEFAULT NULL,
    p_province VARCHAR(30)[] DEFAULT NULL,
    p_month INT[] DEFAULT NULL,
    p_quarter INT[] DEFAULT NULL,
    p_year INT[] DEFAULT NULL
)
RETURNS TABLE (
    power_plant_id VARCHAR(10),
    company_id VARCHAR(10),
    generation_source TEXT,
    site_name VARCHAR(50),
    company_name VARCHAR(255),
    province VARCHAR(30),
    month INT,
    month_name TEXT,
    year INT,
    quarter INT,
    total_energy_generated NUMERIC(10,2),
    total_co2_avoidance NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        feg.power_plant_id,
        feg.company_id,
        feg.generation_source,
        feg.site_name,
        feg.company_name,
        feg.province,
        feg.month,
        feg.month_name,
        feg.year,
        feg.quarter,
        CAST(SUM(feg.energy_generated_kwh) AS NUMERIC(1000,2)) AS energy_generated_kwh,
        CAST(SUM(feg.co2_avoidance_tons) AS NUMERIC(1000,2)) AS co2_avoidance_tons
    FROM gold.fact_energy_generated feg
    WHERE (p_power_plant_id IS NULL OR feg.power_plant_id = ANY(p_power_plant_id))
        AND (p_company_id IS NULL OR feg.company_id = ANY(p_company_id))
        AND (p_generation_source IS NULL OR feg.generation_source = ANY(p_generation_source))
        AND (p_province IS NULL OR feg.province = ANY(p_province))
        AND (p_month IS NULL OR feg.month = ANY(p_month))   
        AND (p_quarter IS NULL OR feg.quarter = ANY(p_quarter))   
        AND (p_year IS NULL OR feg.year = ANY(p_year))   
    GROUP BY 
        feg.power_plant_id,
        feg.company_id, 
        feg.site_name,
        feg.company_name,
        feg.generation_source,
        feg.province,
        feg.month,
        feg.month_name,
        feg.year,
        feg.quarter
    ORDER BY 
        feg.year DESC,
        feg.month DESC;

END;
$$ LANGUAGE plpgsql;
-- ===================================================================================
-- Create Functions for gold.func_fact_energy_monthly (monthly)
-- ===================================================================================
DROP FUNCTION IF EXISTS gold.func_fact_energy_monthly;

CREATE OR REPLACE FUNCTION gold.func_fact_energy_monthly(
    p_power_plant_id      VARCHAR(10)[] DEFAULT NULL,
    p_company_id          VARCHAR(10)[] DEFAULT NULL,
    p_generation_source   TEXT[]        DEFAULT NULL,
    p_province            VARCHAR(30)[] DEFAULT NULL,
    p_month               INT[]         DEFAULT NULL,
    p_quarter             INT[]         DEFAULT NULL,
    p_year                INT[]         DEFAULT NULL
)
RETURNS TABLE (
    month_name           TEXT,
    year                 INT,
    energy_generated_kwh NUMERIC(10,2),
    co2_avoidance_tons   NUMERIC(10,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        feg.month_name,
        feg.year,
        CAST(SUM(feg.energy_generated_kwh) AS NUMERIC(10,2)) AS energy_generated_kwh,
        CAST(SUM(feg.co2_avoidance_tons) AS NUMERIC(10,2)) AS co2_avoidance_tons
    FROM gold.fact_energy_generated feg
    WHERE (p_power_plant_id IS NULL OR feg.power_plant_id = ANY(p_power_plant_id))
      AND (p_company_id IS NULL OR feg.company_id = ANY(p_company_id))
      AND (p_generation_source IS NULL OR feg.generation_source = ANY(p_generation_source))
      AND (p_province IS NULL OR feg.province = ANY(p_province))
      AND (p_month IS NULL OR feg.month = ANY(p_month))   
      AND (p_quarter IS NULL OR feg.quarter = ANY(p_quarter))   
      AND (p_year IS NULL OR feg.year = ANY(p_year))   
    GROUP BY 
        feg.year,
        feg.month,
        feg.month_name
    ORDER BY 
        feg.year DESC,
        feg.month DESC;
END
$$ LANGUAGE plpgsql;



-- ===================================================================================
-- Create Functions for gold.func_fact_energy_quarterly (quarterly)
-- ===================================================================================
DROP FUNCTION IF EXISTS gold.func_fact_energy_quarterly;

CREATE OR REPLACE FUNCTION gold.func_fact_energy_quarterly(
    p_power_plant_id VARCHAR(10)[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_generation_source TEXT[] DEFAULT NULL,
    p_province VARCHAR(30)[] DEFAULT NULL,
    p_month INT[] DEFAULT NULL,
    p_quarter INT[] DEFAULT NULL,
    p_year INT[] DEFAULT NULL
)
RETURNS TABLE (
	quarter TEXT,
	year smallint,	
    energy_generated_kwh NUMERIC(15,2),
    co2_avoidance_tons NUMERIC(15,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
		'Q' || feg.quarter::TEXT AS quarter,
		feg.year::SMALLINT,
        CAST(SUM(feg.energy_generated_kwh) AS NUMERIC(15,2)) AS energy_generated_kwh,
        CAST(SUM(feg.co2_avoidance_tons) AS NUMERIC(15,2)) AS co2_avoidance_tons
    FROM gold.fact_energy_generated feg
    WHERE (p_power_plant_id IS NULL OR feg.power_plant_id = ANY(p_power_plant_id))
    	AND (p_company_id IS NULL OR feg.company_id = ANY(p_company_id))
	    AND (p_generation_source IS NULL OR feg.generation_source = ANY(p_generation_source))
	    AND (p_province IS NULL OR feg.province = ANY(p_province))
        AND (p_month IS NULL OR feg.month = ANY(p_month))   
        AND (p_quarter IS NULL OR feg.quarter = ANY(p_quarter))   
        AND (p_year IS NULL OR feg.year = ANY(p_year))   
    GROUP BY 
        feg.quarter,
		feg.year
	ORDER BY 
		feg.year DESC,
        feg.quarter DESC;
END;
$$ LANGUAGE plpgsql;



-- ===================================================================================
-- Create Functions for gold.func_fact_energy_yearly (yearly)
-- ===================================================================================
DROP FUNCTION IF EXISTS gold.func_fact_energy_yearly;

CREATE OR REPLACE FUNCTION gold.func_fact_energy_yearly(
    p_power_plant_id VARCHAR(10)[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_generation_source TEXT[] DEFAULT NULL,
    p_province VARCHAR(30)[] DEFAULT NULL,
    p_month INT[] DEFAULT NULL,
    p_quarter INT[] DEFAULT NULL,
    p_year INT[] DEFAULT NULL
)
RETURNS TABLE (
	year INT,
    energy_generated_kwh NUMERIC(15,2),
    co2_avoidance_tons NUMERIC(15,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
		feg.year,
        CAST(SUM(feg.energy_generated_kwh) AS NUMERIC(15,2)) AS energy_generated_kwh,
        CAST(SUM(feg.co2_avoidance_tons) AS NUMERIC(15,2)) AS co2_avoidance_tons
    FROM gold.fact_energy_generated feg
    WHERE (p_power_plant_id IS NULL OR feg.power_plant_id = ANY(p_power_plant_id))
    	AND (p_company_id IS NULL OR feg.company_id = ANY(p_company_id))
	    AND (p_generation_source IS NULL OR feg.generation_source = ANY(p_generation_source))
	    AND (p_province IS NULL OR feg.province = ANY(p_province))
        AND (p_month IS NULL OR feg.month = ANY(p_month))   
        AND (p_quarter IS NULL OR feg.quarter = ANY(p_quarter))   
        AND (p_year IS NULL OR feg.year = ANY(p_year))   
    GROUP BY 
        feg.year
	ORDER BY 
        feg.year DESC;
END;
$$ LANGUAGE plpgsql;




-- =============================================================================
-- Create Function for Number of Houses Powered (Total Annual Energy Generated)
-- =============================================================================
CREATE OR REPLACE FUNCTION gold.func_household_powered(
    p_power_plant_id VARCHAR(10)[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_generation_source TEXT[] DEFAULT NULL,
    p_province VARCHAR(30)[] DEFAULT NULL,
    p_month INT[] DEFAULT NULL,
    p_quarter INT[] DEFAULT NULL,
    p_year INT[] DEFAULT NULL
)
RETURNS TABLE (
    year SMALLINT,
    power_plant_id VARCHAR(10),
    company_id VARCHAR(10), 
    generation_source TEXT,
    site_name VARCHAR(50),
    company_name VARCHAR(255),
    province VARCHAR(30),
    month INT,
    month_name TEXT,
    quarter INT,
    energy_generated NUMERIC,
    est_house_powered BIGINT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        fg.year::SMALLINT,
        fg.power_plant_id,
        fg.company_id,
        fg.generation_source,
        fg.site_name,
        fg.company_name,
        fg.province,
        fg.month,
        fg.month_name,
        fg.quarter,
        SUM(fg.energy_generated) AS energy_generated,
        SUM(fg.household_powered)::BIGINT AS est_house_powered
    FROM gold.fact_household_powered fg
    WHERE (p_power_plant_id IS NULL OR fg.power_plant_id = ANY(p_power_plant_id))
        AND (p_company_id IS NULL OR fg.company_id = ANY(p_company_id))
        AND (p_generation_source IS NULL OR fg.generation_source = ANY(p_generation_source))
        AND (p_province IS NULL OR fg.province = ANY(p_province))
        AND (p_month IS NULL OR fg.month = ANY(p_month))   
        AND (p_quarter IS NULL OR fg.quarter = ANY(p_quarter))   
        AND (p_year IS NULL OR fg.year = ANY(p_year))
    GROUP BY 
        fg.year,
        fg.power_plant_id,
        fg.company_id,
        fg.generation_source,
        fg.site_name,
        fg.company_name,
        fg.province,
        fg.month,
        fg.month_name,
        fg.quarter;
END;
$$ LANGUAGE plpgsql;




CREATE OR REPLACE FUNCTION gold.func_fund_alloc(
    p_power_plant_id VARCHAR(10)[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_ff_id VARCHAR(10)[] DEFAULT NULL,
    p_month INT[] DEFAULT NULL,
    p_year INT[] DEFAULT NULL,
    p_ff_category VARCHAR(20) DEFAULT NULL
)
RETURNS TABLE (
    month_name TEXT,
	year SMALLINT,
    power_plant_id  VARCHAR(10),
    company_id VARCHAR(10),
    ff_id VARCHAR(10),
    ff_name TEXT,
    ff_percentage NUMERIC(5,4),
	ff_category VARCHAR(20),
    power_generated_peso NUMERIC,
    funds_allocated_peso NUMERIC
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        dd.month_name,
		dd.year::SMALLINT,
        pp.power_plant_id,
        pp.company_id,
        ff.ff_id,
        ff.ff_name,
        ff.ff_percentage,
		ff.ff_category,
        ROUND(SUM(er.energy_generated_kwh * 0.01), 2) AS power_generated_peso,
        ROUND(SUM((er.energy_generated_kwh * 0.01) * ff.ff_percentage), 2) AS funds_allocated_peso
    FROM silver.csv_energy_records er
    LEFT JOIN ref.ref_power_plants pp ON pp.power_plant_id = er.power_plant_id
    LEFT JOIN gold.dim_date dd ON dd.date_id = DATE_TRUNC('month', er.date_generated)
    CROSS JOIN ref.ref_fa_factors ff
    WHERE (p_power_plant_id IS NULL OR pp.power_plant_id = ANY(p_power_plant_id))
        AND (p_company_id IS NULL OR pp.company_id = ANY(p_company_id))
        AND (p_ff_id IS NULL OR ff.ff_id = ANY(p_ff_id))
        AND (p_month IS NULL OR dd.month = ANY(p_month))
        AND (p_year IS NULL OR dd.year = ANY(p_year))
        AND (p_ff_category IS NULL OR ff.ff_category = p_ff_category)
    GROUP BY 
        dd.month_name,
        dd.month,
		dd.year,
        DATE_TRUNC('month', er.date_generated),
        pp.power_plant_id,
        pp.company_id,
        ff.ff_id,
        ff.ff_name,
        ff.ff_percentage
    ORDER BY dd.month DESC;
END;
$$ LANGUAGE plpgsql;








-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gold.func_fund_alloc_year(
    p_power_plant_id VARCHAR(10)[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_ff_id VARCHAR(10)[] DEFAULT NULL,
    p_year INT[] DEFAULT NULL
)
RETURNS TABLE (
    year INT,
    power_plant_id  VARCHAR(10),
    company_id VARCHAR(10),
    ff_id VARCHAR(10),
    ff_name TEXT,
    ff_percentage NUMERIC(5,4),
    power_generated_peso NUMERIC,
    funds_allocated_peso NUMERIC
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        dd.year,
        pp.power_plant_id,
        pp.company_id,
        ff.ff_id,
        ff.ff_name,
        ff.ff_percentage,
        ROUND(SUM(er.energy_generated_kwh * 0.01), 2) AS power_generated_peso,
        ROUND(SUM((er.energy_generated_kwh * 0.01) * ff.ff_percentage), 2) AS funds_allocated_peso
    FROM silver.csv_energy_records er
    LEFT JOIN ref.ref_power_plants pp ON pp.power_plant_id = er.power_plant_id
    LEFT JOIN gold.dim_date dd ON dd.date_id = DATE_TRUNC('month', er.date_generated)
    CROSS JOIN ref.ref_fa_factors ff
    WHERE (p_power_plant_id IS NULL OR pp.power_plant_id = ANY(p_power_plant_id))
        AND (p_company_id IS NULL OR pp.company_id = ANY(p_company_id))
        AND (p_ff_id IS NULL OR ff.ff_id = ANY(p_ff_id))
        AND (p_year IS NULL OR dd.year = ANY(p_year))
    GROUP BY 
        dd.year,
        pp.power_plant_id,
        pp.company_id,
        ff.ff_id,
        ff.ff_name,
        ff.ff_percentage
    ORDER BY dd.year DESC;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Create Function for Outages Frequency 
-- =============================================================================

CREATE OR REPLACE FUNCTION gold.func_outages_frequency(
    p_power_plant_id VARCHAR(10) DEFAULT NULL,
    p_company_id VARCHAR(10) DEFAULT NULL,
    p_generation_source TEXT DEFAULT NULL,
    p_site_name VARCHAR(50) DEFAULT NULL,
    p_company_name VARCHAR(255) DEFAULT NULL,
    p_city_town VARCHAR(30) DEFAULT NULL,
    p_province VARCHAR(30) DEFAULT NULL,
    p_year INTEGER DEFAULT NULL,
    p_quarter INTEGER DEFAULT NULL,
    p_month INTEGER DEFAULT NULL,
    p_month_name TEXT DEFAULT NULL
)
RETURNS TABLE (
    power_plant_id VARCHAR(10),
    company_id VARCHAR(10),
    generation_source TEXT,
    site_name VARCHAR(50),
    company_name VARCHAR(255),
    city_town VARCHAR(30),
    province VARCHAR(30),
    year INTEGER,
    quarter INT,
    month INTEGER,
    month_name TEXT,
    outage_count BIGINT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        f.power_plant_id,
        f.company_id,
        f.generation_source,
        f.site_name,
        f.company_name,
        f.city_town,
        f.province,
        f.year,
        f.quarter,
        f.month,
        f.month_name,
        COUNT(*) AS outage_count
    FROM gold.fact_energy_generated f
    WHERE f.energy_generated_kwh = 0
      AND (p_power_plant_id IS NULL OR f.power_plant_id = p_power_plant_id)
      AND (p_company_id IS NULL OR f.company_id = p_company_id)
      AND (p_generation_source IS NULL OR f.generation_source = p_generation_source)
      AND (p_site_name IS NULL OR f.site_name = p_site_name)
      AND (p_company_name IS NULL OR f.company_name = p_company_name)
      AND (p_city_town IS NULL OR f.city_town = p_city_town)
      AND (p_province IS NULL OR f.province = p_province)
      AND (p_year IS NULL OR f.year = p_year)
      AND (p_quarter IS NULL OR f.quarter = p_quarter)
      AND (p_month IS NULL OR f.month = p_month)
      AND (p_month_name IS NULL OR f.month_name = p_month_name)
    GROUP BY 
        f.power_plant_id,
        f.company_id,
        f.generation_source,
        f.site_name,
        f.company_name,
        f.city_town,
        f.province,
        f.year,
        f.quarter,
        f.month,
        f.month_name;
END;
$$ LANGUAGE plpgsql;


-- ===================================================================================
-- Create Function: gold.func_co2_equivalence
-- ===================================================================================

DROP FUNCTION IF EXISTS gold.func_co2_equivalence;

CREATE OR REPLACE FUNCTION gold.func_co2_equivalence(
    p_power_plant_id VARCHAR(10)[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_generation_source TEXT[] DEFAULT NULL,
    p_province VARCHAR(30)[] DEFAULT NULL,
    p_month INT[] DEFAULT NULL,
    p_quarter INT[] DEFAULT NULL,
    p_year INT[] DEFAULT NULL
)
RETURNS TABLE (
    energy_generated NUMERIC,
    co2_avoided NUMERIC,
    conversion_value NUMERIC,
    co2_equivalent NUMERIC,
    metric VARCHAR(100),
    equivalence_category TEXT,
    equivalence_label TEXT,
    power_plant_id VARCHAR(10),
    company_id VARCHAR(10), 
    generation_source TEXT,
    site_name VARCHAR(50),
    company_name VARCHAR(255),
    province VARCHAR(30),
    year INT,
    quarter INT,
    month INT,
    month_name TEXT
)
AS $$
BEGIN
    RETURN QUERY
    WITH co2_equivalent AS (
        SELECT 
            rc.equivalence_category,
            rc.equivalence_label,
            rc.metric,
            rc.equivalent_value_co2_emissions
        FROM ref.ref_co2_equivalence rc
    )
    SELECT
        SUM(fe.energy_generated_kwh) AS energy_generated,
        SUM(fe.co2_avoidance_tons) AS co2_avoided,
        ce.equivalent_value_co2_emissions AS conversion_value,
        ROUND(SUM(fe.co2_avoidance_tons) / ce.equivalent_value_co2_emissions, 4) AS co2_equivalent,
        ce.metric,
        ce.equivalence_category,
        ce.equivalence_label,
        fe.power_plant_id,
        fe.company_id,
        fe.generation_source,
        fe.site_name,
        fe.company_name,
        fe.province,
        fe.year,
        fe.quarter,
        fe.month,
        fe.month_name
    FROM gold.fact_energy_generated fe
    CROSS JOIN co2_equivalent ce
    WHERE (p_power_plant_id IS NULL OR fe.power_plant_id = ANY(p_power_plant_id))
      AND (p_company_id IS NULL OR fe.company_id = ANY(p_company_id))
      AND (p_generation_source IS NULL OR fe.generation_source = ANY(p_generation_source))
      AND (p_province IS NULL OR fe.province = ANY(p_province))
      AND (p_month IS NULL OR fe.month = ANY(p_month))
      AND (p_quarter IS NULL OR fe.quarter = ANY(p_quarter))
      AND (p_year IS NULL OR fe.year = ANY(p_year))
    GROUP BY 
        ce.equivalence_category,
        ce.equivalence_label,
        ce.metric,
        ce.equivalent_value_co2_emissions,
        fe.power_plant_id,
        fe.company_id,
        fe.generation_source,
        fe.site_name,
        fe.company_name,
        fe.province,
        fe.year,
        fe.quarter,
        fe.month,
        fe.month_name;
END;
$$ LANGUAGE plpgsql;

-- ===================================================================================
-- Create Function: gold.func_co2_equivalence_per_metric
-- ===================================================================================

DROP FUNCTION IF EXISTS gold.func_co2_equivalence_per_metric;

CREATE OR REPLACE FUNCTION gold.func_co2_equivalence_per_metric(
    p_power_plant_id VARCHAR(10)[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_generation_source TEXT[] DEFAULT NULL,
    p_province VARCHAR(30)[] DEFAULT NULL,
    p_month INT[] DEFAULT NULL,
    p_quarter INT[] DEFAULT NULL,
    p_year INT[] DEFAULT NULL
)
RETURNS TABLE (
    energy_generated NUMERIC,
    co2_avoided NUMERIC,
    conversion_value NUMERIC,
    co2_equivalent NUMERIC,
    metric VARCHAR(100),
    equivalence_category TEXT,
    equivalence_label TEXT
)
AS $$
BEGIN
    RETURN QUERY
    WITH co2_equivalent AS (
        SELECT 
            rc.equivalence_category,
            rc.equivalence_label,
            rc.metric,
            rc.equivalent_value_co2_emissions
        FROM ref.ref_co2_equivalence rc
    )
    SELECT
        SUM(fe.energy_generated_kwh) AS energy_generated,
        SUM(fe.co2_avoidance_tons) AS co2_avoided,
        ce.equivalent_value_co2_emissions AS conversion_value,
        ROUND(SUM(fe.co2_avoidance_tons) / ce.equivalent_value_co2_emissions, 4) AS co2_equivalent,
        ce.metric,
        ce.equivalence_category,
        ce.equivalence_label
    FROM gold.fact_energy_generated fe
    CROSS JOIN co2_equivalent ce
    WHERE (p_power_plant_id IS NULL OR fe.power_plant_id = ANY(p_power_plant_id))
      AND (p_company_id IS NULL OR fe.company_id = ANY(p_company_id))
      AND (p_generation_source IS NULL OR fe.generation_source = ANY(p_generation_source))
      AND (p_province IS NULL OR fe.province = ANY(p_province))
      AND (p_month IS NULL OR fe.month = ANY(p_month))
      AND (p_quarter IS NULL OR fe.quarter = ANY(p_quarter))
      AND (p_year IS NULL OR fe.year = ANY(p_year))
    GROUP BY 
        ce.equivalence_category,
        ce.equivalence_label,
        ce.metric,
        ce.equivalent_value_co2_emissions;
END;
$$ LANGUAGE plpgsql;



-- sample query

SELECT *
FROM gold.func_fund_alloc(
    NULL,  		-- power_plant_id
    ARRAY['PSC'],                         -- company_id
    NULL,                         -- ff_id
    NULL,                         -- month
	ARRAY[2024]							-- year
);

SELECT *
FROM gold.func_fund_alloc_year(
    NULL,     -- power_plant_id
    NULL,     			-- company_id
    NULL,                 -- ff_id
    ARRAY[2024]                 -- year
);



-- =============================================================================
-- Sample Query: Execute this queries to test the functions
-- =============================================================================

SELECT * FROM gold.fact_energy_generated;

SELECT * FROM gold.func_energy_per_hec_unit_rounded();
SELECT * FROM gold.func_fact_energy();
SELECT * FROM gold.func_fact_energy_monthly();
SELECT * FROM gold.func_fact_energy_quarterly();
SELECT * FROM gold.func_fact_energy_yearly();
SELECT * FROM gold.func_fund_alloc();
SELECT * FROM gold.func_fund_alloc_year();
SELECT * FROM gold.func_outages_frequency();

------------------------------------------------ daily ------------------------------------------------
SELECT * FROM gold.func_fact_energy();

SELECT * 
FROM gold.func_fact_energy(
    NULL,      				-- power_plant_id
    ARRAY['MGI'],        	-- company_id
    NULL,                 	-- generation_source
	NULL,              		-- province
	ARRAY[2,3],				-- month
	NULL,					-- quarter
	NULL					-- year
);

SELECT * 
FROM gold.func_fact_energy(
    Null,      				-- power_plant_id
    NULL,               	-- company_id
    NULL,                 	-- generation_source
	ARRAY['Tarlac'],     	-- province
	NULL,					-- month
	NULL,					-- quarter
	NULL					-- year
);

SELECT * 
FROM gold.func_fact_energy(
    Null,      				-- power_plant_id
    NULL,               	-- company_id
    NULL,              		-- generation_source
	NULL,     				-- province
	NULL,					-- month
	NULL,					-- quarter
	NULL					-- year
);


SELECT * 
FROM gold.func_fact_energy(
    Null,   				-- power_plant_id
    NULL,        			-- company_id
    NULL,              		-- generation_source
	NULL,                  	-- province
	ARRAY[3],				-- month
	NULL,					-- quarter
	NULL					-- year
);


------------------------------------------------ annual ------------------------------------------------ 
SELECT * FROM gold.func_energy_per_hec_unit_rounded();

SELECT * FROM gold.func_energy_per_hec_unit_rounded(
    ARRAY['TSPP1'],       		-- power_plant_id
    NULL,      					-- company_id
    NULL,              			-- generation_source
    NULL       				-- province
);
