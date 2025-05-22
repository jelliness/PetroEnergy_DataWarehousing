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
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL, 
    p_month INT[] DEFAULT NULL,
    p_quarter INT[] DEFAULT NULL,
    p_year INT[] DEFAULT NULL
)
RETURNS TABLE (
    power_plant_id VARCHAR(10),
    company_id VARCHAR(10),
    generation_sources TEXT,
    province VARCHAR(30),
    energy_generated_kwh NUMERIC(10,2),
    co2_avoidance_tons NUMERIC(10,2),
    date_generated DATE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        feg.power_plant_id,
        feg.company_id,
        feg.generation_source,
        feg.province,
        CAST(SUM(feg.energy_generated_kwh) AS NUMERIC(10,2)) AS energy_generated_kwh,
        CAST(SUM(feg.co2_avoidance_tons) AS NUMERIC(10,2)) AS co2_avoidance_tons,
        feg.date_generated
    FROM gold.fact_energy_generated feg
    WHERE (p_power_plant_id IS NULL OR feg.power_plant_id = ANY(p_power_plant_id))
    	AND (p_company_id IS NULL OR feg.company_id = ANY(p_company_id))
	    AND (p_generation_source IS NULL OR feg.generation_source = ANY(p_generation_source))
	    AND (p_province IS NULL OR feg.province = ANY(p_province))
	    AND (p_start_date IS NULL OR feg.date_generated >= p_start_date)
	    AND (p_end_date IS NULL OR feg.date_generated <= p_end_date)
        AND (p_month IS NULL OR feg.month = ANY(p_month))   
        AND (p_quarter IS NULL OR feg.quarter = ANY(p_quarter))   
        AND (p_year IS NULL OR feg.year = ANY(p_year))   
    GROUP BY 
        feg.power_plant_id,
        feg.company_id, 
        feg.generation_source,
        feg.province,
        feg.date_generated,
        feg.month
	ORDER BY 
        feg.date_generated desc;

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
    p_start_date          DATE          DEFAULT NULL,
    p_end_date            DATE          DEFAULT NULL,
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
      AND (p_start_date IS NULL OR feg.date_generated >= p_start_date)
      AND (p_end_date IS NULL OR feg.date_generated <= p_end_date)
      AND (p_month IS NULL OR feg.month = ANY(p_month))   
      AND (p_quarter IS NULL OR feg.quarter = ANY(p_quarter))   
      AND (p_year IS NULL OR feg.year = ANY(p_year))   
    GROUP BY 
        feg.year,
        feg.month,
        feg.month_name
    ORDER BY 
        feg.year desc,
        feg.month desc;
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
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL,
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
	    AND (p_start_date IS NULL OR feg.date_generated >= p_start_date)
	    AND (p_end_date IS NULL OR feg.date_generated <= p_end_date)
        AND (p_month IS NULL OR feg.month = ANY(p_month))   
        AND (p_quarter IS NULL OR feg.quarter = ANY(p_quarter))   
        AND (p_year IS NULL OR feg.year = ANY(p_year))   
    GROUP BY 
        feg.quarter,
		feg.year
		
	ORDER BY 
		feg.year desc,
        feg.quarter desc;
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
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL,
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
	    AND (p_start_date IS NULL OR feg.date_generated >= p_start_date)
	    AND (p_end_date IS NULL OR feg.date_generated <= p_end_date)
        AND (p_month IS NULL OR feg.month = ANY(p_month))   
        AND (p_quarter IS NULL OR feg.quarter = ANY(p_quarter))   
        AND (p_year IS NULL OR feg.year = ANY(p_year))   
    GROUP BY 
        feg.year
	ORDER BY 
        feg.year desc;
END;
$$ LANGUAGE plpgsql;



-- =============================================================================
-- Create Function for Number of Houses Powered (Total Annual Energy Generated)
-- =============================================================================
CREATE OR REPLACE FUNCTION gold.func_energy_per_hec_unit_rounded(
    p_power_plant_id VARCHAR(10)[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_generation_source TEXT[] DEFAULT NULL,
    p_province VARCHAR(30)[] DEFAULT NULL,
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
RETURNS TABLE (
    year SMALLINT,
    monthly_energy_generated NUMERIC,
    hec_value DECIMAL(10,4),
    energy_per_hec_unit_rounded NUMERIC
)
AS $$
BEGIN
    RETURN QUERY
    WITH latest_hec AS (
        SELECT DISTINCT ON (hec_id)
            chf.hec_id,
            chf.hec_value,
            chf.hec_year
        FROM ref.ref_hec_factors chf
        ORDER BY hec_id, hec_year DESC
    )
    SELECT 
        fg.year::SMALLINT,
        SUM(fg.energy_generated_kwh) AS monthly_energy_generated,
        lh.hec_value,
        CASE 
            WHEN (SUM(fg.energy_generated_kwh) / 12) / NULLIF(lh.hec_value, 0) / 1000.0 >= 1
                THEN CEIL((SUM(fg.energy_generated_kwh) / 12) / NULLIF(lh.hec_value, 0) / 1000.0) * 1000
            ELSE CEIL((SUM(fg.energy_generated_kwh) / 12) / NULLIF(lh.hec_value, 0) / 100.0) * 100
        END AS energy_per_hec_unit_rounded
    FROM gold.fact_energy_generated fg
    CROSS JOIN latest_hec lh
	WHERE (p_power_plant_id IS NULL OR fg.power_plant_id = ANY(p_power_plant_id))
     	AND (p_company_id IS NULL OR fg.company_id = ANY(p_company_id))
     	AND (p_generation_source IS NULL OR fg.generation_source = ANY(p_generation_source))
      	AND (p_province IS NULL OR fg.province = ANY(p_province))
      	AND (p_start_date IS NULL OR fg.date_generated >= p_start_date)
     	AND (p_end_date IS NULL OR fg.date_generated <= p_end_date)
    GROUP BY fg.year, lh.hec_id, lh.hec_value
    ORDER BY fg.year, lh.hec_id;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION gold.func_fund_alloc(
    p_power_plant_id VARCHAR(10)[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_ff_id VARCHAR(10)[] DEFAULT NULL,
    p_month INT[] DEFAULT NULL,
    p_start_month INT DEFAULT NULL,
    p_end_month INT DEFAULT NULL,
	p_year INT[] DEFAULT NULL
)
RETURNS TABLE (
    month_name TEXT,
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
        dd.month_name,
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
     	AND (p_month IS NULL OR dd.month = ANY(p_month))
     	AND (p_start_month IS NULL OR dd.month >= p_start_month)
     	AND (p_end_month IS NULL OR dd.month <= p_end_month)
	 	AND (p_year IS NULL OR dd.year = ANY(p_year))
    GROUP BY 
        dd.month_name,
        dd.month,
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
    p_year INT[] DEFAULT NULL,
    p_start_year INT DEFAULT NULL,
    p_end_year INT DEFAULT NULL
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
     	AND (p_start_year IS NULL OR dd.year >= p_start_year)
     	AND (p_end_year IS NULL OR dd.year <= p_end_year)
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




-- sample query

SELECT *
FROM gold.func_fund_alloc(
    NULL,  		-- power_plant_id
    ARRAY['PSC'],                         -- company_id
    NULL,                         -- ff_id
    NULL,                         -- month
    3,                         -- start_month
    4,                          -- end_month
	ARRAY[2024]							-- year
);

SELECT *
FROM gold.func_fund_alloc_year(
    NULL,     -- power_plant_id
    NULL,     			-- company_id
    NULL,                 -- ff_id
    ARRAY[2024],                 -- year
    NULL,                 -- start_year
    NULL                  -- end_year
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

------------------------------------------------ daily ------------------------------------------------
SELECT * FROM gold.func_fact_energy();

SELECT * 
FROM gold.func_fact_energy(
    NULL,      				-- power_plant_id
    ARRAY['MGI'],        	-- company_id
    NULL,                 	-- generation_source
	NULL,              		-- province
	NULL,                 	-- start
	NULL,              		-- end
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
	NULL,               	-- start
	NULL,              		-- end
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
	'2025-03-01',     	    -- start
	NULL,                   -- end
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
	'2025-01-01',			-- start
	'2025-03-03',			-- end
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
	NULL,					-- start
	NULL,					-- end
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
    NULL,       				-- province
    '2025-05-01',        		-- start_date
    NULL						-- end_date
);
