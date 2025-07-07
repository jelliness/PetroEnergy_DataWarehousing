/*
======================================================================================
Function Script: Create Gold Functions for Power Plant Data
======================================================================================

======================================================================================
NOTE: Execute the create functions first, then execute the sample queries for checking
======================================================================================
*/

-- ===================================================================================
-- Create Functions for Energy Generated 
-- ===================================================================================
DROP FUNCTION IF EXISTS gold.func_fact_energy;

CREATE OR REPLACE FUNCTION gold.func_fact_energy(
    p_power_plant_id VARCHAR(10)[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_generation_source TEXT[] DEFAULT NULL,
    p_province VARCHAR(30)[] DEFAULT NULL,
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
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
            CAST(SUM(feg.energy_generated_kwh) AS NUMERIC(1000,2)) AS total_energy_generated,
            CAST(SUM(feg.co2_avoidance_tons) AS NUMERIC(1000,2)) AS total_co2_avoidance
        FROM gold.fact_energy_generated feg
        WHERE (p_power_plant_id IS NULL OR feg.power_plant_id = ANY(p_power_plant_id))
            AND (p_company_id IS NULL OR feg.company_id = ANY(p_company_id))
            AND (p_generation_source IS NULL OR feg.generation_source = ANY(p_generation_source))
            AND (p_province IS NULL OR feg.province = ANY(p_province))
            AND (
                (p_start_date IS NULL AND p_end_date IS NULL)
                OR (
                    to_date(feg.year || '-' || feg.month || '-01', 'YYYY-MM-DD')
                    BETWEEN p_start_date AND p_end_date
                )
            )
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



-- =============================================================================
-- Create Function for Number of Houses Powered 
-- =============================================================================

DROP FUNCTION IF EXISTS gold.func_household_powered;

CREATE OR REPLACE FUNCTION gold.func_household_powered(
    p_power_plant_id VARCHAR(10)[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_generation_source TEXT[] DEFAULT NULL,
    p_province VARCHAR(30)[] DEFAULT NULL,
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
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
        AND (
            (p_start_date IS NULL AND p_end_date IS NULL)
            OR (
                to_date(fg.year || '-' || fg.month || '-01', 'YYYY-MM-DD')
                BETWEEN p_start_date AND p_end_date
            )
        )
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
        fg.quarter
    ORDER BY 
        fg.year DESC,
        fg.month DESC;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Create Function for Fund Allocation 
-- =============================================================================


DROP FUNCTION IF EXISTS gold.func_fund_alloc;

CREATE OR REPLACE FUNCTION gold.func_fund_alloc(
    p_power_plant_id VARCHAR(10)[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_ff_id VARCHAR(10)[] DEFAULT NULL,
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL,
    p_ff_category VARCHAR(20) DEFAULT NULL
)
RETURNS TABLE (
    month INT,
    month_name TEXT,
    quarter INT,
    year SMALLINT,
    power_plant_id  VARCHAR(10),
    company_id VARCHAR(10),
    ff_id VARCHAR(10),
    ff_name TEXT,
    ff_percentage NUMERIC(5,4),
    ff_category VARCHAR(20),
    power_generated_peso NUMERIC(12,2),
    funds_allocated_peso NUMERIC(12,2)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT *
    FROM gold.fact_fund_allocation fa
    WHERE (p_power_plant_id IS NULL OR fa.power_plant_id = ANY(p_power_plant_id))
      AND (p_company_id IS NULL OR fa.company_id = ANY(p_company_id))
      AND (p_ff_id IS NULL OR fa.ff_id = ANY(p_ff_id))
      AND (p_ff_category IS NULL OR fa.ff_category = p_ff_category)
      AND (
          (p_start_date IS NULL AND p_end_date IS NULL)
          OR (TO_DATE(fa.year || '-' || fa.month || '-01', 'YYYY-MM-DD') BETWEEN p_start_date AND p_end_date)
      )
    ORDER BY fa.year, fa.month DESC, fa.quarter;
END;
$$ LANGUAGE plpgsql;


-- ===================================================================================
-- Create Function for CO2 Equivalence per Metric
-- ===================================================================================
DROP FUNCTION IF EXISTS gold.func_co2_equivalence_per_metric;

CREATE OR REPLACE FUNCTION gold.func_co2_equivalence_per_metric(
    p_power_plant_id VARCHAR(10)[] DEFAULT NULL,
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_generation_source TEXT[] DEFAULT NULL,
    p_province VARCHAR(30)[] DEFAULT NULL,
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
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
      AND (
            (p_start_date IS NULL AND p_end_date IS NULL)
            OR (
                to_date(fe.year || '-' || fe.month || '-01', 'YYYY-MM-DD')
                BETWEEN p_start_date AND p_end_date
            )
      )
    GROUP BY 
        ce.equivalence_category,
        ce.equivalence_label,
        ce.metric,
        ce.equivalent_value_co2_emissions;
END;
$$ LANGUAGE plpgsql;

