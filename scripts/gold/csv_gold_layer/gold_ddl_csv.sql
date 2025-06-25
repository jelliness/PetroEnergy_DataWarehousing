/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_date
-- =============================================================================
DROP VIEW IF EXISTS gold.dim_date CASCADE;
CREATE OR REPLACE VIEW gold.dim_date AS
SELECT
    d::DATE AS date_id,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    EXTRACT(MONTH FROM d)::INT AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(DAY FROM d)::INT AS day,
    EXTRACT(DOW FROM d)::INT AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    EXTRACT(WEEK FROM d)::INT AS week_of_year
FROM generate_series('2000-01-01'::DATE, '2050-12-31'::DATE, '1 day') AS d;

-- =============================================================================
-- Create Dimension: gold.dim_powerplant_profile
-- =============================================================================
DROP VIEW IF EXISTS gold.dim_powerplant_profile CASCADE;
CREATE OR REPLACE VIEW gold.dim_powerplant_profile AS 
SELECT
    pp.power_plant_id,
    co.company_id,
    ef.generation_source,
    pp.site_name,
    co.company_name,
    pp.site_address, 
    pp.city_town,
    pp.province,
    pp.country
FROM ref.ref_power_plants pp
LEFT JOIN ref.company_main co ON pp.company_id = co.company_id
LEFT JOIN ref.ref_emission_factors ef ON pp.ef_id = ef.ef_id;

-- =============================================================================
-- Create Fact: gold.fact_energy_generated
-- =============================================================================
DROP VIEW IF EXISTS gold.fact_energy_generated CASCADE;
CREATE OR REPLACE VIEW gold.fact_energy_generated AS
SELECT 
    er.power_plant_id,
    CAST(er.date_generated AS DATE) AS date_generated,
    er.energy_generated_kwh,
    er.co2_avoidance_kg*0.001 as co2_avoidance_tons,
	
    -- from dim_power_plant
    pp.company_id,
    pp.generation_source,
    pp.site_name,
    pp.company_name,
    pp.site_address,
    pp.city_town,
    pp.province,
    pp.country,

    -- from dim_date
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,
    dd.day,
    dd.day_of_week,
    dd.day_name,
    dd.is_weekend,
    dd.week_of_year

FROM silver.csv_energy_records er
LEFT JOIN gold.dim_powerplant_profile pp ON er.power_plant_id = pp.power_plant_id
LEFT JOIN gold.dim_date dd ON CAST(er.date_generated AS DATE) = dd.date_id
LEFT JOIN record_status rs ON rs.record_id=er.energy_id
WHERE rs.status_id = 'APP';

-- =============================================================================
-- Create Fact: gold.fact_fund_allocation
-- =============================================================================
DROP VIEW IF EXISTS gold.fact_fund_allocation CASCADE;
CREATE OR REPLACE VIEW gold.fact_fund_allocation AS
SELECT 
    dd.month_name,
	dd.month,
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
LEFT JOIN gold.dim_date dd on dd.date_id = DATE_TRUNC('month', er.date_generated)
CROSS JOIN ref.ref_fa_factors ff
GROUP BY 
    dd.month_name,
    dd.month,
	dd.year,
    DATE_TRUNC('month', er.date_generated),
    pp.power_plant_id,
    ff.ff_id,
    ff.ff_name
ORDER BY dd.month desc;

-- select * from gold.fact_fund_allocation;


-- =============================================================================
-- Create Fact: gold.fact_fund_allocation (yearly)
-- =============================================================================
DROP VIEW IF EXISTS gold.fact_fund_allocation_yearly CASCADE;
CREATE OR REPLACE VIEW gold.fact_fund_allocation_yearly AS
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
LEFT JOIN gold.dim_date dd on dd.date_id = DATE_TRUNC('month', er.date_generated)
CROSS JOIN ref.ref_fa_factors ff
GROUP BY 
    dd.year,
    pp.power_plant_id,
    ff.ff_id,
    ff.ff_name
ORDER BY dd.year desc;

-- select * from gold.fact_fund_allocation_yearly;

-- =============================================================================
-- Create Fact: gold.fact_outages_frequency 
-- =============================================================================

DROP VIEW IF EXISTS gold.fact_outages_frequency CASCADE;
CREATE OR REPLACE VIEW gold.fact_outages_frequency AS
SELECT power_plant_id, company_id, generation_source,site_name, company_name, city_town, province, year, month, month_name, COUNT(*) 
FROM gold.fact_energy_generated 
WHERE energy_generated_kwh = 0
GROUP BY power_plant_id, company_id, generation_source,site_name, company_name, city_town, province, year, month, month_name

-- =============================================================================
-- Create Fact: gold.fact_household_powered 
-- =============================================================================
CREATE OR REPLACE VIEW gold.fact_household_powered AS
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
        fg.power_plant_id,
        fg.company_id,
        fg.generation_source,
        fg.site_name,
        fg.company_name,
        fg.province,
        fg.month,
        fg.month_name,
        fg.quarter,
        SUM(fg.energy_generated_kwh) AS energy_generated,
        lh.hec_value,
		ROUND((SUM(fg.energy_generated_kwh) / 12) / NULLIF(lh.hec_value, 0), -2) as household_powered
    FROM gold.fact_energy_generated fg
    CROSS JOIN latest_hec lh
    GROUP BY 
        fg.year,
        fg.power_plant_id,
        fg.company_id,
        fg.site_name,
        fg.company_name,
        fg.generation_source,
        fg.province,
        fg.month,
        fg.month_name,
        fg.quarter,
        lh.hec_id,
        lh.hec_value
    ORDER BY fg.year, lh.hec_id;