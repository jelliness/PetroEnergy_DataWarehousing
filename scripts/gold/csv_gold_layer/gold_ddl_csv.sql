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
CREATE OR REPLACE VIEW gold.fact_energy_generated AS
SELECT 
    er.power_plant_id,
    CAST(er.date_generated AS DATE) AS date_generated,
    er.energy_generated_kwh,
    er.co2_avoidance_kg,
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
LEFT JOIN gold.dim_date dd ON CAST(er.date_generated AS DATE) = dd.date_id;