/*
===============================================================================
DDL Script: Create Gold Views for Environment Data
===============================================================================
*/

-- DROP VIEW IF EXISTS gold.vw_environment_water_abstraction;
DROP VIEW IF EXISTS gold.vw_environment_water_abstraction;
-- Now create our new views
-- =============================================================================
-- Create View: gold.vw_environment_water_abstraction
-- =============================================================================

-- VIEW for gold environment electric vw_environment_water_abstraction
CREATE OR REPLACE VIEW gold.vw_environment_water_abstraction AS
SELECT
    wa.wa_id AS water_abstraction_id,
    cm.company_name,
    CAST(wa.volume AS NUMERIC(10,2)),
    wa.unit_of_measurement AS unit,
    wa.quarter,
    wa.year,
    COALESCE(latest_status.status_name, 'Head Approved') AS status_name
FROM silver.envi_water_abstraction wa
LEFT JOIN (
    SELECT DISTINCT ON (record_id)
        record_id,
        s.status_name
    FROM public.checker_status_log csl
    LEFT JOIN public.status s ON csl.status_id = s.status_id
    ORDER BY record_id, csl.status_timestamp DESC
) latest_status ON wa.wa_id = latest_status.record_id
LEFT JOIN ref.company_main cm ON wa.company_id = cm.company_id
ORDER BY wa.year DESC;


-- DROP VIEW IF EXISTS gold.vw_environment_water_discharge;
DROP VIEW IF EXISTS gold.vw_environment_water_discharge;
-- Now create our new views
-- =============================================================================
-- Create View: gold.vw_environment_water_discharge
-- =============================================================================

-- VIEW for gold environment electric vw_environment_water_discharge
CREATE OR REPLACE VIEW gold.vw_environment_water_discharge AS
SELECT
    wd.wd_id AS water_discharge_id,
    cm.company_name,
    CAST(wd.volume AS NUMERIC(10,2)),
    wd.unit_of_measurement AS unit,
    wd.quarter,
    wd.year,
    COALESCE(latest_status.status_name, 'Head Approved') AS status_name
FROM silver.envi_water_discharge wd
LEFT JOIN (
    SELECT DISTINCT ON (record_id)
        record_id,
        s.status_name
    FROM public.checker_status_log csl
    LEFT JOIN public.status s ON csl.status_id = s.status_id
    ORDER BY record_id, csl.status_timestamp DESC
) latest_status ON wd.wd_id = latest_status.record_id
LEFT JOIN ref.company_main cm ON wd.company_id = cm.company_id
ORDER BY wd.year DESC;


-- DROP VIEW IF EXISTS gold.vw_environment_water_consumption;
DROP VIEW IF EXISTS gold.vw_environment_water_consumption;
-- Now create our new views
-- =============================================================================
-- Create View: gold.vw_environment_water_consumption
-- =============================================================================

-- VIEW for gold environment electric vw_environment_water_consumption
CREATE OR REPLACE VIEW gold.vw_environment_water_consumption AS
SELECT
    wc.wc_id AS water_consumption_id,
    cm.company_name,
    CAST(wc.volume AS NUMERIC(10,2)),
    wc.unit_of_measurement AS unit,
    wc.quarter,
    wc.year,
    COALESCE(latest_status.status_name, 'Head Approved') AS status_name
FROM silver.envi_water_consumption wc
LEFT JOIN (
    SELECT DISTINCT ON (record_id)
        record_id,
        s.status_name
    FROM public.checker_status_log csl
    LEFT JOIN public.status s ON csl.status_id = s.status_id
    ORDER BY record_id, csl.status_timestamp DESC
) latest_status ON wc.wc_id = latest_status.record_id
LEFT JOIN ref.company_main cm ON wc.company_id = cm.company_id
ORDER BY wc.year DESC;


-- DROP VIEW IF EXISTS gold.vw_environment_diesel_consumption;
DROP VIEW IF EXISTS gold.vw_environment_diesel_consumption;

-- Now create our new views
-- =============================================================================
-- Create View: gold.vw_environment_diesel_consumption
-- =============================================================================

-- VIEW for gold environment diesel consumption
CREATE OR REPLACE VIEW gold.vw_environment_diesel_consumption AS
SELECT
    edc.dc_id AS diesel_consumption_id,
    cm.company_name,
    ecp.cp_name AS company_property_name,
    ecp.cp_type AS company_property_type,
    edc.unit_of_measurement,
    edc.consumption,
    edc.month,
    edc.year,
    edc.quarter,
    edc.date,
    COALESCE(latest_status.status_name, 'Head Approved') AS status_name
FROM silver.envi_company_property ecp
RIGHT JOIN silver.envi_diesel_consumption edc ON edc.cp_id = ecp.cp_id
LEFT JOIN (
    SELECT DISTINCT ON (record_id)
        record_id,
        s.status_name
    FROM public.checker_status_log csl
    LEFT JOIN public.status s ON csl.status_id = s.status_id
    ORDER BY record_id, csl.status_timestamp DESC
) latest_status ON edc.cp_id = latest_status.record_id
LEFT JOIN ref.company_main cm ON edc.company_id = cm.company_id
ORDER BY edc.year DESC;


-- DROP VIEW IF EXISTS gold.vw_environment_electric_consumption;
DROP VIEW IF EXISTS gold.vw_environment_electric_consumption;
-- Now create our new views
-- =============================================================================
-- Create View: gold.vw_environment_electrical_consumption
-- =============================================================================

-- VIEW for gold environment electric consumption
CREATE OR REPLACE VIEW gold.vw_environment_electric_consumption AS
SELECT
    ec.ec_id AS electric_consumption_id,
    cm.company_name,
    ec.source AS consumption_source,
    ec.unit_of_measurement,
    ec.consumption,
    ec.quarter,
    ec.year,
    COALESCE(latest_status.status_name, 'Head Approved') AS status_name
FROM silver.envi_electric_consumption ec
LEFT JOIN (
    SELECT DISTINCT ON (record_id)
        record_id,
        s.status_name
    FROM public.checker_status_log csl
    LEFT JOIN public.status s ON csl.status_id = s.status_id
    ORDER BY record_id, csl.status_timestamp DESC
) latest_status ON ec.ec_id = latest_status.record_id
LEFT JOIN ref.company_main cm ON ec.company_id = cm.company_id
ORDER BY ec.year DESC;


-- DROP VIEW IF EXISTS gold.vw_environment_non_hazard_waste;
DROP VIEW IF EXISTS gold.vw_environment_non_hazard_waste;
-- Now create our new views
-- =============================================================================
-- Create View: gold.vw_environment_non_hazard_waste
-- =============================================================================

-- VIEW for gold environment non hazard waste
CREATE OR REPLACE VIEW gold.vw_environment_non_hazard_waste AS
SELECT
    nhw.nhw_id AS non_hazardous_waste_id,
    cm.company_name,
    nhw.metrics,
    nhw.unit_of_measurement,
    nhw.waste,
    nhw.quarter,
    nhw.year,
    COALESCE(latest_status.status_name, 'Head Approved') AS status_name
FROM silver.envi_non_hazard_waste nhw
LEFT JOIN (
    SELECT DISTINCT ON (record_id)
        record_id,
        s.status_name
    FROM public.checker_status_log csl
    LEFT JOIN public.status s ON csl.status_id = s.status_id
    ORDER BY record_id, csl.status_timestamp DESC
) latest_status ON nhw.nhw_id = latest_status.record_id
LEFT JOIN ref.company_main cm ON nhw.company_id = cm.company_id
ORDER BY nhw.year DESC;


-- DROP VIEW IF EXISTS gold.vw_environment_hazard_waste_generated;
DROP VIEW IF EXISTS gold.vw_environment_hazard_waste_generated;
-- Now create our new views
-- =============================================================================
-- Create View: gold.vw_environment_hazard_waste_generated
-- =============================================================================

-- VIEW for gold environment hazard waste generated
CREATE OR REPLACE VIEW gold.vw_environment_hazard_waste_generated AS
SELECT
    ehwg.hwg_id AS hazard_waste_generated_id,
    cm.company_name,
    ehwg.metrics AS waste_type,
    ehwg.unit_of_measurement AS unit,
    ehwg.waste_generated AS waste_generated,
    ehwg.quarter,
    ehwg.year,
    COALESCE(latest_status.status_name, 'Head Approved') AS status_name
FROM silver.envi_hazard_waste_generated ehwg
LEFT JOIN (
    SELECT DISTINCT ON (record_id)
        record_id,
        s.status_name
    FROM public.checker_status_log csl
    LEFT JOIN public.status s ON csl.status_id = s.status_id
    ORDER BY record_id, csl.status_timestamp DESC
) latest_status ON ehwg.hwg_id = latest_status.record_id
LEFT JOIN ref.company_main cm ON ehwg.company_id = cm.company_id
ORDER BY ehwg.year DESC;


-- DROP VIEW IF EXISTS gold.vw_environment_hazard_waste_disposed;
DROP VIEW IF EXISTS gold.vw_environment_hazard_waste_disposed;
-- Now create our new views
-- =============================================================================
-- Create View: gold.vw_environment_hazard_waste_disposed
-- =============================================================================

-- VIEW for gold environment hazard waste disposed
CREATE OR REPLACE VIEW gold.vw_environment_hazard_waste_disposed AS
SELECT
    ehwd.hwd_id AS hazard_waste_disposed_id,
    cm.company_name,
    ehwd.metrics AS waste_type,
    ehwd.unit_of_measurement AS unit,
    ehwd.waste_disposed AS waste_disposed,
    ehwd.year,
    COALESCE(latest_status.status_name, 'Head Approved') AS status_name
FROM silver.envi_hazard_waste_disposed ehwd
LEFT JOIN (
    SELECT DISTINCT ON (record_id)
        record_id,
        s.status_name
    FROM public.checker_status_log csl
    LEFT JOIN public.status s ON csl.status_id = s.status_id
    ORDER BY record_id, csl.status_timestamp DESC
) latest_status ON ehwd.hwd_id = latest_status.record_id
LEFT JOIN ref.company_main cm ON ehwd.company_id = cm.company_id
ORDER BY ehwd.year DESC;
