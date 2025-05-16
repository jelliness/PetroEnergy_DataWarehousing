/*
===============================================================================
DDL Script: Create Gold Views for Environment Data
===============================================================================
*/

-- DROP VIEW IF EXISTS gold.vw_environment_water_withdrawal;
DROP VIEW IF EXISTS gold.vw_environment_water_withdrawal;

-- Now create our new views
-- =============================================================================
-- Create View: gold.vw_environment_water_withdrawal
-- =============================================================================

-- VIEW for gold environtment water withdrawal
CREATE OR REPLACE VIEW gold.vw_environment_water_withdrawal AS
SELECT
    eww.ww_id                        AS water_withdrawal_id,
    eww.company_id                    AS company_id,
    ens.ns_name                        AS natural_sources,
    eww.volume                        AS water_volume,
    eww.unit_of_measurement,
    eww.month,
    eww.quarter,
    eww.year                        
FROM silver.envi_natural_sources ens
RIGHT JOIN silver.envi_water_withdrawal eww ON eww.ns_id = ens.ns_id;

-- DROP VIEW IF EXISTS gold.vw_environment_diesel_consumption;
DROP VIEW IF EXISTS gold.vw_environment_diesel_consumption;

-- Now create our new views
-- =============================================================================
-- Create View: gold.vw_environment_diesel_consumption
-- =============================================================================

-- VIEW for gold environment diesel consumption
CREATE OR REPLACE VIEW gold.vw_environment_diesel_consumption AS
SELECT
    edc.dc_id                    AS diesel_consumption_id,
    edc.company_id                AS company_id,
    ecp.cp_name                    AS company_property_name,
    ecp.cp_type                    AS company_property_type,
    edc.unit_of_measurement,
    edc.consumption,
    edc.month,
    edc.year,
    edc.quarter,
    edc.date        
FROM silver.envi_company_property ecp
RIGHT JOIN silver.envi_diesel_consumption edc ON edc.cp_id = ecp.cp_id;

-- DROP VIEW IF EXISTS gold.vw_environment_electric_consumption;
DROP VIEW IF EXISTS gold.vw_environment_electric_consumption;
-- Now create our new views
-- =============================================================================
-- Create View: gold.vw_environment_electrical_consumption
-- =============================================================================

-- VIEW for gold environment electric consumption
CREATE OR REPLACE VIEW gold.vw_environment_electric_consumption AS
SELECT
    ec.ec_id                    AS electric_consumption_id,
    ec.company_id                AS company_id,
    ec.unit_of_measurement,
    ec.consumption,
    ec.quarter,
    ec.year
FROM silver.envi_electric_consumption ec;

-- DROP VIEW IF EXISTS gold.vw_environment_non_hazard_waste;
DROP VIEW IF EXISTS gold.vw_environment_non_hazard_waste;
-- Now create our new views
-- =============================================================================
-- Create View: gold.vw_environment_non_hazard_waste
-- =============================================================================

-- VIEW for gold environment non hazard waste
CREATE OR REPLACE VIEW gold.vw_environment_non_hazard_waste AS
SELECT
    nhw.nhw_id                    AS non_hazardous_waste_id,
    nhw.company_id                AS company_id,
    nhw.waste_source,      
    nhw.metrics,             
    nhw.unit_of_measurement,
    nhw.waste,               
    nhw.month,
    nhw.quarter,
    nhw.year
FROM silver.envi_non_hazard_waste nhw;

-- DROP VIEW IF EXISTS gold.vw_environment_hazard_waste_generated;
DROP VIEW IF EXISTS gold.vw_environment_hazard_waste_generated;
-- Now create our new views
-- =============================================================================
-- Create View: gold.vw_environment_hazard_waste_generated
-- =============================================================================

-- VIEW for gold environment hazard waste generated
CREATE VIEW gold.vw_environment_hazard_waste_generated AS
SELECT
    ehwg.hwg_id                           AS hazard_waste_generated_id,
    ehwg.company_id                     AS company_name,
    ehwg.metrics                        AS waste_type,
    ehwg.unit_of_measurement            AS unit,
    ehwg.waste_generated                AS generate,                        
    ehwg.quarter,                                                                    
    ehwg.year                               
FROM silver.envi_hazard_waste_generated ehwg;

-- DROP VIEW IF EXISTS gold.vw_environment_hazard_waste_disposed;
DROP VIEW IF EXISTS gold.vw_environment_hazard_waste_disposed;
-- Now create our new views
-- =============================================================================
-- Create View: gold.vw_environment_hazard_waste_disposed
-- =============================================================================

-- VIEW for gold environment hazard waste disposed
CREATE VIEW gold.vw_environment_hazard_waste_disposed AS
SELECT
    ehwd.hwd_id                            AS hazard_waste_generated_id,
    ehwd.company_id                     AS company_name,
    ehwd.metrics                        AS waste_type,
    ehwd.unit_of_measurement            AS unit,
    ehwd.waste_disposed                    AS disposed,
    ehwd.year
FROM silver.envi_hazard_waste_disposed ehwd;