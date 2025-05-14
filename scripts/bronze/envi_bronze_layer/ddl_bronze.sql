/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates foundational tables in the 'bronze' schema for 
    environmental data management. It first drops existing versions of the 
    tables to avoid conflicts, then recreates them with the defined structure.

    The tables included are:
    
    1. envi_company_info
       - Stores company-level information including name, site details, and location.

    2. envi_company_property
       - Stores properties owned by companies (e.g., equipment or vehicles) 
         with a link to their corresponding company.

    3. envi_natural_sources
       - Stores natural water sources (e.g., rivers, wells) linked to companies.

    4. envi_water_withdrawal
       - Tracks monthly water withdrawal volumes by company and location.

    5. envi_diesel_consumption
       - Logs diesel consumption per company property, with measurements 
         and dates.

	6. envi_elec_cons
       - Records electricity consumption data by company or property over time.

    7. envi_pow_gen
       - Logs electricity generated internally by the company or its equipment.

    8. envi_nonhaz_waste
       - Tracks disposal amounts of non-hazardous waste by type and destination.

	9. envi_haz_waste
       - Tracks disposal amounts of hazardous waste by type and destination.
	   
    10. envi_activity
       - Monitors and records the environmental actions and initiatives undertaken by each company.
	   
    11. envi_act_output
       - Summarizes the outcomes and results of environmental activities conducted by each company.

    These tables are prepared for eventual normalization through foreign keys 
    and are part of the initial data ingestion (bronze layer) in the data pipeline.
===============================================================================
*/

-- Drop and recreate tables in the 'bronze' schema

-- envi_company_info
DROP TABLE IF EXISTS bronze.envi_company_info;
CREATE TABLE bronze.envi_company_info (
    company_id     VARCHAR(20),       -- Example: PSC, MGI, RGEC
    company_name   VARCHAR(255),
    resources      TEXT,
    site_name      VARCHAR(255),
    site_address   TEXT,
    city_town      VARCHAR(100),
    province       VARCHAR(100),
    zip            VARCHAR(10)
);

-- envi_company_property
DROP TABLE IF EXISTS bronze.envi_company_property;
CREATE TABLE bronze.envi_company_property (
    cp_id      VARCHAR(30),       -- Example: CP-PSC-001
    company_id VARCHAR(20),       -- Referenced to company_info.
    cp_name    VARCHAR(100),
    cp_type    VARCHAR(50)        -- Example values: Equipment, Vehicle
);

-- envi_natural_sources
DROP TABLE IF EXISTS bronze.envi_natural_sources;
CREATE TABLE bronze.envi_natural_sources (
    ns_id      VARCHAR(30),       -- Example: NS-PSC-001
    company_id VARCHAR(20),       -- Referenced to company_info.
    ns_name    VARCHAR(100)
);

-- envi_water_withdrawal
DROP TABLE IF EXISTS bronze.envi_water_withdrawal;
CREATE TABLE bronze.envi_water_withdrawal (
    ww_id                 VARCHAR(30),         -- Example: WW-PSC-2022-002
    company_id            VARCHAR(20),         -- Referenced to company_info.
    year                  SMALLINT,
    month                 VARCHAR(20),
    ns_id                 VARCHAR(30),         -- Referenced to natural sources.
    volume                DOUBLE PRECISION,    -- Allows decimal values (e.g., 123.456)
    unit_of_measurement   VARCHAR(20)
);

-- envi_diesel_consumption
DROP TABLE IF EXISTS bronze.envi_diesel_consumption;
CREATE TABLE bronze.envi_diesel_consumption (
    dc_id                VARCHAR(30),          -- Example: DC-PSC-2024-006
    company_id           VARCHAR(20),          -- Referenced to company info.
    cp_id                VARCHAR(30),          -- Referenced to envi_company_property.
    unit_of_measurement  VARCHAR(20),
    consumption          DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    date                 DATE,
	month			     VARCHAR(15)
);

-- envi_electric_consumption
DROP TABLE IF EXISTS bronze.envi_electric_consumption;
CREATE TABLE bronze.envi_electric_consumption (
    ec_id					VARCHAR(30),           -- Example: EC-PSC-2023-001
    company_id			 	VARCHAR(20),      -- Referenced to company_info.
    unit_of_measurement	 	VARCHAR(20),
    consumption 		 	DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    quarter           	 	VARCHAR(5),
    year               	 	INT
);

-- envi_power_generation
DROP TABLE IF EXISTS bronze.envi_power_generation;
CREATE TABLE bronze.envi_power_generation (
    pg_id 					VARCHAR(30),          -- Example: PG-PSC-2023-001
    company_id 				VARCHAR(20),     -- Referenced to company_info.
    unit_of_measurement 	VARCHAR(20),
    generation 				DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    quarter 				VARCHAR(5),
    year INT
);

-- envi_non_hazard_waste
DROP TABLE IF EXISTS bronze.envi_non_hazard_waste;
CREATE TABLE bronze.envi_non_hazard_waste (
    nhw_id 					VARCHAR(30),           -- Example: NHW-PSC-2024-001
    company_id 				VARCHAR(20),       -- Referenced to company_info.
    waste_source 			VARCHAR(50),     -- Example: Staff House, Security, Utility
    metrics 				VARCHAR(20),
    unit_of_measurement 	VARCHAR(20),
    waste 					DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    month 					VARCHAR(15),
    year 					INT
);

-- envi_hazard_waste
DROP TABLE IF EXISTS bronze.envi_hazard_waste;
CREATE TABLE bronze.envi_hazard_waste (
    hw_id 					VARCHAR(30),          		-- Example: HW-PSC-2023-001
    company_id 				VARCHAR(20),      		-- Referenced to company_info.
    metrics 				VARCHAR(20),
    unit_of_measurement 	VARCHAR(20),
    waste 					DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    quarter 				VARCHAR(2),  		 		-- Example: 'Q1', 'Q2', 'Q3', 'Q4'
    year 					INT
);

-- envi_activity
DROP TABLE IF EXISTS bronze.envi_activity;
CREATE TABLE bronze.envi_activity (
    ea_id 			VARCHAR(30),           -- Example: EA_PSC_001
	metrics 				VARCHAR(20),
    company_id 				VARCHAR(20),       	   -- Referenced to company_info.
    envi_act_name 			TEXT
);

-- envi_act_output
DROP TABLE IF EXISTS bronze.envi_activity_output;
CREATE TABLE bronze.envi_activity_output (
    eao_id 					VARCHAR(30),           		-- Example: EAO-PSC-2018-001
    company_id 				VARCHAR(20),       		    -- Referenced to company_info.
    ea_id 				    VARCHAR(30),                -- Referenced to envi_activity.
    unit_of_measurement 	VARCHAR(20),
    act_output 				INT,
    year INT
);