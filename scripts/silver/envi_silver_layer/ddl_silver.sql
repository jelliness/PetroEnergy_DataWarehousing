/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

-- Drop and recreate tables in the 'silver' schema

-- envi_company_info
DROP TABLE IF EXISTS silver.envi_company_info;
CREATE TABLE silver.envi_company_info (
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
DROP TABLE IF EXISTS silver.envi_company_property;
CREATE TABLE silver.envi_company_property (
    cp_id      VARCHAR(30),       -- Example: CP-PSC-001
    company_id VARCHAR(20),       -- Referenced to company_info.
    cp_name    VARCHAR(100),
    cp_type    VARCHAR(50)        -- Example values: Equipment, Vehicle
);

-- envi_natural_sources
DROP TABLE IF EXISTS silver.envi_natural_sources;
CREATE TABLE silver.envi_natural_sources (
    ns_id      VARCHAR(30),       -- Example: NS-PSC-001
    company_id VARCHAR(20),       -- Referenced to company_info.
    ns_name    VARCHAR(100)
);

-- envi_water_withdrawal
DROP TABLE IF EXISTS silver.envi_water_withdrawal;
CREATE TABLE silver.envi_water_withdrawal (
    ww_id                 VARCHAR(30),         -- Example: WW-PSC-2022-002
    company_id            VARCHAR(20),         -- Referenced to company_info.
    year                  SMALLINT,
    month                 VARCHAR(20),
    ns_id                 VARCHAR(30),         -- Referenced to natural sources.
    volume                DOUBLE PRECISION,    -- Allows decimal values (e.g., 123.456)
    unit_of_measurement   VARCHAR(20)
);

-- envi_diesel_consumption
DROP TABLE IF EXISTS silver.envi_diesel_consumption;
CREATE TABLE silver.envi_diesel_consumption (
    dc_id                VARCHAR(30),          -- Example: DC-PSC-2024-006
    company_id           VARCHAR(20),          -- Referenced to company info.
    cp_id                VARCHAR(30),          -- Referenced to envi_company_property.
    unit_of_measurement  VARCHAR(20),
    consumption          DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    date                 DATE,
	month			     VARCHAR(15)
);

-- envi_electric_consumption
DROP TABLE IF EXISTS silver.envi_electric_consumption;
CREATE TABLE silver.envi_electric_consumption (
    ec_id					VARCHAR(30),           -- Example: EC-PSC-2023-001
    company_id			 	VARCHAR(20),      -- Referenced to company_info.
    unit_of_measurement	 	VARCHAR(20),
    consumption 		 	DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    quarter           	 	VARCHAR(5),
    year               	 	INT
);

-- envi_power_generation
DROP TABLE IF EXISTS silver.envi_power_generation;
CREATE TABLE silver.envi_power_generation (
    pg_id 					VARCHAR(30),          -- Example: PG-PSC-2023-001
    company_id 				VARCHAR(20),     -- Referenced to company_info.
    unit_of_measurement 	VARCHAR(20),
    generation 				DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    quarter 				VARCHAR(5),
    year INT
);

-- envi_non_hazard_waste
DROP TABLE IF EXISTS silver.envi_non_hazard_waste;
CREATE TABLE silver.envi_non_hazard_waste (
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
DROP TABLE IF EXISTS silver.envi_hazard_waste;
CREATE TABLE silver.envi_hazard_waste (
    hw_id 					VARCHAR(30),          		-- Example: NHW-PSC-2024-001
    company_id 				VARCHAR(20),      		-- Referenced to company_info.
    metrics 				VARCHAR(20),
    unit_of_measurement 	VARCHAR(20),
    waste 					DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    quarter 				VARCHAR(2),  		 		-- Example: 'Q1', 'Q2', 'Q3', 'Q4'
    year 					INT
);

-- envi_activity
DROP TABLE IF EXISTS silver.envi_activity;
CREATE TABLE silver.envi_activity (
    envi_act_id 			VARCHAR(30),           -- Example: NHW-PSC-2024-001
	metrics 				VARCHAR(20),
    company_id 				VARCHAR(20),       	   -- Referenced to company_info.
    envi_act_name 			TEXT
);

-- envi_act_output
DROP TABLE IF EXISTS silver.envi_activity_output;
CREATE TABLE silver.envi_activity_output (
    nhw_id 					VARCHAR(30),           		-- Example: NHW-PSC-2024-001
    company_id 				VARCHAR(20),       		-- Referenced to company_info.
    envi_act 				VARCHAR(20),
    unit_of_measurement 	VARCHAR(20),
    act_output 				INT,
    year INT
);