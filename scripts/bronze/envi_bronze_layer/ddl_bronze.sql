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

	10. envi_elec_cons
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

-- envi_company_property
DROP TABLE IF EXISTS bronze.envi_company_property;
CREATE TABLE bronze.envi_company_property (
    cp_id      VARCHAR(20),       -- Example: CP-PSC-001
    company_id VARCHAR(10),       -- Referenced to company_info.
    cp_name    VARCHAR(30),
    cp_type    VARCHAR(15)        -- Example values: Equipment, Vehicle
);

-- envi_natural_sources
DROP TABLE IF EXISTS bronze.envi_natural_sources;
CREATE TABLE bronze.envi_natural_sources (
    ns_id      VARCHAR(20),       -- Example: NS-PSC-001
    company_id VARCHAR(10),       -- Referenced to company_info.
    ns_name    VARCHAR(30)
);

-- envi_water_withdrawal
DROP TABLE IF EXISTS bronze.envi_water_withdrawal;
CREATE TABLE bronze.envi_water_withdrawal (
    ww_id                 VARCHAR(20),         -- Example: WW-PSC-2022-002
    company_id            VARCHAR(10),         -- Referenced to company_info.
    year                  SMALLINT,
    month                 VARCHAR(10),
    ns_id                 VARCHAR(20),         -- Referenced to natural sources.
    volume                DOUBLE PRECISION,    -- Allows decimal values (e.g., 123.4510)
    unit_of_measurement   VARCHAR(15)
);

-- envi_diesel_consumption
DROP TABLE IF EXISTS bronze.envi_diesel_consumption;
CREATE TABLE bronze.envi_diesel_consumption (
    dc_id                VARCHAR(20),          -- Example: DC-PSC-2024-0010
    company_id           VARCHAR(10),          -- Referenced to company info.
    cp_id                VARCHAR(20),          -- Referenced to envi_company_property.
    unit_of_measurement  VARCHAR(15),
    consumption          DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    date                 DATE
);

-- envi_electric_consumption
DROP TABLE IF EXISTS bronze.envi_electric_consumption;
CREATE TABLE bronze.envi_electric_consumption (
    ec_id					VARCHAR(20),           -- Example: EC-PSC-2023-001
    company_id			 	VARCHAR(10),      -- Referenced to company_info.
    unit_of_measurement	 	VARCHAR(15),
    consumption 		 	DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    quarter           	 	VARCHAR(2),
    year               	 	SMALLINT
);

-- envi_non_hazard_waste
DROP TABLE IF EXISTS bronze.envi_non_hazard_waste;
CREATE TABLE bronze.envi_non_hazard_waste (
    nhw_id 					VARCHAR(20),           -- Example: NHW-PSC-2024-001
    company_id 				VARCHAR(10),       -- Referenced to company_info.
    waste_source 			VARCHAR(20),     -- Example: Staff House, Security, Utility
    metrics                 VARCHAR(20),
    unit_of_measurement 	VARCHAR(15),
    waste 					DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    month 					VARCHAR(10),
    year 					SMALLINT
);

-- envi_hazard_waste
DROP TABLE IF EXISTS bronze.envi_hazard_waste_generated;
CREATE TABLE bronze.envi_hazard_waste_generated (
    hwg_id 					VARCHAR(20),          		-- Example: HW-PSC-2023-001
    company_id 				VARCHAR(10),      		-- Referenced to company_info.
    metrics                 VARCHAR(20),
    unit_of_measurement 	VARCHAR(15),
    waste_generated 		DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    quarter 				VARCHAR(2),  		 		-- Example: 'Q1', 'Q2', 'Q3', 'Q4'
    year 					SMALLINT
);

-- envi_hazard_waste
DROP TABLE IF EXISTS bronze.envi_hazard_waste_disposed;
CREATE TABLE bronze.envi_hazard_waste_disposed (
    hwd_id 					VARCHAR(20),          		-- Example: HW-PSC-2023-001
    company_id 				VARCHAR(10),      		-- Referenced to company_info.
    metrics                 VARCHAR(20),
    unit_of_measurement 	VARCHAR(15),
    waste_disposed 			DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    year 				   	SMALLINT
);

-- Adding constraints (UNIQUE)
ALTER TABLE bronze.envi_company_property ADD CONSTRAINT unique_cp_id UNIQUE (cp_id);
ALTER TABLE bronze.envi_natural_sources ADD CONSTRAINT unique_ns_id UNIQUE (ns_id);
ALTER TABLE bronze.envi_water_withdrawal ADD CONSTRAINT unique_ww_id UNIQUE (ww_id);
ALTER TABLE bronze.envi_diesel_consumption ADD CONSTRAINT unique_dc_id UNIQUE (dc_id);
ALTER TABLE bronze.envi_electric_consumption ADD CONSTRAINT unique_ec_id UNIQUE (ec_id);
ALTER TABLE bronze.envi_non_hazard_waste ADD CONSTRAINT unique_nhw_id UNIQUE (nhw_id);
ALTER TABLE bronze.envi_hazard_waste_generated ADD CONSTRAINT unique_hwg_id UNIQUE (hwg_id);
ALTER TABLE bronze.envi_hazard_waste_disposed ADD CONSTRAINT unique_hwd_id UNIQUE (hwd_id);