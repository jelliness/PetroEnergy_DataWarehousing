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

-- envi_company_property
DROP TABLE IF EXISTS silver.envi_company_property;
CREATE TABLE silver.envi_company_property (
    cp_id      VARCHAR(20) NOT NULL,       -- Example: CP-PSC-001
    company_id VARCHAR(10) NOT NULL,       -- Referenced to company_info.
    cp_name    VARCHAR(30),
    cp_type    VARCHAR(15),       -- Example values: Equipment, Vehicle
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_cp_pk PRIMARY KEY (cp_id)
	--FOREIGN KEY (company_id) REFERENCES company_info(company_id)
);

-- envi_natural_sources
DROP TABLE IF EXISTS silver.envi_natural_sources;
CREATE TABLE silver.envi_natural_sources (
    ns_id      VARCHAR(20) NOT NULL,       -- Example: NS-PSC-001
    company_id VARCHAR(10) NOT NULL,       -- Referenced to company_info.
    ns_name    VARCHAR(30),
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_ns_pk PRIMARY KEY (ns_id)
	--FOREIGN KEY (company_id) REFERENCES company_info(company_id)
);

-- envi_water_withdrawal
DROP TABLE IF EXISTS silver.envi_water_withdrawal;
CREATE TABLE silver.envi_water_withdrawal (
    ww_id                 	VARCHAR(20) NOT NULL,         -- Example: WW-PSC-2022-002
    company_id            	VARCHAR(10) NOT NULL,         -- Referenced to company_info.
    ns_id                 	VARCHAR(20),         -- Referenced to natural sources.
    volume                	DOUBLE PRECISION,    -- Allows decimal values (e.g., 123.456)
    unit_of_measurement   	VARCHAR(15),
    month                 	VARCHAR(10),
    quarter					VARCHAR(2),
    year					SMALLINT,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_ww_pk PRIMARY KEY (ww_id)
	--FOREIGN KEY (company_id) REFERENCES company_info(company_id)
);

-- envi_diesel_consumption
DROP TABLE IF EXISTS silver.envi_diesel_consumption;
CREATE TABLE silver.envi_diesel_consumption (
    dc_id                    VARCHAR(20) NOT NULL,            -- Example: EC-PSC-2023-001
    company_id               VARCHAR(10) NOT NULL,            -- Referenced to company_info.
    cp_id                    VARCHAR(20),            -- Referenced to company_property.
    unit_of_measurement      VARCHAR(15),
    consumption              DOUBLE PRECISION,        -- Allows decimal values (e.g., 234.789)
    month                    VARCHAR(10),
    year                     SMALLINT,
    quarter                  VARCHAR(2),
	date					 date,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_dc_pk PRIMARY KEY (dc_id)
	--FOREIGN KEY (company_id) REFERENCES company_info(company_id)
);

-- envi_electric_consumption
DROP TABLE IF EXISTS silver.envi_electric_consumption;
CREATE TABLE silver.envi_electric_consumption (
    ec_id                   VARCHAR(20) NOT NULL,            -- Example: EC-PSC-2023-001
    company_id              VARCHAR(10) NOT NULL,            -- Referenced to company_info.
    unit_of_measurement     VARCHAR(15),
    consumption             DOUBLE PRECISION,        -- Allows decimal values (e.g., 234.789)
    quarter                 VARCHAR(2),
    year                    SMALLINT,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_ec_pk PRIMARY KEY (ec_id)
	--FOREIGN KEY (company_id) REFERENCES company_info(company_id)
); 

-- envi_non_hazard_waste
DROP TABLE IF EXISTS silver.envi_non_hazard_waste;
CREATE TABLE silver.envi_non_hazard_waste (
    nhw_id                  VARCHAR(20) NOT NULL,           -- Example: NHW-PSC-2024-001
    company_id              VARCHAR(10) NOT NULL,       -- Referenced to company_info.
    waste_source            VARCHAR(20),     -- Example: Staff House, Security, Utility
    metrics                 VARCHAR(20),
    unit_of_measurement     VARCHAR(15),
    waste                   DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    month                   VARCHAR(10),
    year                    SMALLINT,
    quarter                 VARCHAR(2),
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_nhw_pk PRIMARY KEY (nhw_id)
	--FOREIGN KEY (company_id) REFERENCES company_info(company_id)
);


-- envi_hazard_waste_generated
DROP TABLE IF EXISTS silver.envi_hazard_waste_generated;
CREATE TABLE silver.envi_hazard_waste_generated (
    hwg_id                  VARCHAR(20) NOT NULL,             -- Example: HW-PSC-2023-001
    company_id              VARCHAR(10) NOT NULL,              -- Referenced to company_info.
    metrics                 VARCHAR(20),
    unit_of_measurement     VARCHAR(15),
    waste_generated         DOUBLE PRECISION,                 -- Allows decimal values (e.g., 234.789)
    quarter                 VARCHAR(2),               -- Example: 'Q1', 'Q2', 'Q3', 'Q4'
    year                    SMALLINT,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_hwg_pk PRIMARY KEY (hwg_id)
	--FOREIGN KEY (company_id) REFERENCES company_info(company_id)
);

-- envi_hazard_waste_disposed
DROP TABLE IF EXISTS silver.envi_hazard_waste_disposed;
CREATE TABLE silver.envi_hazard_waste_disposed (
    hwd_id                  VARCHAR(20) NOT NULL,            -- Example: HW-PSC-2023-001
    company_id              VARCHAR(10) NOT NULL,              -- Referenced to company_info.
    metrics                 VARCHAR(20),
    unit_of_measurement     VARCHAR(15),
    waste_disposed          DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    year                    SMALLINT,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_hwd_pk PRIMARY KEY (hwd_id)
	--FOREIGN KEY (company_id) REFERENCES company_info(company_id)
);

-- Adding constraints (UNIQUE)
ALTER TABLE silver.envi_company_property ADD CONSTRAINT unique_cp_id UNIQUE (cp_id);
ALTER TABLE silver.envi_natural_sources ADD CONSTRAINT unique_ns_id UNIQUE (ns_id);
ALTER TABLE silver.envi_water_withdrawal ADD CONSTRAINT unique_ww_id UNIQUE (ww_id);
ALTER TABLE silver.envi_diesel_consumption ADD CONSTRAINT unique_dc_id UNIQUE (dc_id);
ALTER TABLE silver.envi_electric_consumption ADD CONSTRAINT unique_ec_id UNIQUE (ec_id);
ALTER TABLE silver.envi_non_hazard_waste ADD CONSTRAINT unique_nhw_id UNIQUE (nhw_id);
ALTER TABLE silver.envi_hazard_waste_generated ADD CONSTRAINT unique_hwg_id UNIQUE (hwg_id);
ALTER TABLE silver.envi_hazard_waste_disposed ADD CONSTRAINT unique_hwd_id UNIQUE (hwd_id);