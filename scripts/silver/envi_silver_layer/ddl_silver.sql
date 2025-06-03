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
    company_id VARCHAR(10) NOT NULL,       -- Referenced to company_main.
    cp_name    VARCHAR(30),
    cp_type    VARCHAR(15),       -- Example values: Equipment, Vehicle
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_cp_pk PRIMARY KEY (cp_id),
	FOREIGN KEY (company_id) REFERENCES ref.company_main(company_id)
);

-- envi_water_abstraction
DROP TABLE IF EXISTS silver.envi_water_abstraction;
CREATE TABLE silver.envi_water_abstraction (
    wa_id                 	VARCHAR(20) NOT NULL,         -- Example: WW-PSC-2022-002
    company_id            	VARCHAR(10) NOT NULL,         -- Referenced to company_main.
    volume                	DOUBLE PRECISION,    -- Allows decimal values (e.g., 123.456)
    unit_of_measurement   	VARCHAR(15),
    quarter					VARCHAR(2),
    year					SMALLINT,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_wa_pk PRIMARY KEY (wa_id),
	FOREIGN KEY (company_id) REFERENCES ref.company_main(company_id)
);

-- envi_water_discharge
DROP TABLE IF EXISTS silver.envi_water_discharge;
CREATE TABLE silver.envi_water_discharge (
    wd_id                 	VARCHAR(20) NOT NULL,         -- Example: WW-PSC-2022-002
    company_id            	VARCHAR(10) NOT NULL,         -- Referenced to company_main.
    volume                	DOUBLE PRECISION,    -- Allows decimal values (e.g., 123.456)
    unit_of_measurement   	VARCHAR(15),
    quarter					VARCHAR(2),
    year					SMALLINT,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_wd_pk PRIMARY KEY (wd_id),
    FOREIGN KEY (company_id) REFERENCES ref.company_main(company_id)
);

-- envi_water_consumption
DROP TABLE IF EXISTS silver.envi_water_consumption;
CREATE TABLE silver.envi_water_consumption (
    wc_id                  VARCHAR(20) NOT NULL,         -- Example: WW-PSC-2022-002
    company_id             VARCHAR(10) NOT NULL,         -- Referenced to company_main.
    volume                 DOUBLE PRECISION,    -- Allows decimal values (e.g., 123.456)
    unit_of_measurement    VARCHAR(15),
    quarter                VARCHAR(2),
    year                   SMALLINT,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_wc_pk PRIMARY KEY (wc_id),
    FOREIGN KEY (company_id) REFERENCES ref.company_main(company_id)
);

-- envi_diesel_consumption
DROP TABLE IF EXISTS silver.envi_diesel_consumption;
CREATE TABLE silver.envi_diesel_consumption (
    dc_id                    VARCHAR(20) NOT NULL,            -- Example: EC-PSC-2023-001
    company_id               VARCHAR(10) NOT NULL,            -- Referenced to company_main.
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
    CONSTRAINT silver_dc_pk PRIMARY KEY (dc_id),
	FOREIGN KEY (company_id) REFERENCES ref.company_main(company_id),
    FOREIGN KEY (cp_id) REFERENCES silver.envi_company_property(cp_id)
);

-- envi_electric_consumption
DROP TABLE IF EXISTS silver.envi_electric_consumption;
CREATE TABLE silver.envi_electric_consumption (
    ec_id                   VARCHAR(20) NOT NULL,            -- Example: EC-PSC-2023-001
    company_id              VARCHAR(10) NOT NULL,            -- Referenced to company_main.
    source                  VARCHAR(20),            -- Example: Logistics Station, Control Building
    unit_of_measurement     VARCHAR(15),
    consumption             DOUBLE PRECISION,        -- Allows decimal values (e.g., 234.789)
    quarter                 VARCHAR(2),
    year                    SMALLINT,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_ec_pk PRIMARY KEY (ec_id),
	FOREIGN KEY (company_id) REFERENCES ref.company_main(company_id)
); 

-- envi_non_hazard_waste
DROP TABLE IF EXISTS silver.envi_non_hazard_waste;
CREATE TABLE silver.envi_non_hazard_waste (
    nhw_id                  VARCHAR(20) NOT NULL,           -- Example: NHW-PSC-2024-001
    company_id              VARCHAR(10) NOT NULL,       -- Referenced to company_main.
    metrics                 VARCHAR(50),
    unit_of_measurement     VARCHAR(15),
    waste                   DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    year                    SMALLINT,
    quarter                 VARCHAR(2),
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_nhw_pk PRIMARY KEY (nhw_id),
	FOREIGN KEY (company_id) REFERENCES ref.company_main(company_id)
);


-- envi_hazard_waste_generated
DROP TABLE IF EXISTS silver.envi_hazard_waste_generated;
CREATE TABLE silver.envi_hazard_waste_generated (
    hwg_id                  VARCHAR(20) NOT NULL,             -- Example: HW-PSC-2023-001
    company_id              VARCHAR(10) NOT NULL,              -- Referenced to company_main.
    metrics                 VARCHAR(50),
    unit_of_measurement     VARCHAR(15),
    waste_generated         DOUBLE PRECISION,                 -- Allows decimal values (e.g., 234.789)
    quarter                 VARCHAR(2),               -- Example: 'Q1', 'Q2', 'Q3', 'Q4'
    year                    SMALLINT,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_hwg_pk PRIMARY KEY (hwg_id),
	FOREIGN KEY (company_id) REFERENCES ref.company_main(company_id)
);

-- envi_hazard_waste_disposed
DROP TABLE IF EXISTS silver.envi_hazard_waste_disposed;
CREATE TABLE silver.envi_hazard_waste_disposed (
    hwd_id                  VARCHAR(20) NOT NULL,            -- Example: HW-PSC-2023-001
    company_id              VARCHAR(10) NOT NULL,              -- Referenced to company_main.
    metrics                 VARCHAR(50),
    unit_of_measurement     VARCHAR(15),
    waste_disposed          DOUBLE PRECISION,     -- Allows decimal values (e.g., 234.789)
    year                    SMALLINT,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_hwd_pk PRIMARY KEY (hwd_id),
	FOREIGN KEY (company_id) REFERENCES ref.company_main(company_id)
);

CREATE TABLE silver.wa_id_mapping (
    wa_id_bronze VARCHAR(20) NOT NULL,
    wa_id_silver VARCHAR(20) NOT NULL,
    mapped_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT wa_id_mapping_pk PRIMARY KEY (wa_id_bronze, wa_id_silver),
    CONSTRAINT fk_wa_id_bronze FOREIGN KEY (wa_id_bronze)
        REFERENCES bronze.envi_water_abstraction(wa_id),
    CONSTRAINT fk_wa_id_silver FOREIGN KEY (wa_id_silver)
        REFERENCES silver.envi_water_abstraction(wa_id)
);


-- Adding constraints (UNIQUE)
ALTER TABLE silver.envi_company_property ADD CONSTRAINT unique_cp_id UNIQUE (cp_id);
ALTER TABLE silver.envi_water_abstraction ADD CONSTRAINT unique_wa_id UNIQUE (wa_id);
ALTER TABLE silver.envi_water_discharge ADD CONSTRAINT unique_wd_id UNIQUE (wd_id);
ALTER TABLE silver.envi_water_consumption ADD CONSTRAINT unique_wc_id UNIQUE (wc_id);
ALTER TABLE silver.envi_diesel_consumption ADD CONSTRAINT unique_dc_id UNIQUE (dc_id);
ALTER TABLE silver.envi_electric_consumption ADD CONSTRAINT unique_ec_id UNIQUE (ec_id);
ALTER TABLE silver.envi_non_hazard_waste ADD CONSTRAINT unique_nhw_id UNIQUE (nhw_id);
ALTER TABLE silver.envi_hazard_waste_generated ADD CONSTRAINT unique_hwg_id UNIQUE (hwg_id);
ALTER TABLE silver.envi_hazard_waste_disposed ADD CONSTRAINT unique_hwd_id UNIQUE (hwd_id);