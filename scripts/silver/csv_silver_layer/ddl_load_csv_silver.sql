/*
===============================================================================
DDL Script: Create Silver Tables for Power Plant Data Warehouse
===============================================================================
*/

-- ======================================
-- Table: silver.csv_emission_factors
-- ======================================
DROP TABLE IF EXISTS silver.csv_emission_factors CASCADE;
CREATE TABLE silver.csv_emission_factors (
    ef_id VARCHAR(20) PRIMARY KEY,           				-- PK: Unique emission factor ID
    generation_source TEXT,                    			-- Type of generation source
    kg_co2_per_kwh DECIMAL(10,5),               			-- CO2 emission per kWh
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ======================================
-- Table: silver.csv_power_plants
-- ======================================
DROP TABLE IF EXISTS silver.csv_power_plants CASCADE;
CREATE TABLE silver.csv_power_plants (
    power_plant_id VARCHAR(10) PRIMARY KEY,     			-- PK: Unique power plant ID
    company_id VARCHAR(10),                     			-- FK: References csv_company
    site_name VARCHAR(50),
    site_address TEXT,
    city_town VARCHAR(30),
    province VARCHAR(30),
    country VARCHAR(30),
    zip VARCHAR(4),
    ef_id VARCHAR(20) NOT NULL,                        	-- FK: References csv_emission_factors
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Foreign Keys
    CONSTRAINT fk_power_plant_company FOREIGN KEY (company_id)
        REFERENCES ref.company_main (company_id) ON DELETE CASCADE,
    CONSTRAINT fk_power_plant_emission FOREIGN KEY (ef_id)
        REFERENCES silver.csv_emission_factors (ef_id) ON DELETE SET NULL
);

-- ======================================
-- Table: silver.csv_energy_records
-- ======================================
DROP TABLE IF EXISTS silver.csv_energy_records CASCADE;
CREATE TABLE silver.csv_energy_records (
    energy_id VARCHAR(20) PRIMARY KEY,          			-- PK: Unique energy record ID
    power_plant_id VARCHAR(20) NOT NULL,                 	-- FK: References csv_power_plants
    date_generated TIMESTAMP NOT NULL,
    energy_generated_kwh NUMERIC(15, 4) NOT NULL DEFAULT 0,
    co2_avoidance_kg NUMERIC(15, 4) NOT NULL DEFAULT 0,
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Foreign Key
    CONSTRAINT fk_energy_power_plant FOREIGN KEY (power_plant_id)
        REFERENCES silver.csv_power_plants (power_plant_id) ON DELETE CASCADE
);

-- ======================================
-- Table: silver.csv_fa_factors
-- ======================================
DROP TABLE IF EXISTS silver.csv_fa_factors CASCADE;
CREATE TABLE silver.csv_fa_factors (
    ff_id VARCHAR(20) PRIMARY KEY,
    ff_name TEXT,
    ff_percentage DECIMAL(5,4),
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ======================================
-- Table: silver.csv_hec_factors
-- ======================================
DROP TABLE IF EXISTS silver.csv_hec_factors CASCADE;
CREATE TABLE silver.csv_hec_factors (
    hec_id VARCHAR(20) PRIMARY KEY,
    hec_value DECIMAL(10,4) NOT NULL DEFAULT 0,
    hec_year SMALLINT NOT NULL,
    source_name TEXT,
    source_link TEXT,
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
