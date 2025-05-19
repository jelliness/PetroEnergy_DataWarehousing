/*
===============================================================================
DDL Script: Create Bronze Tables for Power Plants with UPSERT Support
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema with PRIMARY KEYs to
    support UPSERT operations using INSERT ... ON CONFLICT DO UPDATE.
===============================================================================
*/

-- ============================================================================
-- Table: csv_emission_factors
-- ============================================================================
DROP TABLE IF EXISTS bronze.csv_emission_factors;
CREATE TABLE bronze.csv_emission_factors (
    ef_id VARCHAR(6) PRIMARY KEY,                   -- Unique identifier for emission factors
    generation_source TEXT,                         -- Type of energy source (e.g., coal, hydro, solar)
    kg_co2_per_kwh NUMERIC,                            -- Emissions factor in kg CO2 per kWh
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Record update timestamp
);

-- ============================================================================
-- Table: csv_power_plants
-- ============================================================================
DROP TABLE IF EXISTS bronze.csv_power_plants;
CREATE TABLE bronze.csv_power_plants (
    power_plant_id VARCHAR(10) PRIMARY KEY,         -- Unique identifier for the power plant
    company_id VARCHAR(10),                         -- Reference to the owning company
    site_name TEXT,                                 -- Name of the power plant site
    site_address TEXT,                              -- Street address of the plant site
    city_town TEXT,                                 -- City or town where the plant is located
    province TEXT,                                  -- Province or state
    country TEXT,                                   -- Country
    zip VARCHAR(4),                                 -- Postal or ZIP code
    ef_id VARCHAR(6),                               -- 
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Record update timestamp
);

-- ============================================================================
-- Table: csv_energy_records
-- ============================================================================
DROP TABLE IF EXISTS bronze.csv_energy_records;
CREATE TABLE bronze.csv_energy_records (
    power_plant_id VARCHAR(10),                     -- Reference to the power plant
    datetime TEXT,                                  -- Date and time of energy measurement
    energy_generated NUMERIC,                       -- Amount of energy generated
    unit_of_measurement VARCHAR(10),                -- Unit of measurement (e.g., kWh, MWh)
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Record update timestamp
    PRIMARY KEY (power_plant_id, datetime)          -- Composite key per plant and timestamp
);

-- ============================================================================
-- Table: csv_fa_factors
-- ============================================================================
DROP TABLE IF EXISTS bronze.csv_fa_factors;
CREATE TABLE bronze.csv_fa_factors (
    ff_id VARCHAR(10) PRIMARY KEY,                  -- Unique identifier
    ff_name TEXT,                                   -- Name of the factor
    ff_percentage NUMERIC,                          -- Percentage value
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Record update timestamp
);

-- ============================================================================
-- Table: csv_hec_factors
-- ============================================================================
DROP TABLE IF EXISTS bronze.csv_hec_factors;
CREATE TABLE bronze.csv_hec_factors (
    hec_id VARCHAR(6) PRIMARY KEY,                  -- Unique identifier
    hec_value INT,                                  -- Value of HEC
    hec_year INT,                                   -- Year of the HEC value
    source_name TEXT,                               -- Source of the data
    source_link TEXT,                               -- Reference link
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Record update timestamp
);