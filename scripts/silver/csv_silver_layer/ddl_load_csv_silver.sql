/*
===============================================================================
DDL Script: Create Silver Tables for Power Plant Data Warehouse
===============================================================================
Script Purpose:
    This script creates refined "silver" layer tables for storing transformed
    and cleaned data from the bronze layer. These tables include meaningful
    data types, primary keys for data integrity, and fields prepared for
    analytical processing (e.g., CO2 avoidance calculations).

    Key Improvements Over Bronze Layer:
        - Structured types (e.g., VARCHAR, DECIMAL)
        - Additional derived or reference columns (e.g., ef_id, co2_avoidance)
        - Enforced primary keys
===============================================================================
*/

-- ======================================
-- Table: silver.csv_company
-- ======================================
DROP TABLE IF EXISTS silver.csv_company;
CREATE TABLE silver.csv_company (
    company_id VARCHAR(20) PRIMARY KEY,         -- Unique identifier for each company
    company_name VARCHAR(50),                   -- Name of the company
    resources VARCHAR(20),                      -- Type of energy resources used (e.g., wind, solar)
    dwh_date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    dwh_date_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- Record last update timestamp
);

-- ======================================
-- Table: silver.csv_emission_factors
-- ======================================
DROP TABLE IF EXISTS silver.csv_emission_factors;
CREATE TABLE silver.csv_emission_factors (
    ef_id VARCHAR(20) PRIMARY KEY,              -- Unique identifier for each emission factor record
    generation_source TEXT,                     -- Type of generation source (e.g., coal, hydro)
    kg_co2_per_kwh DECIMAL(10,5),               -- Emission factor in kg of CO2 per kWh generated
    dwh_date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    dwh_date_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- Record last update timestamp
);

-- ======================================
-- Table: silver.csv_power_plants
-- ======================================
DROP TABLE IF EXISTS silver.csv_power_plants;
CREATE TABLE silver.csv_power_plants (
    power_plant_id VARCHAR(20) PRIMARY KEY,     -- Unique identifier for the power plant
    company_id VARCHAR(20),                     -- Foreign key reference to silver.csv_company
    site_name VARCHAR(50),                      -- Name of the plant site
    site_address TEXT,                          -- Street address of the plant
    city_town VARCHAR(30),                      -- City or town where the plant is located
    province VARCHAR(30),                       -- Province or state
    country VARCHAR(30),                        -- Country
    zip VARCHAR(4),                             -- Postal or ZIP code
    ef_id VARCHAR(20),                          -- Foreign key reference to emission factor used
    dwh_date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    dwh_date_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- Record last update timestamp
);

-- ======================================
-- Table: silver.csv_energy_records
-- ======================================
DROP TABLE IF EXISTS silver.csv_energy_records;
CREATE TABLE silver.csv_energy_records (
    energy_id VARCHAR(20) PRIMARY KEY,           -- Unique ID for each energy record
    power_plant_id VARCHAR(20),                  -- Foreign key to power plant
    date_generated TIMESTAMP,                    -- Timestamp of energy measurement
    energy_generated_kwh VARCHAR(50),            -- Energy generated
    co2_avoidance_kg NUMERIC(15, 4),             -- CO2 avoided due to clean energy (kg)
    dwh_date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    dwh_date_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- Record last update timestamp
);
