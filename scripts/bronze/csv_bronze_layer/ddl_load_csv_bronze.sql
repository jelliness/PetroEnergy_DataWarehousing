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
-- Table: csv_company
-- ============================================================================
DROP TABLE IF EXISTS bronze.csv_company;
CREATE TABLE bronze.csv_company (
    company_id TEXT PRIMARY KEY,                    -- Unique identifier for the company
    company_name TEXT,                              -- Name of the power generation company
    resources TEXT,                                 -- Comma-separated list or description of resources used (e.g., solar, wind)
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Timestamp when the record was first created
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Timestamp of the most recent update
);

-- ============================================================================
-- Table: csv_emission_factors
-- ============================================================================
DROP TABLE IF EXISTS bronze.csv_emission_factors;
CREATE TABLE bronze.csv_emission_factors (
    generation_source TEXT PRIMARY KEY,             -- Type of energy source (e.g., coal, hydro, solar)
    kg_co2_per_kwh TEXT,                            -- Emissions factor in kg CO2 per kWh
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Record update timestamp
);

-- ============================================================================
-- Table: csv_power_plants
-- ============================================================================
DROP TABLE IF EXISTS bronze.csv_power_plants;
CREATE TABLE bronze.csv_power_plants (
    power_plant_id TEXT PRIMARY KEY,                -- Unique identifier for the power plant
    company_id TEXT,                                -- Reference to the owning company
    site_name TEXT,                                 -- Name of the power plant site
    site_address TEXT,                              -- Street address of the plant site
    city_town TEXT,                                 -- City or town where the plant is located
    province TEXT,                                  -- Province or state
    country TEXT,                                   -- Country
    zip TEXT,                                       -- Postal or ZIP code
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Record update timestamp
);

-- ============================================================================
-- Table: csv_energy_records
-- ============================================================================
DROP TABLE IF EXISTS bronze.csv_energy_records;
CREATE TABLE bronze.csv_energy_records (
    power_plant_id TEXT,                            -- Reference to the power plant
    datetime TEXT,                                  -- Date and time of energy measurement
    energy_generated TEXT,                          -- Amount of energy generated
    unit_of_measurement TEXT,                       -- Unit of measurement (e.g., kWh, MWh)
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Record update timestamp
    PRIMARY KEY (power_plant_id, datetime)          -- Composite key per plant and timestamp
);

-- ============================================================================
-- Table: csv_fa_factors
-- ============================================================================
DROP TABLE IF EXISTS bronze.csv_fa_factors;
CREATE TABLE bronze.csv_fa_factors (
    ff_id TEXT PRIMARY KEY,                         -- Unique identifier
    ff_name TEXT,                                   -- Name of the factor
    ff_percentage TEXT,                             -- Percentage value
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Record update timestamp
);

-- ============================================================================
-- Table: csv_hec_factors
-- ============================================================================
DROP TABLE IF EXISTS bronze.csv_hec_factors;
CREATE TABLE bronze.csv_hec_factors (
    hec_id TEXT PRIMARY KEY,                        -- Unique identifier
    hec_value TEXT,                                 -- Value of HEC
    hec_year TEXT,                                  -- Year of the HEC value
    source_name TEXT,                               -- Source of the data
    link TEXT,                                      -- Reference link
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Record update timestamp
);
