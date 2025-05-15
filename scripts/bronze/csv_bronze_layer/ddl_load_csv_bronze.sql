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
    dwh_date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Timestamp when the record was first created in the data warehouse
    dwh_date_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- Timestamp of the most recent update to the record
);

-- ============================================================================
-- Table: csv_emission_factors
-- ============================================================================
DROP TABLE IF EXISTS bronze.csv_emission_factors;
CREATE TABLE bronze.csv_emission_factors (
    generation_source TEXT PRIMARY KEY,             -- Type of energy source (e.g., coal, hydro, solar)
    kg_co2_per_kwh TEXT,                            -- Emissions factor: kilograms of CO2 emitted per kilowatt-hour of energy produced
    dwh_date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    dwh_date_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- Record update timestamp
);

-- ============================================================================
-- Table: csv_power_plants
-- ============================================================================
DROP TABLE IF EXISTS bronze.csv_power_plants;
CREATE TABLE bronze.csv_power_plants (
    power_plant_id TEXT PRIMARY KEY,                -- Unique identifier for the power plant
    company_id TEXT,                                -- Foreign key referencing the company that owns or operates the plant
    site_name TEXT,                                 -- Name of the power plant site
    site_address TEXT,                              -- Street address of the plant site
    city_town TEXT,                                 -- City or town where the plant is located
    province TEXT,                                  -- Province or state of the plant location
    country TEXT,                                   -- Country where the power plant is located
    zip TEXT,                                       -- Postal or ZIP code for the plant site
    dwh_date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    dwh_date_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- Record update timestamp
);

-- ============================================================================
-- Table: csv_energy_records
-- ============================================================================
DROP TABLE IF EXISTS bronze.csv_energy_records;
CREATE TABLE bronze.csv_energy_records (
    power_plant_id TEXT,                            -- Foreign key referencing the power plant generating the energy
    datetime TEXT,                                  -- Date and time when the energy measurement was recorded
    energy_generated TEXT,                          -- Amount of energy generated (could be in kWh, MWh, etc.)
    unit_of_measurement TEXT,                       -- Unit in which energy is measured (e.g., kWh, MWh)
    dwh_date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    dwh_date_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record update timestamp
    PRIMARY KEY (power_plant_id, datetime)          -- Composite key for uniquely identifying an energy record per plant and timestamp
);
