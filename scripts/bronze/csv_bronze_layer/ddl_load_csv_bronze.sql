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
-- Table: csv_energy_records
-- ============================================================================
DROP TABLE IF EXISTS bronze.csv_energy_records;
CREATE TABLE bronze.csv_energy_records (
    energy_id VARCHAR(20) PRIMARY KEY,
    power_plant_id VARCHAR(10),                     -- Reference to the power plant
    datetime TIMESTAMP,                                  -- Date and time of energy measurement
    energy_generated NUMERIC,                       -- Amount of energy generated
    unit_of_measurement VARCHAR(10),                -- Unit of measurement (e.g., kWh, MWh)
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Record update timestamp    
);
