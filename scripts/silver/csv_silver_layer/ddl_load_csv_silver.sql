/*
===============================================================================
DDL Script: Create Silver Tables for Power Plant Data Warehouse
===============================================================================
*/

-- ======================================
-- Table: silver.csv_energy_records
-- ======================================
DROP TABLE IF EXISTS silver.csv_energy_records CASCADE;
CREATE TABLE silver.csv_energy_records (
    energy_id VARCHAR(20) PRIMARY KEY,          			-- PK: Unique energy record ID
    power_plant_id VARCHAR(10) NOT NULL,                 	-- FK: References csv_power_plants
    date_generated TIMESTAMP NOT NULL,
    energy_generated_kwh NUMERIC(15, 4) NOT NULL DEFAULT 0,
    co2_avoidance_kg NUMERIC(15, 4) NOT NULL DEFAULT 0,
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Foreign Key
    CONSTRAINT fk_energy_power_plant FOREIGN KEY (power_plant_id)
        REFERENCES ref.ref_power_plants (power_plant_id) ON DELETE CASCADE
);

-- ======================================
-- Table: silver.csv_funds_allocation
-- ======================================
DROP TABLE IF EXISTS silver.csv_funds_allocation CASCADE;
CREATE TABLE silver.csv_funds_allocation (
    month_generated VARCHAR(20),
    power_plant_id VARCHAR(10),
	ff_id VARCHAR(10), 
    power_generated_peso NUMERIC (10,4) NOT NULL DEFAULT 0,
    funds_allocated_peso NUMERIC (10,4) NOT NULL DEFAULT 0,
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE silver.csv_funds_allocation
ADD CONSTRAINT unique_fund_allocation_key
UNIQUE (month_generated, power_plant_id, ff_id);
