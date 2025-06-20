/*
===============================================================================
DDL Script: Create ref Tables
===============================================================================
*/

CREATE SCHEMA ref;
DROP TABLE IF EXISTS ref.company_main CASCADE;

CREATE TABLE ref.company_main (
    company_id VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    parent_company_id VARCHAR(10) REFERENCES ref.company_main(company_id) ON DELETE SET NULL,
    address TEXT
);

DROP TABLE IF EXISTS ref.expenditure_type CASCADE;
CREATE TABLE ref.expenditure_type (
    type_id VARCHAR(4) PRIMARY KEY,
    type_description TEXT
);



-- ======================================
-- Table: ref.ref_emission_factors
-- ======================================
DROP TABLE IF EXISTS ref.ref_emission_factors CASCADE;
CREATE TABLE ref.ref_emission_factors (
    ef_id VARCHAR(10) PRIMARY KEY,           				-- PK: Unique emission factor ID
    generation_source TEXT,                    			    -- Type of generation source
    kg_co2_per_kwh DECIMAL(10,5),               			-- CO2 emission per kWh
    co2_emitted_kg DECIMAL(10,5),
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ======================================
-- Table: ref.ref_power_plants
-- ======================================
DROP TABLE IF EXISTS ref.ref_power_plants CASCADE;
CREATE TABLE ref.ref_power_plants (
    power_plant_id VARCHAR(10) PRIMARY KEY,     			-- PK: Unique power plant ID
    company_id VARCHAR(10),                     			-- FK: References ref_company
    site_name VARCHAR(50),
    site_address TEXT,
    city_town VARCHAR(30),
    province VARCHAR(30),
    country VARCHAR(30),
    zip VARCHAR(4),
    ef_id VARCHAR(10) NOT NULL,                        	    -- FK: References ref_emission_factors
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Foreign Keys
    CONSTRAINT fk_power_plant_company FOREIGN KEY (company_id)
        REFERENCES ref.company_main (company_id) ON DELETE CASCADE,
    CONSTRAINT fk_power_plant_emission FOREIGN KEY (ef_id)
        REFERENCES ref.ref_emission_factors (ef_id) ON DELETE SET NULL
);

-- ======================================
-- Table: ref.ref_fa_factors
-- ======================================
DROP TABLE IF EXISTS ref.ref_fa_factors CASCADE;
CREATE TABLE ref.ref_fa_factors (
    ff_id VARCHAR(10) PRIMARY KEY,
    ff_name TEXT,
    ff_percentage DECIMAL(5,4),
    ff_category VARCHAR(20),
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ======================================
-- Table: ref.ref_hec_factors
-- ======================================
DROP TABLE IF EXISTS ref.ref_hec_factors CASCADE;
CREATE TABLE ref.ref_hec_factors (
    hec_id VARCHAR(10) PRIMARY KEY,
    hec_value DECIMAL(10,4) NOT NULL DEFAULT 0,
    hec_year SMALLINT NOT NULL,
    source_name TEXT,
    source_link TEXT,
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ======================================
-- Table: ref.ref_co2_equivalence
-- ======================================
DROP TABLE IF EXISTS ref.ref_co2_equivalence CASCADE;
CREATE TABLE ref.ref_co2_equivalence (
    equivalence_category TEXT,
    equivalence_label TEXT,
    metric VARCHAR(100),
    equivalent_value_co2_emissions DECIMAL(20, 10),
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS ref.hr_position CASCADE;
CREATE TABLE ref.hr_position (
	position_id VARCHAR(2) PRIMARY KEY,
	position_name VARCHAR(20)
);

