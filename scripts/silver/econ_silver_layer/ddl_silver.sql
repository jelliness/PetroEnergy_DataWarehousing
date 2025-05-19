/*
===============================================================================
DDL Script: Create Silver Tables for Economic Data
===============================================================================
Script Purpose:
    This script creates silver-layer tables for economic reporting,
    including derived columns and metadata tracking.
===============================================================================
*/

-- First drop dependent gold views
DROP VIEW IF EXISTS gold.vw_economic_value_summary;
DROP VIEW IF EXISTS gold.vw_economic_value_distributed;
DROP VIEW IF EXISTS gold.vw_economic_value_generated;
DROP VIEW IF EXISTS gold.vw_economic_value_distributed_by_company;
DROP VIEW IF EXISTS gold.vw_economic_expenditure_by_company;

-- Drop and recreate table for economic value
DROP TABLE IF EXISTS silver.econ_value;
CREATE TABLE silver.econ_value (
    year SMALLINT,
    electricity_sales NUMERIC NOT NULL DEFAULT 0,
    oil_revenues NUMERIC NOT NULL DEFAULT 0,
    other_revenues NUMERIC NOT NULL DEFAULT 0,
    interest_income NUMERIC NOT NULL DEFAULT 0,
    share_in_net_income_of_associate NUMERIC NOT NULL DEFAULT 0,
    miscellaneous_income NUMERIC NOT NULL DEFAULT 0,
    -- Derived columns
    total_revenue NUMERIC GENERATED ALWAYS AS (
        COALESCE(electricity_sales, 0) +
        COALESCE(oil_revenues, 0) +
        COALESCE(other_revenues, 0) +
        COALESCE(interest_income, 0) +
        COALESCE(share_in_net_income_of_associate, 0) +
        COALESCE(miscellaneous_income, 0)
    ) STORED,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_econ_value_pk PRIMARY KEY (year)
);

-- Drop and recreate table for economic expenditures
DROP TABLE IF EXISTS silver.econ_expenditures;
CREATE TABLE silver.econ_expenditures (
    year SMALLINT,
    company_id VARCHAR(20),
    type_id VARCHAR(10),
    government_payments NUMERIC NOT NULL DEFAULT 0,
    supplier_spending_local NUMERIC NOT NULL DEFAULT 0,
    supplier_spending_abroad NUMERIC NOT NULL DEFAULT 0,
    employee_wages_benefits NUMERIC NOT NULL DEFAULT 0,
    community_investments NUMERIC NOT NULL DEFAULT 0,
    depreciation NUMERIC NOT NULL DEFAULT 0,
    depletion NUMERIC NOT NULL DEFAULT 0,
    others NUMERIC NOT NULL DEFAULT 0,
    -- Derived columns
    total_supplier_spending NUMERIC GENERATED ALWAYS AS (
        COALESCE(supplier_spending_local, 0) +
        COALESCE(supplier_spending_abroad, 0)
    ) STORED,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT silver_econ_expenditures_pk PRIMARY KEY (year, company_id, type_id)
);

-- Drop and recreate table for capital provider payments
DROP TABLE IF EXISTS silver.econ_capital_provider_payment;
CREATE TABLE silver.econ_capital_provider_payment (
    year SMALLINT,
    interest NUMERIC NOT NULL DEFAULT 0,
    dividends_to_nci NUMERIC NOT NULL DEFAULT 0,
    dividends_to_parent NUMERIC NOT NULL DEFAULT 0,
    -- Derived columns
    total_dividends_interest NUMERIC GENERATED ALWAYS AS (
        COALESCE(interest, 0) +
        COALESCE(dividends_to_nci, 0) +
        COALESCE(dividends_to_parent, 0)
    ) STORED,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
); 


-- alter 
ALTER TABLE silver.econ_value ADD CONSTRAINT econ_value_unique_year UNIQUE (year);
ALTER TABLE silver.econ_expenditures ADD CONSTRAINT econ_expenditures_unique_key UNIQUE (year, company_id, type_id);
ALTER TABLE silver.econ_capital_provider_payment ADD CONSTRAINT econ_capital_provider_payment_unique_year UNIQUE (year);
