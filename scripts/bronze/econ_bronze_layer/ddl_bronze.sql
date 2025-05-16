/*
===============================================================================
DDL Script: Create Bronze Tables for Econ Value, Expenditures, and Capital Provider Payments
===============================================================================
Script Purpose:
    This script creates bronze-layer tables for economic reporting by year.
    Each table will be dropped if it exists, then recreated.
===============================================================================
*/

-- Drop and recreate table for economic value
DROP TABLE IF EXISTS bronze.econ_value;

CREATE TABLE bronze.econ_value (
    year SMALLINT,  -- Year of the data entry
    electricity_sales NUMERIC,
    oil_revenues NUMERIC,
    other_revenues NUMERIC,
    interest_income NUMERIC,
    share_in_net_income_of_associate NUMERIC,
    miscellaneous_income NUMERIC,
    CONSTRAINT econ_value_pk PRIMARY KEY (year)
);

-- Drop and recreate table for economic expenditures
DROP TABLE IF EXISTS bronze.econ_expenditures;

CREATE TABLE bronze.econ_expenditures (
    year SMALLINT,  -- Year of the data entry
    company_id VARCHAR(20),  -- Foreign key to econ_company_info
    type VARCHAR(10),  -- Cost category (e.g., COS or G&A)
    government_payments NUMERIC,
    supplier_spending_local NUMERIC,
    supplier_spending_abroad NUMERIC,
    employee_wages_benefits NUMERIC, 
    community_investments NUMERIC,
    depreciation NUMERIC,
    depletion NUMERIC,
    others NUMERIC,
    CONSTRAINT econ_expenditures_pk PRIMARY KEY (year, company_id, type)
);

-- Drop and recreate table for capital provider payments (detailed breakdown)
DROP TABLE IF EXISTS bronze.econ_capital_provider_payment;

CREATE TABLE bronze.econ_capital_provider_payment (
    year SMALLINT,  -- Year of the expenditures
    interest NUMERIC,  -- Payments made to lenders
    dividends_to_nci NUMERIC,  -- Dividends to non-controlling interests
    dividends_to_parent NUMERIC,  -- Dividends to parent/holding company
    CONSTRAINT econ_capital_provider_payment_pk PRIMARY KEY (year)
);


-- Adding constraints (UNIQUE)
ALTER TABLE bronze.econ_value ADD CONSTRAINT econ_value_unique_year UNIQUE (year);
ALTER TABLE bronze.econ_expenditures ADD CONSTRAINT econ_expenditures_unique_key UNIQUE (year, company_id, type);
ALTER TABLE bronze.econ_capital_provider_payment ADD CONSTRAINT econ_capital_provider_payment_unique_year UNIQUE (year);