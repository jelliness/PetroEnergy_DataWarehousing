/*
===============================================================================
DDL Script: Create Gold Views for Economic Data
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final business-ready views combining data
    from the Silver layer with other dimensions.

    Each view performs transformations and combines data from the Silver layer 
    to produce clean, enriched, and business-ready datasets.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create View: gold.vw_economic_value_by_year
-- Purpose: Provides yearly economic value metrics
-- =============================================================================
DROP VIEW IF EXISTS gold.vw_economic_value_by_year;

CREATE VIEW gold.vw_economic_value_by_year AS
SELECT
    ev.year,
    ev.electricity_sales,
    ev.oil_revenues,
    ev.other_revenues,
    ev.interest_income,
    ev.share_in_net_income_of_associate,
    ev.miscellaneous_income,
    (ev.electricity_sales + ev.oil_revenues + ev.other_revenues + 
     ev.interest_income + ev.share_in_net_income_of_associate + 
     ev.miscellaneous_income) as total_economic_value
FROM silver.econ_value ev;

-- =============================================================================
-- Create View: gold.vw_expenditures_by_company
-- Purpose: Provides expenditure details by company with company information
-- =============================================================================
DROP VIEW IF EXISTS gold.vw_expenditures_by_company;

CREATE VIEW gold.vw_expenditures_by_company AS
SELECT
    ee.year,
    ee.company_id,
    c.company_name,
    p.company_name as parent_company_name,
    ee.type as cost_type,
    ee.government_payments,
    ee.supplier_spending_local,
    ee.supplier_spending_abroad,
    (ee.supplier_spending_local + ee.supplier_spending_abroad) as total_supplier_spending,
    ee.community_investments,
    ee.depreciation,
    ee.depletion,
    ee.others,
    (ee.government_payments + ee.supplier_spending_local + 
     ee.supplier_spending_abroad + ee.community_investments + 
     ee.depreciation + ee.depletion + ee.others) as total_expenditure
FROM silver.econ_expenditures ee
LEFT JOIN ref.company_main c
    ON ee.company_id = c.company_id
LEFT JOIN ref.company_main p
    ON c.parent_company_id = p.company_id;

-- =============================================================================
-- Create View: gold.vw_capital_provider_payments_by_year
-- Purpose: Provides yearly capital provider payment details
-- =============================================================================
DROP VIEW IF EXISTS gold.vw_capital_provider_payments_by_year;

CREATE VIEW gold.vw_capital_provider_payments_by_year AS
SELECT
    cpp.year,
    cpp.interest as interest_payments,
    cpp.dividends_to_nci as non_controlling_interest_dividends,
    cpp.dividends_to_parent as parent_company_dividends,
    (cpp.interest + cpp.dividends_to_nci + cpp.dividends_to_parent) as total_capital_provider_payments
FROM silver.econ_capital_provider_payment cpp;

-- =============================================================================
-- Create View: gold.vw_economic_summary_by_year
-- Purpose: Provides a comprehensive yearly summary of economic metrics
-- =============================================================================
DROP VIEW IF EXISTS gold.vw_economic_summary_by_year;

CREATE VIEW gold.vw_economic_summary_by_year AS
SELECT
    ev.year,
    ev.total_economic_value,
    SUM(ex.government_payments) as total_government_payments,
    SUM(ex.supplier_spending_local) as total_local_supplier_spending,
    SUM(ex.supplier_spending_abroad) as total_foreign_supplier_spending,
    SUM(ex.community_investments) as total_community_investments,
    SUM(ex.depreciation) as total_depreciation,
    SUM(ex.depletion) as total_depletion,
    SUM(ex.others) as total_other_expenditures,
    cpp.total_capital_provider_payments,
    (ev.total_economic_value - 
     (SUM(ex.government_payments) + 
      SUM(ex.supplier_spending_local) + 
      SUM(ex.supplier_spending_abroad) + 
      SUM(ex.community_investments) + 
      SUM(ex.depreciation) + 
      SUM(ex.depletion) + 
      SUM(ex.others) +
      cpp.total_capital_provider_payments)) as net_economic_value
FROM gold.vw_economic_value_by_year ev
LEFT JOIN gold.vw_expenditures_by_company ex
    ON ev.year = ex.year
LEFT JOIN gold.vw_capital_provider_payments_by_year cpp
    ON ev.year = cpp.year
GROUP BY 
    ev.year,
    ev.total_economic_value,
    cpp.total_capital_provider_payments;