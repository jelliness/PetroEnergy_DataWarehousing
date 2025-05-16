/*
===============================================================================
DDL Script: Create Gold Views for Economic Data
===============================================================================
*/

-- Drop views if they already exist
DROP VIEW IF EXISTS gold.vw_economic_value_summary;
DROP VIEW IF EXISTS gold.vw_economic_value_distributed;
DROP VIEW IF EXISTS gold.vw_economic_value_generated;

-- Now create our new views
-- =============================================================================
-- Create View: gold.vw_economic_value_generated
-- =============================================================================
CREATE VIEW gold.vw_economic_value_generated AS
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
     ev.miscellaneous_income) as total_economic_value_generated
FROM silver.econ_value ev;

-- =============================================================================
-- Create View: gold.vw_economic_value_distributed
-- =============================================================================
CREATE VIEW gold.vw_economic_value_distributed AS
WITH expenditure_totals AS (
    SELECT
        year,
        SUM(government_payments) as total_government_payments,
        SUM(supplier_spending_local) as total_local_supplier_spending,
        SUM(supplier_spending_abroad) as total_foreign_supplier_spending,
        SUM(employee_wages_benefits) as total_employee_wages_benefits,
        SUM(community_investments) as total_community_investments,
        SUM(depreciation) as total_depreciation,
        SUM(depletion) as total_depletion,
        SUM(others) as total_other_expenditures
    FROM silver.econ_expenditures
    GROUP BY year
)
SELECT
    et.year,
    et.total_government_payments,
    et.total_local_supplier_spending,
    et.total_foreign_supplier_spending,
    et.total_employee_wages_benefits,
    et.total_community_investments,
    et.total_depreciation,
    et.total_depletion,
    et.total_other_expenditures,
    COALESCE(cpp.total_dividends_interest, 0) as total_capital_provider_payments,
    (et.total_government_payments + 
     et.total_local_supplier_spending + 
     et.total_foreign_supplier_spending + 
     et.total_employee_wages_benefits + 
     et.total_community_investments + 
     COALESCE(cpp.total_dividends_interest, 0)) as total_economic_value_distributed
FROM expenditure_totals et
LEFT JOIN silver.econ_capital_provider_payment cpp ON et.year = cpp.year;

-- =============================================================================
-- Create View: gold.vw_economic_value_summary
-- =============================================================================
CREATE VIEW gold.vw_economic_value_summary AS
SELECT
    g.year,
    g.total_economic_value_generated,
    d.total_economic_value_distributed,
    (g.total_economic_value_generated - d.total_economic_value_distributed) as economic_value_retained
FROM gold.vw_economic_value_generated g
LEFT JOIN gold.vw_economic_value_distributed d
    ON g.year = d.year;