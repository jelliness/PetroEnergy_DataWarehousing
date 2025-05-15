/*
===============================================================================
DDL Script: Create Gold Views for Economic Data
===============================================================================
*/

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
SELECT
    ex.year,
    SUM(ex.government_payments) as total_government_payments,
    SUM(ex.supplier_spending_local) as total_local_supplier_spending,
    SUM(ex.supplier_spending_abroad) as total_foreign_supplier_spending,
    SUM(ex.community_investments) as total_community_investments,
    SUM(ex.depreciation) as total_depreciation,
    SUM(ex.depletion) as total_depletion,
    SUM(ex.others) as total_other_expenditures,
    cpp.interest as interest_payments,
    cpp.dividends_to_nci as non_controlling_interest_dividends,
    cpp.dividends_to_parent as parent_company_dividends,
    (SUM(ex.government_payments) + 
     SUM(ex.supplier_spending_local) + 
     SUM(ex.supplier_spending_abroad) + 
     SUM(ex.community_investments) + 
     SUM(ex.depreciation) + 
     SUM(ex.depletion) + 
     SUM(ex.others) +
     cpp.interest + 
     cpp.dividends_to_nci + 
     cpp.dividends_to_parent) as total_economic_value_distributed
FROM silver.econ_expenditures ex
LEFT JOIN silver.econ_capital_provider_payment cpp
    ON ex.year = cpp.year
GROUP BY 
    ex.year,
    cpp.interest,
    cpp.dividends_to_nci,
    cpp.dividends_to_parent;

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