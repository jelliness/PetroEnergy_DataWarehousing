/*
===============================================================================
DDL Script: Create Gold Views for Economic Data
===============================================================================
*/

-- Drop views if they already exist
DROP VIEW IF EXISTS gold.vw_economic_value_summary;
DROP VIEW IF EXISTS gold.vw_economic_value_distributed;
DROP VIEW IF EXISTS gold.vw_economic_value_generated;
DROP VIEW IF EXISTS gold.vw_economic_expenditure_by_company;
DROP VIEW IF EXISTS gold.vw_economic_value_distributed_by_company;

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
    COALESCE(d.total_economic_value_distributed, 0) as total_economic_value_distributed,
    (g.total_economic_value_generated - COALESCE(d.total_economic_value_distributed, 0)) as economic_value_retained
FROM gold.vw_economic_value_generated g
LEFT JOIN gold.vw_economic_value_distributed d
    ON g.year = d.year;

-- =============================================================================
-- Create View: gold.vw_economic_expenditure_by_company
-- =============================================================================
CREATE VIEW gold.vw_economic_expenditure_by_company AS
SELECT
    ex.year,
    ex.company_id,
    cm.company_name,
    ex.type_id,
    et.type_description,
    SUM(ex.government_payments) as government_payments,
    SUM(ex.supplier_spending_local) as local_supplier_spending,
    SUM(ex.supplier_spending_abroad) as foreign_supplier_spending,
    SUM(ex.employee_wages_benefits) as employee_wages_benefits,
    SUM(ex.community_investments) as community_investments,
    SUM(ex.depreciation) as depreciation,
    SUM(ex.depletion) as depletion,
    SUM(ex.others) as other_expenditures,
    (SUM(ex.government_payments) + 
     SUM(ex.supplier_spending_local) + 
     SUM(ex.supplier_spending_abroad) + 
     SUM(ex.employee_wages_benefits) + 
     SUM(ex.community_investments)) as total_distributed_value_by_company
FROM silver.econ_expenditures ex
JOIN ref.company_main cm ON ex.company_id = cm.company_id
JOIN ref.expenditure_type et ON ex.type_id = et.type_id
GROUP BY ex.year, ex.company_id, cm.company_name, ex.type_id, et.type_description
ORDER BY ex.year, cm.company_name, et.type_description;

-- =============================================================================
-- Create View: gold.vw_economic_value_distributed_by_company
-- =============================================================================
CREATE VIEW gold.vw_economic_value_distributed_by_company AS
WITH company_totals AS (
    -- Calculate totals per company per year
    SELECT
        ex.year,
        ex.company_id,
        cm.company_name,
        SUM(ex.government_payments) as total_government_payments,
        SUM(ex.supplier_spending_local) as total_local_supplier_spending,
        SUM(ex.supplier_spending_abroad) as total_foreign_supplier_spending,
        SUM(ex.employee_wages_benefits) as total_employee_wages_benefits,
        SUM(ex.community_investments) as total_community_investments,
        SUM(ex.depreciation) as total_depreciation,
        SUM(ex.depletion) as total_depletion,
        SUM(ex.others) as total_other_expenditures,
        (SUM(ex.government_payments) + 
         SUM(ex.supplier_spending_local) + 
         SUM(ex.supplier_spending_abroad) + 
         SUM(ex.employee_wages_benefits) + 
         SUM(ex.community_investments)) as total_economic_value_distributed_by_company
    FROM silver.econ_expenditures ex
    JOIN ref.company_main cm ON ex.company_id = cm.company_id
    GROUP BY ex.year, ex.company_id, cm.company_name
),
year_totals AS (
    -- Calculate overall totals per year across all companies
    SELECT
        year,
        SUM(government_payments + 
            supplier_spending_local + 
            supplier_spending_abroad + 
            employee_wages_benefits + 
            community_investments) as year_total_distribution
    FROM silver.econ_expenditures
    GROUP BY year
)
SELECT
    ct.year,
    ct.company_id,
    ct.company_name,
    ct.total_government_payments,
    ct.total_local_supplier_spending,
    ct.total_foreign_supplier_spending,
    ct.total_employee_wages_benefits,
    ct.total_community_investments,
    ct.total_depreciation,
    ct.total_depletion,
    ct.total_other_expenditures,
    ct.total_economic_value_distributed_by_company,
    -- Calculate percentage contribution to total economic distribution
    CASE 
        WHEN yt.year_total_distribution > 0 
        THEN 
            ROUND(
                (ct.total_economic_value_distributed_by_company * 100.0) / 
                yt.year_total_distribution,
                2
            )
        ELSE 0 
    END as percentage_of_total_distribution
FROM company_totals ct
JOIN year_totals yt ON ct.year = yt.year
ORDER BY ct.year, ct.company_name;