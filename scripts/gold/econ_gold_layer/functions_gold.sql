/*
===============================================================================
Function Script: Create Gold Functions for Economic Data
===============================================================================
*/
-- =============================================================================
-- NOTE: Execute the create functions first, then execute the sample queries for checking
-- =============================================================================

-- =============================================================================
-- Create Functions for gold.func_economic_value_by_year
-- =============================================================================
DROP FUNCTION IF EXISTS gold.func_economic_value_by_year;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_economic_value_by_year(
    p_year SMALLINT[] DEFAULT NULL,
    p_order_by VARCHAR(50) DEFAULT 'year',
    p_order_direction VARCHAR(4) DEFAULT 'ASC'
)
RETURNS TABLE (
    year SMALLINT,
    total_economic_value_generated NUMERIC,
    total_economic_value_distributed NUMERIC,
    economic_value_retained NUMERIC
)
AS $$
BEGIN
    RETURN QUERY EXECUTE format('
    SELECT
        evs.year,
        evs.total_economic_value_generated,
        evs.total_economic_value_distributed,
        evs.economic_value_retained
    FROM gold.vw_economic_value_summary evs
    WHERE ($1 IS NULL OR evs.year = ANY($1))
    ORDER BY %I %s',
        p_order_by, p_order_direction)
    USING p_year;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Create Functions for gold.func_economic_value_generated_details
-- =============================================================================
DROP FUNCTION IF EXISTS gold.func_economic_value_generated_details;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_economic_value_generated_details(
    p_year SMALLINT[] DEFAULT NULL,
    p_order_by VARCHAR(50) DEFAULT 'year',
    p_order_direction VARCHAR(4) DEFAULT 'ASC'
)
RETURNS TABLE (
    year SMALLINT,
    electricity_sales NUMERIC,
    oil_revenues NUMERIC,
    other_revenues NUMERIC,
    interest_income NUMERIC,
    share_in_net_income_of_associate NUMERIC,
    miscellaneous_income NUMERIC,
    total_economic_value_generated NUMERIC
)
AS $$
BEGIN
    RETURN QUERY EXECUTE format('
    SELECT
        evg.year,
        evg.electricity_sales,
        evg.oil_revenues,
        evg.other_revenues,
        evg.interest_income,
        evg.share_in_net_income_of_associate,
        evg.miscellaneous_income,
        evg.total_economic_value_generated
    FROM gold.vw_economic_value_generated evg
    WHERE ($1 IS NULL OR evg.year = ANY($1))
    ORDER BY %I %s',
        p_order_by, p_order_direction)
    USING p_year;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Create Functions for gold.func_economic_value_distributed_details
-- =============================================================================
DROP FUNCTION IF EXISTS gold.func_economic_value_distributed_details;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_economic_value_distributed_details(
    p_year SMALLINT[] DEFAULT NULL,
    p_order_by VARCHAR(50) DEFAULT 'year',
    p_order_direction VARCHAR(4) DEFAULT 'ASC'
)
RETURNS TABLE (
    year SMALLINT,
    total_government_payments NUMERIC,
    total_local_supplier_spending NUMERIC,
    total_foreign_supplier_spending NUMERIC,
    total_employee_wages_benefits NUMERIC,
    total_community_investments NUMERIC,
    total_depreciation NUMERIC,
    total_depletion NUMERIC,
    total_other_expenditures NUMERIC,
    total_capital_provider_payments NUMERIC,
    total_economic_value_distributed NUMERIC
)
AS $$
BEGIN
    RETURN QUERY EXECUTE format('
    SELECT
        evd.year,
        evd.total_government_payments,
        evd.total_local_supplier_spending,
        evd.total_foreign_supplier_spending,
        evd.total_employee_wages_benefits,
        evd.total_community_investments,
        evd.total_depreciation,
        evd.total_depletion,
        evd.total_other_expenditures,
        evd.total_capital_provider_payments,
        evd.total_economic_value_distributed
    FROM gold.vw_economic_value_distributed evd
    WHERE ($1 IS NULL OR evd.year = ANY($1))
    ORDER BY %I %s',
        p_order_by, p_order_direction)
    USING p_year;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Create Functions for gold.func_economic_expenditure_by_company
-- =============================================================================
DROP FUNCTION IF EXISTS gold.func_economic_expenditure_by_company;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_economic_expenditure_by_company(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_type_id VARCHAR(10)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL,
    p_order_by VARCHAR(50) DEFAULT 'year',
    p_order_direction VARCHAR(4) DEFAULT 'ASC'
)
RETURNS TABLE (
    year SMALLINT,
    company_name VARCHAR(255),
    type_id VARCHAR(10),
    government_payments NUMERIC,
    local_supplier_spending NUMERIC,
    foreign_supplier_spending NUMERIC,
    employee_wages_benefits NUMERIC,
    community_investments NUMERIC,
    depreciation NUMERIC,
    depletion NUMERIC,
    other_expenditures NUMERIC,
    total_distributed_value_by_company NUMERIC
)
AS $$
BEGIN
    RETURN QUERY EXECUTE format('
    WITH filtered_data AS (
        SELECT 
            eec.year,
            eec.company_name,
            eec.type_id,
            eec.government_payments,
            eec.local_supplier_spending,
            eec.foreign_supplier_spending,
            eec.employee_wages_benefits,
            eec.community_investments,
            eec.depreciation,
            eec.depletion,
            eec.other_expenditures,
            eec.total_distributed_value_by_company,
            cm.company_id
        FROM gold.vw_economic_expenditure_by_company eec
        JOIN ref.company_main cm ON eec.company_name = cm.company_name
    )
    SELECT
        fd.year,
        fd.company_name,
        fd.type_id,
        fd.government_payments,
        fd.local_supplier_spending,
        fd.foreign_supplier_spending,
        fd.employee_wages_benefits,
        fd.community_investments,
        fd.depreciation,
        fd.depletion,
        fd.other_expenditures,
        fd.total_distributed_value_by_company
    FROM filtered_data fd
    WHERE ($1 IS NULL OR fd.company_id = ANY($1))
      AND ($2 IS NULL OR fd.type_id = ANY($2))
      AND ($3 IS NULL OR fd.year = ANY($3))
    ORDER BY %I %s',
        p_order_by, p_order_direction)
    USING p_company_id, p_type_id, p_year;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Create Functions for gold.func_economic_value_distributed_by_company
-- =============================================================================
DROP FUNCTION IF EXISTS gold.func_economic_value_distributed_by_company;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_economic_value_distributed_by_company(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL,
    p_order_by VARCHAR(50) DEFAULT 'year',
    p_order_direction VARCHAR(4) DEFAULT 'ASC'
)
RETURNS TABLE (
    year SMALLINT,
    company_name VARCHAR(255),
    total_government_payments NUMERIC,
    total_local_supplier_spending NUMERIC,
    total_foreign_supplier_spending NUMERIC,
    total_employee_wages_benefits NUMERIC,
    total_community_investments NUMERIC,
    total_depreciation NUMERIC,
    total_depletion NUMERIC,
    total_other_expenditures NUMERIC,
    total_economic_value_distributed_by_company NUMERIC,
    percentage_of_total_distribution NUMERIC
)
AS $$
BEGIN
    RETURN QUERY EXECUTE format('
    WITH filtered_data AS (
        SELECT 
            evdc.year,
            evdc.company_name,
            evdc.total_government_payments,
            evdc.total_local_supplier_spending,
            evdc.total_foreign_supplier_spending,
            evdc.total_employee_wages_benefits,
            evdc.total_community_investments,
            evdc.total_depreciation,
            evdc.total_depletion,
            evdc.total_other_expenditures,
            evdc.total_economic_value_distributed_by_company,
            evdc.percentage_of_total_distribution,
            cm.company_id
        FROM gold.vw_economic_value_distributed_by_company evdc
        JOIN ref.company_main cm ON evdc.company_name = cm.company_name
    )
    SELECT
        fd.year,
        fd.company_name,
        fd.total_government_payments,
        fd.total_local_supplier_spending,
        fd.total_foreign_supplier_spending,
        fd.total_employee_wages_benefits,
        fd.total_community_investments,
        fd.total_depreciation,
        fd.total_depletion,
        fd.total_other_expenditures,
        fd.total_economic_value_distributed_by_company,
        fd.percentage_of_total_distribution
    FROM filtered_data fd
    WHERE ($1 IS NULL OR fd.company_id = ANY($1))
      AND ($2 IS NULL OR fd.year = ANY($2))
    ORDER BY %I %s',
        p_order_by, p_order_direction)
    USING p_company_id, p_year;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Create Functions for gold.func_economic_value_distribution_percentage
-- =============================================================================
DROP FUNCTION IF EXISTS gold.func_economic_value_distribution_percentage;

-- Now create our new functions
CREATE OR REPLACE FUNCTION gold.func_economic_value_distribution_percentage(
    p_company_id VARCHAR(10)[] DEFAULT NULL,
    p_year SMALLINT[] DEFAULT NULL,
    p_order_by VARCHAR(50) DEFAULT 'percentage_of_total_distribution',
    p_order_direction VARCHAR(4) DEFAULT 'DESC'
)
RETURNS TABLE (
    year SMALLINT,
    company_name VARCHAR(255),
    total_economic_value_distributed_by_company NUMERIC,
    percentage_of_total_distribution NUMERIC
)
AS $$
BEGIN
    RETURN QUERY EXECUTE format('
    WITH filtered_data AS (
        SELECT 
            evdc.year,
            evdc.company_name,
            evdc.total_economic_value_distributed_by_company,
            evdc.percentage_of_total_distribution,
            cm.company_id
        FROM gold.vw_economic_value_distributed_by_company evdc
        JOIN ref.company_main cm ON evdc.company_name = cm.company_name
    )
    SELECT
        fd.year,
        fd.company_name,
        fd.total_economic_value_distributed_by_company,
        fd.percentage_of_total_distribution
    FROM filtered_data fd
    WHERE ($1 IS NULL OR fd.company_id = ANY($1))
      AND ($2 IS NULL OR fd.year = ANY($2))
    ORDER BY %I %s',
        p_order_by, p_order_direction)
    USING p_company_id, p_year;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Sample Queries for Testing Functions
-- =============================================================================
/*
-- Get economic value summary for all years
SELECT * FROM gold.func_economic_value_by_year();

-- Get economic value summary for specific years
SELECT * FROM gold.func_economic_value_by_year(
    ARRAY[2022, 2023]::SMALLINT[]
);

-- Order by economic value retained, highest first
SELECT * FROM gold.func_economic_value_by_year(
    NULL,
    'economic_value_retained',
    'DESC'
);

-- Get detailed breakdown of value generation for all years
SELECT * FROM gold.func_economic_value_generated_details();

-- Get value generation details for 2023 only
SELECT * FROM gold.func_economic_value_generated_details(
    ARRAY[2023]::SMALLINT[]
);

-- Order by highest electricity sales
SELECT * FROM gold.func_economic_value_generated_details(
    NULL,
    'electricity_sales', 
    'DESC'
);

-- Get all years' distribution details
SELECT * FROM gold.func_economic_value_distributed_details();

-- Get distribution details for specific years
SELECT * FROM gold.func_economic_value_distributed_details(
    ARRAY[2021, 2022]::SMALLINT[]
);

-- Order by highest total employee wages and benefits
SELECT * FROM gold.func_economic_value_distributed_details(
    NULL,
    'total_employee_wages_benefits',
    'DESC'
);

-- Get expenditures for all companies, years, and types
SELECT * FROM gold.func_economic_expenditure_by_company();

-- Filter by specific companies
SELECT * FROM gold.func_economic_expenditure_by_company(
    ARRAY['PERC', 'PGEC']::VARCHAR(10)[]
);

-- Filter by year and expenditure type
SELECT * FROM gold.func_economic_expenditure_by_company(
    NULL,
    ARRAY['CS']::TEXT[],
    ARRAY[2023]::SMALLINT[]
);

-- Filter by company and year, ordered by highest employee wages
SELECT * FROM gold.func_economic_expenditure_by_company(
    ARRAY['PSC']::VARCHAR(10)[],
    NULL,
    ARRAY[2022, 2023]::SMALLINT[],
    'employee_wages_benefits',
    'DESC'
);

-- Get distribution totals for all companies and years
SELECT * FROM gold.func_economic_value_distributed_by_company();

-- Filter by specific companies
SELECT * FROM gold.func_economic_value_distributed_by_company(
    ARRAY['PERC', 'PGEC', 'PSC']::VARCHAR(10)[]
);

-- Filter by year and order by highest government payments
SELECT * FROM gold.func_economic_value_distributed_by_company(
    NULL,
    ARRAY[2023]::SMALLINT[],
    'total_government_payments',
    'DESC'
);

-- Get percentage distribution for all companies and years
-- Default sort is by highest percentage contribution
SELECT * FROM gold.func_economic_value_distribution_percentage();

-- Filter to see only specific companies
SELECT * FROM gold.func_economic_value_distribution_percentage(
    ARRAY['PWEI', 'MGI']::VARCHAR(10)[]
);

-- Get contribution percentages for 2023, ordered by year
SELECT * FROM gold.func_economic_value_distribution_percentage(
    NULL,
    ARRAY[2023]::SMALLINT[],
    'year',
    'ASC'
);

-- Compare total economic value generated vs. distributed for specific years
SELECT 
    g.year,
    g.total_economic_value_generated,
    d.total_economic_value_distributed,
    (g.total_economic_value_generated - d.total_economic_value_distributed) AS economic_value_retained
FROM gold.func_economic_value_generated_details(
    ARRAY[2022, 2024]::SMALLINT[]
) g
JOIN gold.func_economic_value_distributed_details(
    ARRAY[2022, 2024]::SMALLINT[]
) d ON g.year = d.year
ORDER BY g.year;

-- Get top 3 contributing companies for 2023
SELECT * FROM gold.func_economic_value_distribution_percentage(
    NULL,
    ARRAY[2023]::SMALLINT[],
    'percentage_of_total_distribution',
    'DESC'
) LIMIT 3;
*/ 