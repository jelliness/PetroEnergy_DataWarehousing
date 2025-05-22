/*
===============================================================================
Function Script: Create Gold Functions for CSR Data
===============================================================================
*/

-- =============================================================================
-- Create Function for gold.func_program_descriptions
-- =============================================================================
DROP FUNCTION IF EXISTS gold.func_program_descriptions;

CREATE OR REPLACE FUNCTION gold.func_program_descriptions(
    p_category VARCHAR[] DEFAULT NULL,
    p_project VARCHAR[] DEFAULT NULL,
    p_order_by VARCHAR(50) DEFAULT 'Category',
    p_order_direction VARCHAR(4) DEFAULT 'ASC'
)
RETURNS TABLE (
    "Category" TEXT,
    "Project" TEXT,
    "Metric" TEXT
)
AS $$
BEGIN
    RETURN QUERY EXECUTE format('
    SELECT
        pd."Category"::TEXT,
        pd."Project"::TEXT,
        pd."Metric"::TEXT
    FROM gold.dim_program_descriptions pd
    WHERE ($1 IS NULL OR pd."Category" = ANY($1))
      AND ($2 IS NULL OR pd."Project" = ANY($2))
    ORDER BY %I %s',
        p_order_by, p_order_direction)
    USING p_category, p_project;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Create Function for gold.func_csr_numbers
-- =============================================================================
DROP FUNCTION IF EXISTS gold.func_csr_numbers;

CREATE OR REPLACE FUNCTION gold.func_csr_numbers(
    p_company VARCHAR[] DEFAULT NULL,
    p_program VARCHAR[] DEFAULT NULL,
    p_year INT[] DEFAULT NULL,
    p_project VARCHAR[] DEFAULT NULL,
    p_order_by VARCHAR(50) DEFAULT 'Company',
    p_order_direction VARCHAR(4) DEFAULT 'ASC'
)
RETURNS TABLE (
    "Company" TEXT,
    "Program Name" TEXT,
    "Year" SMALLINT,
    "Project" TEXT,
    "Report" TEXT,
    "Metrics" TEXT,
    "Expenses" NUMERIC
)
AS $$
BEGIN
    RETURN QUERY EXECUTE format('
    SELECT
        cn."Company"::TEXT,
        cn."Program Name"::TEXT,
        cn."Year",
        cn."Project"::TEXT,
        cn."Report"::TEXT,
        cn."Metrics"::TEXT,
        cn."Expenses"
    FROM gold.dim_csr_numbers cn
    WHERE ($1 IS NULL OR cn."Company" = ANY($1))
      AND ($2 IS NULL OR cn."Program Name" = ANY($2))
      AND ($3 IS NULL OR cn."Year" = ANY($3))
      AND ($4 IS NULL OR cn."Project" = ANY($4))
    ORDER BY %I %s',
        p_order_by, p_order_direction)
    USING p_company, p_program, p_year, p_project;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Create Function for gold.func_perc_report
-- =============================================================================
DROP FUNCTION IF EXISTS gold.func_perc_report;

CREATE OR REPLACE FUNCTION gold.func_perc_report(
    p_program VARCHAR[] DEFAULT NULL,
    p_project VARCHAR[] DEFAULT NULL,
    p_year INT[] DEFAULT NULL,
    p_order_by VARCHAR(50) DEFAULT 'Program',
    p_order_direction VARCHAR(4) DEFAULT 'ASC'
)
RETURNS TABLE (
    "Program" TEXT,
    "Project" TEXT,
    "Year" SMALLINT,
    "Total" NUMERIC
)
AS $$
BEGIN
    RETURN QUERY EXECUTE format('
    SELECT
        pr."Program"::TEXT,
        pr."Project"::TEXT,
        pr."Year",
        pr."Total"
    FROM gold.fact_perc_report pr
    WHERE ($1 IS NULL OR pr."Program" = ANY($1))
      AND ($2 IS NULL OR pr."Project" = ANY($2))
      AND ($3 IS NULL OR pr."Year" = ANY($3))
    ORDER BY %I %s',
        p_order_by, p_order_direction)
    USING p_program, p_project, p_year;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Create Function for gold.func_investment_per_company
-- =============================================================================
DROP FUNCTION IF EXISTS gold.func_investment_per_company;

CREATE OR REPLACE FUNCTION gold.func_investment_per_company(
    p_company VARCHAR[] DEFAULT NULL,
    p_program VARCHAR[] DEFAULT NULL,
    p_min_investment NUMERIC DEFAULT NULL,
    p_max_investment NUMERIC DEFAULT NULL,
    p_order_by VARCHAR(50) DEFAULT 'Company',
    p_order_direction VARCHAR(4) DEFAULT 'ASC'
)
RETURNS TABLE (
    "Company" TEXT,
    "Program" TEXT,
    "Investments" NUMERIC
)
AS $$
BEGIN
    RETURN QUERY EXECUTE format('
    SELECT
        ipc."Company"::TEXT,
        ipc."Program"::TEXT,
        ipc."Investments"
    FROM gold.fact_investment_per_company ipc
    WHERE ($1 IS NULL OR ipc."Company" = ANY($1))
      AND ($2 IS NULL OR ipc."Program" = ANY($2))
      AND ($3 IS NULL OR ipc."Investments" >= $3)
      AND ($4 IS NULL OR ipc."Investments" <= $4)
    ORDER BY %I %s',
        p_order_by, p_order_direction)
    USING p_company, p_program, p_min_investment, p_max_investment;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Create Function for gold.func_perc_investment
-- =============================================================================
DROP FUNCTION IF EXISTS gold.func_perc_investment;

CREATE OR REPLACE FUNCTION gold.func_perc_investment(
    p_program_name VARCHAR[] DEFAULT NULL,
    p_min_investment NUMERIC DEFAULT NULL,
    p_max_investment NUMERIC DEFAULT NULL,
    p_order_by VARCHAR(50) DEFAULT 'program_name',
    p_order_direction VARCHAR(4) DEFAULT 'ASC'
)
RETURNS TABLE (
    program_name TEXT,
    "Investments" NUMERIC
)
AS $$
BEGIN
    RETURN QUERY EXECUTE format('
    SELECT
        pi.program_name::TEXT,
        pi."Investments"
    FROM gold.fact_perc_investment pi
    WHERE ($1 IS NULL OR pi.program_name = ANY($1))
      AND ($2 IS NULL OR pi."Investments" >= $2)
      AND ($3 IS NULL OR pi."Investments" <= $3)
    ORDER BY %I %s',
        p_order_by, p_order_direction)
    USING p_program_name, p_min_investment, p_max_investment;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Create Function for gold.func_csr_summary (combines data from multiple views)
-- =============================================================================
DROP FUNCTION IF EXISTS gold.func_csr_summary;

CREATE OR REPLACE FUNCTION gold.func_csr_summary(
    p_year INT[] DEFAULT NULL,
    p_company VARCHAR[] DEFAULT NULL,
    p_program VARCHAR[] DEFAULT NULL
)
RETURNS TABLE (
    "Year" SMALLINT,
    "Company" TEXT,
    "Program" TEXT,
    "Total_Projects" BIGINT,
    "Total_Investment" NUMERIC,
    "Avg_Report_Score" NUMERIC
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        cn."Year",
        cn."Company"::TEXT,
        cn."Program Name"::TEXT AS "Program",
        COUNT(DISTINCT cn."Project") AS "Total_Projects",
        SUM(cn."Expenses") AS "Total_Investment",
        AVG(cn."Report"::NUMERIC) AS "Avg_Report_Score"
    FROM gold.dim_csr_numbers cn
    WHERE ($1 IS NULL OR cn."Year" = ANY($1))
      AND ($2 IS NULL OR cn."Company" = ANY($2))
      AND ($3 IS NULL OR cn."Program Name" = ANY($3))
    GROUP BY cn."Year", cn."Company", cn."Program Name"
    ORDER BY cn."Year", cn."Company", cn."Program Name";
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Sample queries to test the functions
-- =============================================================================
/*
-- Get all program descriptions
SELECT * FROM gold.func_program_descriptions();

-- Filter by Category
SELECT * FROM gold.func_program_descriptions(
    ARRAY['Education']::VARCHAR[]
);

-- Get CSR numbers for a specific company and year
SELECT * FROM gold.func_csr_numbers(
    ARRAY['Maibarara Geothermal Incorporated']::VARCHAR[],
    NULL,
    ARRAY[2024]::INT[]
);

-- Get percentage reports ordered by total in descending order
SELECT * FROM gold.func_perc_report(
    NULL, NULL, NULL,
    'Total',
    'DESC'
);

-- Get investments per company with minimum threshold
SELECT * FROM gold.func_investment_per_company(
    NULL, NULL,
    1000, -- Min investment
    NULL,  -- No max
    'Investments',
    'DESC'
);

-- Get percentage investments for specific programs
SELECT * FROM gold.func_perc_investment(
    ARRAY['Community Development', 'Disaster Relief']::VARCHAR[],
    NULL, NULL,
    'Investments',
    'DESC'
);

-- Get CSR summary for a specific year
SELECT * FROM gold.func_csr_summary(
    ARRAY[2024]::INT[]
);

-- Get CSR summary for a specific company across all years
SELECT * FROM gold.func_csr_summary(
    NULL,
    ARRAY['Maibarara Geothermal Incorporated']::VARCHAR[]
);
*/