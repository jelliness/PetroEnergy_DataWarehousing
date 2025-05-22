-- CREATE SCHEMA ref;
DROP TABLE IF EXISTS ref.company_main;

CREATE TABLE ref.company_main (
    company_id VARCHAR(20) PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    parent_company_id VARCHAR(20) REFERENCES ref.company_main(company_id) ON DELETE SET NULL,
    address TEXT
);

DROP TABLE IF EXISTS ref.expenditure_type;
CREATE TABLE ref.expenditure_type (
    type_id VARCHAR(4) PRIMARY KEY,
    type_description TEXT
);

