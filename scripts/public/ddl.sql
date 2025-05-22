-- CREATE SCHEMA ref;
DROP TABLE IF EXISTS ref.company_main;

CREATE TABLE ref.company_main (
    company_id VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    parent_company_id VARCHAR(20) REFERENCES ref.company_main(company_id) ON DELETE SET NULL,
    address TEXT
);
