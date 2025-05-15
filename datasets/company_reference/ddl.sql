
DROP TABLE IF EXISTS company_main;

CREATE TABLE company_main (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    parent_company_id INT REFERENCES company_main(company_id) ON DELETE SET NULL,
    is_branch BOOLEAN NOT NULL DEFAULT FALSE,
    branch_of INT REFERENCES company_main(company_id) ON DELETE SET NULL,
    renewable_energy_type VARCHAR(100),
    address TEXT
);
