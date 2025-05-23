-- Table: public.status
CREATE TABLE public.status (
    status_id VARCHAR(3) PRIMARY KEY,
    status_name VARCHAR(10) NOT NULL
);

-- Table: public.roles
CREATE TABLE public.roles (
    role_id VARCHAR(3) PRIMARY KEY,
    role_name VARCHAR(15) NOT NULL
);

-- Table: public.account
CREATE TABLE public.account (
    account_id VARCHAR(20) PRIMARY KEY,
    email VARCHAR(254) NOT NULL,
    account_role VARCHAR(3) NOT NULL,
    power_plant_id VARCHAR(10) NOT NULL,
    company_id VARCHAR(10) NOT NULL,
    account_status VARCHAR(10) NOT NULL,
    date_created TIMESTAMP NOT NULL,
    date_updated TIMESTAMP NOT NULL,
    CONSTRAINT fk_account_role FOREIGN KEY (account_role) REFERENCES public.roles(role_id)
);

-- Table: public.audit_trail
CREATE TABLE public.audit_trail (
    audit_id VARCHAR(20) PRIMARY KEY,
    account_id VARCHAR(20) NOT NULL,
    target_table VARCHAR(20) NOT NULL,
    record_id VARCHAR(20) NOT NULL,
    action_type VARCHAR(10) NOT NULL,
    old_value TEXT NOT NULL,
    new_value TEXT NOT NULL,
    audit_timestamp TIMESTAMP NOT NULL,
    description TEXT NOT NULL,
    CONSTRAINT fk_audit_account FOREIGN KEY (account_id) REFERENCES public.account(account_id)
);

-- Table: public.checker_status_log
CREATE TABLE public.checker_status_log (
    cs_id VARCHAR(20) PRIMARY KEY,
    checker_id VARCHAR(20) NOT NULL,
    record_id VARCHAR(20) NOT NULL,
    status_id VARCHAR(3) NOT NULL,
    status_timestamp TIMESTAMP NOT NULL,
    CONSTRAINT fk_checker_account FOREIGN KEY (checker_id) REFERENCES public.account(account_id),
    CONSTRAINT fk_status_log FOREIGN KEY (status_id) REFERENCES public.status(status_id)
);

-- Table: public.attachment
CREATE TABLE public.attachment (
    record_id VARCHAR(20) PRIMARY KEY,
    attachment_file TEXT NOT NULL
);

CREATE TABLE public.user_profile (
    account_id VARCHAR(20) PRIMARY KEY,  -- also serves as profile_id
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    middle_name VARCHAR(50),
	suffix VARCHAR(5),
    contact_number VARCHAR(20),
    address TEXT,
    birthdate DATE,
    gender VARCHAR(10),
    profile_created TIMESTAMP NOT NULL,
    profile_updated TIMESTAMP NOT NULL,
    CONSTRAINT fk_user_account FOREIGN KEY (account_id) REFERENCES public.account(account_id)
);
