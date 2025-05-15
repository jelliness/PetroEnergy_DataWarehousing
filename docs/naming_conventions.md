# **Naming Conventions**

This document outlines the naming conventions used for schemas, tables, views, columns, and other objects in the data warehouse.

## **Table of Contents**

1. [General Principles](#general-principles)  
2. [Table Naming Conventions](#table-naming-conventions)  
   - [Bronze Rules](#bronze-rules)  
   - [Silver Rules](#silver-rules)  
   - [Gold Rules](#gold-rules)  
3. [Column Naming Conventions](#column-naming-conventions)  
   - [Surrogate Keys](#surrogate-keys)  
   - [Metadata Columns](#metadata-columns)  
4. [Stored Procedure Naming Conventions](#stored-procedure-naming-conventions)

---

## **General Principles**

- **Naming Conventions**: Use `snake_case`, with lowercase letters and underscores (`_`) to separate words.  
- **Language**: Use English for all names.  
- **Avoid Reserved Words**: Do not use SQL reserved words as object names.

---

## **Table Naming Conventions**

All tables must be placed in their respective **schema layer** (`bronze`, `silver`, `gold`) and follow the format:  
**`<layer>.<entity>_<table_name>`**

### **Bronze Rules**

- Schema: `bronze`  
- Table names must match the original names from the source system without renaming.  
- **Format**: `bronze.<sourcesystem>_<entity>`  
  - `<sourcesystem>`: Source system name (e.g., `envi`, `csv`, `csr`, `hr`, `econ`, `corp`)  
  - `<entity>`: Exact name from the source  
  - Example: `bronze.envi_water_withdrawal` → Water withdrawal from the ENVI dataset

### **Silver Rules**

- Schema: `silver`  
- Table names must also match the original names from the source system.  
- **Format**: `silver.<sourcesystem>_<entity>`  
  - Example: `silver.csr_project_funding`

### **Gold Rules**

- Schema: `gold`  
- Table names should reflect business logic, using domain-aligned naming conventions.  
- **Format**: `gold.<category>_<entity>`  
  - `<category>`: `dim`, `fact`, or `report`  
  - `<entity>`: Business-aligned table name  
  - Examples:
    - `gold.dim_customers` → Dimension table for customer data  
    - `gold.fact_sales` → Fact table for sales transactions  
    - `gold.report_sales_monthly` → Monthly sales reporting table  

#### **Glossary of Category Patterns**

| Pattern     | Meaning             | Example(s)                         |
|-------------|---------------------|------------------------------------|
| `dim_`      | Dimension table      | `gold.dim_product`                 |
| `fact_`     | Fact table           | `gold.fact_orders`                 |
| `report_`   | Reporting table      | `gold.report_user_engagement`      |

---

## **Column Naming Conventions**

### **Surrogate Keys**

- Use the suffix `_key` for all primary keys in dimension tables.  
- **Format**: `<table_name>_key`  
  - Example: `customer_key` in `gold.dim_customers`
  

### **Metadata Columns**

- Standard metadata tracking columns must be named:  
  - `created_at`: Timestamp when the record was created  
  - `updated_at`: Timestamp when the record was last updated

---

## **Stored Procedure Naming Conventions**

All stored procedures for loading data into each layer must reside in the corresponding schema and follow this pattern:  
**`<layer>.proc_load_<entity>_<layer>`**

- `<layer>`: `bronze`, `silver`, or `gold`  
- `<entity>`: Source or business entity name  
- Examples:
  - `bronze.proc_load_envi_bronze` → Loads ENVI data into the bronze layer  
  - `silver.proc_load_envi_silver` → Loads processed ENVI data into the silver layer  
  - `gold.proc_load_sales_gold` → Loads business-transformed sales data into the gold layer
