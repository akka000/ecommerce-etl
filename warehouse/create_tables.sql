CREATE EXTENSION IF NOT EXISTS pgcrypto; 
 
CREATE TABLE IF NOT EXISTS dim_date ( 
    date_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
    invoice_date DATE UNIQUE 
); 
 
CREATE TABLE IF NOT EXISTS dim_customer ( 
    customer_id INT PRIMARY KEY, 
    country TEXT 
); 
 
CREATE TABLE IF NOT EXISTS sales_fact ( 
    sale_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
    invoice_date DATE REFERENCES dim_date(invoice_date), 
    total_sales NUMERIC, 
    total_quantity INT, 
    avg_price NUMERIC, 
    unique_customers INT 
); 
 
CREATE INDEX IF NOT EXISTS idx_sales_fact_date ON sales_fact(invoice_date);