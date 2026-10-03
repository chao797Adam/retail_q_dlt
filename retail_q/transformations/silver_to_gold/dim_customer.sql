-- Databricks notebook source
-- DBTITLE 1,Create Customer Dimension Table (Gold Layer)

CREATE OR REFRESH MATERIALIZED VIEW retail_q.gold.dim_customer
COMMENT "Gold layer customer dimension derived from Silver account table."
AS
SELECT
    account_name AS customer_id,
    account_name AS customer_name,
    type AS customer_type,
    billing_city,
    billing_state,
    billing_country,
    phone,
    website,
    industry,
    description
FROM retail_q.silver.account
WHERE account_name IS NOT NULL;