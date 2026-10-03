-- Databricks notebook source
-- DBTITLE 1,Create Product Dimension Table (Gold Layer)

CREATE OR REFRESH MATERIALIZED VIEW retail_q.gold.dim_product
COMMENT "Gold layer product dimension derived from Silver product catalog."
AS
SELECT
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    product_segment,
    unit_price,
    supplier_name,
    launch_date,
    updated_at
FROM retail_q.silver.product_catalog
WHERE is_active = true;