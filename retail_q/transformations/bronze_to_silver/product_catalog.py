from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.silver.product_catalog",
    comment="Silver layer product catalog with standardized data and data quality rules."
)
# Data Quality Rules
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0")
@dp.expect_or_drop("valid_product_name", "product_name IS NOT NULL AND LENGTH(TRIM(product_name)) > 0")
@dp.expect("valid_category", "category IS NOT NULL")
@dp.expect("valid_price", "unit_price > 0")
@dp.expect_or_drop("valid_launch_date", "launch_date IS NOT NULL")
@dp.expect("valid_supplier", "supplier_name IS NOT NULL")
def product_catalog():
    return (
        # Read from the Postgres Bronze table
        spark.readStream.table("retail_q.postgres_bronze.product_catalog")
        .select(
            # Standardize product_id: trim and uppercase
            F.upper(F.trim(F.col("product_id"))).alias("product_id"),
            
            # Standardize product_name: trim and title case
            F.initcap(F.trim(F.col("product_name"))).alias("product_name"),
            
            # Standardize category: trim and title case
            F.initcap(F.trim(F.col("category"))).alias("category"),
            
            # Standardize subcategory: trim and title case, handle nulls
            F.when(F.col("subcategory").isNotNull(), 
                   F.initcap(F.trim(F.col("subcategory"))))
             .otherwise(F.lit("Unknown")).alias("subcategory"),
            
            # Standardize brand: trim and title case, handle nulls
            F.when(F.col("brand").isNotNull(), 
                   F.initcap(F.trim(F.col("brand"))))
             .otherwise(F.lit("Unknown")).alias("brand"),
            
            # Standardize price: round to 2 decimal places
            F.round(F.col("unit_price"), 2).alias("unit_price"),
            
            # Standardize supplier_name: trim and title case
            F.initcap(F.trim(F.col("supplier_name"))).alias("supplier_name"),
            
            # Keep dates and timestamps as-is
            F.col("launch_date"),
            
            # Derive product_segment based on unit_price
            F.when(F.col("unit_price") > 50000, "PREMIUM")
             .when(F.col("unit_price") > 10000, "MID_RANGE")
             .otherwise("BUDGET")
             .alias("product_segment"),
            
            # Keep CDC tracking columns with correct names
            F.col("__START_AT").alias("start_at"),
            F.col("__END_AT").alias("end_at"),
            
            # Derive is_active from end_at: null means current/active record
            F.when(F.col("__END_AT").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_active"),
            
            # Keep updated_at timestamp
            F.col("updated_at"),
            
            # Add processing timestamp for audit trail
            F.current_timestamp().alias("processed_at")
        )
    )