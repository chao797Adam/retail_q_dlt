from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.silver.inventory",
    comment="Silver layer inventory data with normalized columns and data quality checks."
)
# Data Quality Rules
@dp.expect_or_drop("non-null inventory_id", "inventory_id IS NOT NULL")
@dp.expect("valid stock_quantity", "stock_quantity > 0")
@dp.expect("non-null product_id", "product_id IS NOT NULL")
@dp.expect("non-null store_id", "store_id IS NOT NULL")
def inventory_clean():
    # Read from the Postgres Bronze table
    source_df = spark.readStream.table("retail_q.postgres_bronze.inventory")
    
    # Select and transform columns, including derived inventory_status
    return (
        source_df.select(
            F.col("inventory_id"),
            F.col("product_id"),
            F.col("store_id"),
            F.col("stock_quantity"),
            F.col("reorder_level"),
            # Business Logic: Flag as LOW_STOCK if quantity is below reorder level, otherwise HEALTHY
            F.when(
                F.col("stock_quantity") < F.col("reorder_level"),
                "LOW_STOCK"
            ).otherwise("HEALTHY").alias("inventory_status"),
            F.col("warehouse_location"),
            F.col("last_stock_update")
        )
    )