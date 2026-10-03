from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.silver.transactions",
    comment="Silver layer transaction data with type casting and data quality checks."
)
# Data Quality Rules (Note: DQ rules apply AFTER type casting in Spark, but written here as metadata)
@dp.expect_or_drop("non-null transaction_id", "transaction_id IS NOT NULL")
@dp.expect("valid quantity", "quantity > 0")
@dp.expect("valid selling_price", "selling_price >= 0")
@dp.expect("valid discount_amount", "discount_amount >= 0")
@dp.expect("non-null product_id", "product_id IS NOT NULL")
@dp.expect("valid payment_mode",
           "payment_mode IN ('UPI', 'Card', 'Cash', 'Net Banking')")
def transactions_clean():
    # Read from the Bronze table as a stream (to handle incremental data)
    # Note: Changed from 'read' to 'readStream' and corrected the table path.
    source_df = spark.readStream.table("retail_q.bronze.blob_transactions")
    
    # Select and cast columns based on actual schema
    return source_df.select(
        F.col("transaction_id"),
        F.col("opportunity_name"),
        F.col("product_id"),
        F.col("store_id"),
        # Cast string columns to numeric types
        F.col("quantity").cast("int").alias("quantity"),
        F.col("selling_price").cast("double").alias("selling_price"),
        # Business Logic: Calculate gross_amount on the fly
        (F.col("quantity").cast("int") * F.col("selling_price").cast("double")).alias("gross_amount"),
        F.col("discount_amount").cast("double").alias("discount_amount"),
        # Parse the timestamp string into a proper timestamp type
        # ⚠️ Warning: This format might not match your actual data! Check first.
        F.to_timestamp(F.col("transaction_timestamp"), "dd-MMM-yyyy hh.mm.ss a").alias("transaction_timestamp"),
        F.col("payment_mode"),
        F.col("sales_channel")
    )