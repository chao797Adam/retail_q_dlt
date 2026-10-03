from pyspark.sql.functions import upper, trim, col
from pyspark import pipelines as dp

@dp.table(
    name="retail_q.gold.fact_sales",
    comment="Gold layer fact table joining transactions and opportunities."
)
def fact_sales():
    # 1. Read from Silver layer tables
    transactions_df = spark.read.table("retail_q.silver.transactions")
    opportunity_df = spark.read.table("retail_q.silver.opportunity")
    
    # 2. Join on opportunity_name (case-insensitive and trimmed to avoid mismatches)
    joined_df = transactions_df.alias("t").join(
        opportunity_df.alias("o"),
        upper(trim(col("t.opportunity_name"))) == upper(trim(col("o.opportunity_name"))),
        how="left"
    )
    
    # 3. Select and rename columns based on actual schema
    return joined_df.select(
        # --- From transactions (t) ---
        col("t.transaction_id"),
        col("t.opportunity_name"),
        col("t.product_id"),
        col("t.store_id"),
        col("t.quantity"),
        col("t.selling_price"),
        col("t.gross_amount"),
        col("t.discount_amount"),
        col("t.transaction_timestamp"),
        col("t.transaction_timestamp").cast("date").alias("transaction_date"),
        col("t.payment_mode"),
        col("t.sales_channel"),
        
        # --- From opportunity (o) ---
        col("o.stage"),           # Renamed from 'stage_name' to 'stage' to match schema
        col("o.amount"),          # Total opportunity amount
        col("o.deal_size"),       # Derived in Silver layer
        
        # --- Key mapping ---
        # Map 'account_name' to 'customer_id' to simulate a foreign key, 
        # enabling downstream JOINs with the customer dimension.
        col("o.account_name").alias("customer_id")
    )