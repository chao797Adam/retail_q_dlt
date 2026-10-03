from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.silver.account",
    comment="Salesforce account data with core business columns and data quality checks."
)
# Note: Since there is no 'id' column in the source table, 'account_name' is used as the primary key for the DQ check.
@dp.expect_or_drop("non-null account_name", "account_name IS NOT NULL")
@dp.expect("valid type", 
           "type IS NULL OR type IN ('Customer - Channel', 'Customer - Direct', 'Other')") # Adjust based on actual data, comment out if unsure
def account_clean():
    # Read from the Salesforce Bronze table
    source_df = spark.readStream.table("retail_q.bronze.salesforce_acc")
    
    # Select and rename columns based on the actual schema, standardizing to lowercase
    return source_df.select(
        F.col("account_name").alias("account_name"),
        F.col("type").alias("type"),
        F.col("phone").alias("phone"),
        F.col("website").alias("website"),
        F.col("billing_city").alias("billing_city"),
        F.col("billing_state_province").alias("billing_state"), # Simplified from the original 'billing_state_province'
        F.col("billing_country").alias("billing_country"),
        F.col("industry").alias("industry"),
        F.col("description").alias("description")
    )