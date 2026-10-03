from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.silver.opportunity",
    comment="Salesforce opportunity data with core sales fields and data quality checks."
)
# Since there is no 'id' column, use 'opportunity_name' as the primary key for DQ check
@dp.expect_or_drop("non-null opportunity_name", "opportunity_name IS NOT NULL")
@dp.expect("valid amount", "amount IS NULL OR CAST(amount AS DOUBLE) >= 0")
@dp.expect("valid stage",
           "stage IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Negotiation/Review', 'Closed Won', 'Closed Lost')")
def opportunity_clean():
    # Read from the Salesforce Bronze table
    source_df = spark.readStream.table("retail_q.bronze.salesforce_oppo")
    
    # Select and rename columns based on actual schema, standardizing to lowercase
    return source_df.select(
        F.col("opportunity_name").alias("opportunity_name"),
        F.col("account_name").alias("account_name"),
        F.col("description").alias("description"),
        F.col("stage").alias("stage"),
        # Cast amount to double since it's stored as string in the CSV
        F.col("amount").cast("double").alias("amount"),
        # Business Logic: Classify deal size based on amount
        F.when(F.col("amount").cast("double") > 100000, "ENTERPRISE")
         .when(F.col("amount").cast("double") > 25000, "MID_MARKET")
         .otherwise("SMALL")
         .alias("deal_size"),
        F.col("type").alias("type"),
        F.col("lead_source").alias("lead_source"),
        F.col("close_date").alias("close_date")
    )