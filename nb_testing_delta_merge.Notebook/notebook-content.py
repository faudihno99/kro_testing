# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "69120f60-2c6f-4fd6-a0f2-f1776e95a434",
# META       "default_lakehouse_name": "lh",
# META       "default_lakehouse_workspace_id": "98c91999-02e5-4d24-8f5f-5b95d409608e",
# META       "known_lakehouses": [
# META         {
# META           "id": "69120f60-2c6f-4fd6-a0f2-f1776e95a434"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit, sha2, concat
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define schema for testing data
schema = StructType([
    StructField("primary_key", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initial data to create dataframe
data = [
    (1, "Alice", 25, "New York"),
    (2, "Bob", 30, "Los Angeles"),
    (3, "Charlie", 35, "San Francisco")
]

# Create initial dataframe
df_initial = spark.createDataFrame(data, schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create initial dataframe
df_initial = spark.createDataFrame(data, schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add hash_diff column using SHA-256 for change detection
df_with_hash = df_initial.withColumn("hash_diff", sha2(concat("name", "age", "city"), 256))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write the dataframe as a Delta table
DELTA_TABLE_PATH = "Tables/testing_upsert/upsert_table"
df_with_hash.write.format("delta").mode("overwrite").save(DELTA_TABLE_PATH)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Testing Upsert**

# CELL ********************

# Simulating changes: 1) Modify a row, 2) Add a new row
updated_data = [
    (1, "Alice", 26, "New York"),  # Change in the 'age'
    (2, "Bob", 30, "Los Angeles"),
    (3, "Charlie", 35, "San Francisco"),
    (4, "Diana", 28, "Seattle")   # New row
]

# Create the updated dataframe
df_updated = spark.createDataFrame(updated_data, schema)

# Add updated hash_diff column using SHA-256
df_with_updated_hash = df_updated.withColumn("hash_diff", sha2(concat("name", "age", "city"), 256))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Upsert Operation
def upsert_delta(source_df, delta_table, hk_sid):
    # Merge: update all columns for matches, insert all columns for new keys
    (delta_table.alias("target")
    .merge(
        source_df.alias("source"),
        f"target.{hk_sid} = source.{hk_sid}"
    )
    .whenMatchedUpdateAll(condition="target.hash_diff != source.hash_diff")
    .whenNotMatchedInsertAll()
    .execute())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if DeltaTable.isDeltaTable(spark, DELTA_TABLE_PATH):
    # Define Delta Table
    deltatable = DeltaTable.forPath(spark, DELTA_TABLE_PATH)

    # Perform Upsert based on defined function
    upsert_delta(source_df=df_with_updated_hash, delta_table=deltatable, hk_sid="primary_key")
    print(f"Upsert completed at {DELTA_TABLE_PATH}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
