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

# Example: Process Excel-exported hierarchical data into parent-child relationships
from pyspark.sql.functions import col, lit, concat_ws, when

# Step 1: Load the pre-defined hierarchy data exported from Excel
HIERARCHY_CSV_PATH = "abfss://ws_fcp_enablement_felix@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/Aufbau Lucanet PowerBI.csv"  # Replace with actual path
df_hierarchy = spark.read.option("header", "true").option("sep", ";").csv(HIERARCHY_CSV_PATH)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_hierarchy)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 2: Create parent-child relationships using non-null hierarchy levels
parent_cols = ["Level_1", "Level_2", "Level_3", "Level_4"]  # Define the hierarchy columns

# Explode parent-child relationships by keeping the current and next columns
df_relationships = df_hierarchy.withColumn(
    "parent",
    concat_ws(" > ", *[col(c) for c in parent_cols[:-1]])  # Combine preceding levels as parent
).withColumn(
    "child",
    when(col("Level_4").isNotNull(), col("Level_4"))
    .when(col("Level_3").isNotNull(), col("Level_3"))
    .when(col("Level_2").isNotNull(), col("Level_2"))
    .when(col("Level_1").isNotNull(), col("Level_1"))
)

# Finalize structured output with only parent-child pairs
df_parent_child = df_relationships.select(
    col("parent").alias("Parent"),
    col("child").alias("Child")
).filter(col("Child").isNotNull())  # Exclude empty relationships

# Limit the results to a small sample for review
df_parent_child.limit(20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
