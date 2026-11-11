# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("binary_hash_test").getOrCreate()

# Create sample DataFrame with 3 rows
df = spark.createDataFrame([
    ("John", "Doe", 25),
    ("Jane", "Smith", 30),
    ("Bob", "Johnson", 35)
], ["first_name", "last_name", "age"])

print("Sample DataFrame:")
df.show()

# Test both methods
df_result = df.withColumn(
    "hash_to_binary", 
    F.to_binary(F.sha2(F.concat_ws("", F.col("first_name"), F.col("last_name"), F.col("age").cast("string")), 256), F.lit("hex"))
).withColumn(
    "hash_unhex", 
    F.unhex(F.sha2(F.concat_ws("", F.col("first_name"), F.col("last_name"), F.col("age").cast("string")), 256))
).withColumn(
    "hashes_equal", 
    F.col("hash_to_binary") == F.col("hash_unhex")
)

print("\nComparison Results:")
df_result.select("first_name", "last_name", "hashes_equal").show()

print("\nHex representations:")
df_result.withColumn("hex_to_binary", F.hex(F.col("hash_to_binary"))).withColumn("hex_unhex", F.hex(F.col("hash_unhex"))).select("first_name", "hex_to_binary", "hex_unhex").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
