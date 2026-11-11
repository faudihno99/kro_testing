# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

import struct
from pyspark.sql.functions import udf, col, sha2
from pyspark.sql.types import StringType

# Custom Serialization Functions
def serialize_integer(value):
    # Serialize a 32-bit signed integer in Little-Endian
    return struct.pack('<i', value).hex()  # Convert to hex string

def serialize_long(value):
    # Serialize a 64-bit signed long in Little-Endian
    return struct.pack('<q', value).hex()  # Convert to hex string

def serialize_string(value):
    # Serialize a string to UTF-8 encoded bytes
    return value.encode('utf-8').hex()  # Convert to hex string


# UDFs for Serialization
serialize_integer_udf = udf(serialize_integer, StringType())
serialize_long_udf = udf(serialize_long, StringType())
serialize_string_udf = udf(serialize_string, StringType())

# Sample Data
data = [
    ("2147172661",),  # String that represents a number
    ("2103914247",),  # String that represents a number
    ("2103493395",),  # String that represents a number
]

# Create a DataFrame
df = spark.createDataFrame(data, ["zaehlernummer"])

# Case (a): Serialize as 32-bit integer and hash
df = df.withColumn(
    "serialized_int",
    serialize_integer_udf(col("zaehlernummer").cast("int"))  # Serialize to 32-bit integer
).withColumn(
    "hash_int",
    sha2(col("serialized_int"), 256)  # Hash using sha2
)

# Case (b): Serialize as 64-bit long and hash
df = df.withColumn(
    "serialized_long",
    serialize_long_udf(col("zaehlernummer").cast("long"))  # Serialize to 64-bit long
).withColumn(
    "hash_long",
    sha2(col("serialized_long"), 256)  # Hash using sha2
)

# Case (c): Serialize as UTF-8 string and hash
df = df.withColumn(
    "serialized_string",
    serialize_string_udf(col("zaehlernummer"))  # Serialize to string (UTF-8)
).withColumn(
    "hash_string",
    sha2(col("serialized_string"), 256)  # Hash using sha2
)

# Show Results
df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import struct
import hashlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, LongType

# Custom Serialization Function
def serialize_long_and_hash(value):
    """
    Serialize a long into a Little-Endian signed 64-bit representation
    and calculate the SHA-256 hash of that representation.
    """
    if value is None:  # Handle null/None values clearly
        return None
    
    try:
        # Serialize long value into bytes using struct.pack('<q')
        serialized_value = struct.pack('<q', value)
        # Calculate SHA-256 hash
        sha256_hash = hashlib.sha256(serialized_value).hexdigest()
        return sha256_hash
    except Exception as e:
        raise ValueError(f"Serialization or hashing failed for value {value}: {str(e)}")

# Register the Function as a UDF (User-Defined Function)
serialize_long_udf = udf(serialize_long_and_hash, StringType())

# Apply the UDF to the DataFrame
df = df.withColumn("hash_long", serialize_long_udf(df["zaehlernummer"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Data
df_customer = spark.read.format("delta").load("Tables/gold/dim_kundenstamm_sugar")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Remove empty values
df_cleaned = df_customer.filter((col("SAP_ID").isNotNull()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df_customer.count())
print(df_cleaned.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# upsert including deletions
def upsert_delta_table(target, source, primary_key):
    """
    Perform upsert operation on a Delta Table.

    Parameters: 
    target = target delta table to compare with the new data
    source = df consisting of the source data
    primary_key = Primary Key used to identify each row
    """
    (target.alias("target")
        .merge(
            source.alias("source"), f"target.{primary_key} = source.{primary_key}")
        .whenNotMatchedBySourceDelete()
        .execute())



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write DataFrame as Delta table if it doesn't exist
target_path = "Tables/gold/dim_kundenstamm_sugar"

# Merge into statement to load changes to target delta table since the last update.
prim_key = "SAP_ID"
delta_table = DeltaTable.forPath(spark, target_path)

upsert_delta_table(target=delta_table, source=df_cleaned, primary_key=prim_key)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
