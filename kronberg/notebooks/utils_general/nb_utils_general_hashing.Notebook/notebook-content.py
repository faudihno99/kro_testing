# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Import required packages
import pyspark.sql.functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Hashing Utils

# CELL ********************

def generate_sha2_hash(hash_input_columns: list[str]) -> F.Column:
    """
    Build the SHA-256 hash of one or more columns.

    Args:
        hash_input_columns (list[str]): Names of the columns to hash.

    Returns:
        F.Column: Binary SHA-256 hash over the string-concatenated input columns.
    """
    hash_input_columns_expr = [
        F.col(column).cast("string") for column in hash_input_columns
    ]
    return F.to_binary(
        F.sha2(F.concat_ws("", *hash_input_columns_expr), 256), F.lit("hex")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
