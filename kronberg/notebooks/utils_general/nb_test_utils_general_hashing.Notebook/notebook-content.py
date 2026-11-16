# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Test notebook for the general hashing utilities
# Focus: unit tests for nb_utils_general_hashing
# Manual test (no CI)

import pyspark.sql.functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_utils_general_hashing

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_generate_sha2_hash_is_deterministic(spark):
    """
    Tests that the same input produces the same hash.

    - **What**: Validates determinism of 'generate_sha2_hash'.
    - **How**: Hashes the same column twice on identical DataFrames.
    - **Expectations**: Both hashes are equal.
    """
    df = spark.createDataFrame([("a", "b"), ("c", "d")], ["col_a", "col_b"])

    hashed_1 = df.select(generate_sha2_hash(["col_a", "col_b"]).alias("h")).collect()
    hashed_2 = df.select(generate_sha2_hash(["col_a", "col_b"]).alias("h")).collect()

    assert hashed_1 == hashed_2, "identical input must yield identical hashes"
    assert hashed_1[0]["h"] != hashed_1[1]["h"], "different rows must yield different hashes"

    print("test_generate_sha2_hash_is_deterministic passed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_generate_sha2_hash_column_order_irrelevant(spark):
    """
    Tests that the column order does not affect the hash.

    - **What**: Validates the internal column sorting of 'generate_sha2_hash'.
    - **How**: Hashes two columns in both orders on the same row.
    - **Expectations**: Both hashes are equal.
    """
    df = spark.createDataFrame([("a", "b")], ["col_a", "col_b"])

    row = df.select(
        generate_sha2_hash(["col_a", "col_b"]).alias("forward"),
        generate_sha2_hash(["col_b", "col_a"]).alias("reverse"),
    ).collect()[0]

    assert row["forward"] == row["reverse"], "column order must not affect the hash"

    print("test_generate_sha2_hash_column_order_irrelevant passed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_generate_sha2_hash_separator_prevents_collision(spark):
    """
    Tests that the separator prevents value-boundary shifts.

    - **What**: Validates the sentinel separator in 'generate_sha2_hash'.
    - **How**: Hashes ['a', 'bc'] and ['ab', 'c'], which concatenate to the same string.
    - **Expectations**: The two hashes differ.
    """
    df = spark.createDataFrame([("a", "bc", "ab", "c")], ["x1", "x2", "y1", "y2"])

    row = df.select(
        generate_sha2_hash(["x1", "x2"]).alias("split_first"),
        generate_sha2_hash(["y1", "y2"]).alias("split_second"),
    ).collect()[0]

    assert row["split_first"] != row["split_second"], \
        "boundary shift must not collide"

    print("test_generate_sha2_hash_separator_prevents_collision passed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_generate_sha2_hash_empty_column_list_raises(spark):
    """
    Tests fail-early behaviour on an empty column list.

    - **What**: Validates the input guard of 'generate_sha2_hash'.
    - **How**: Calls the function with an empty list.
    - **Expectations**: A ValueError is raised.
    """
    df = spark.createDataFrame([("a",)], ["col_a"])

    try:
        df.select(generate_sha2_hash([])).collect()
    except ValueError:
        print("test_generate_sha2_hash_empty_column_list_raises passed successfully")
        return

    raise AssertionError("empty column list must raise ValueError")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_add_hash_pk_and_add_hash_diff_share_logic(spark):
    """
    Tests that both wrappers produce the same hash for the same columns.

    - **What**: Validates identical hashing logic of 'add_hash_pk' and 'add_hash_diff'.
    - **How**: Applies both wrappers to the same DataFrame with equal column lists.
    - **Expectations**: hash_pk and hash_diff are equal per row.
    """
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])

    pk_df = add_hash_pk(df, ["id", "value"])
    diff_df = add_hash_diff(df, ["id", "value"])

    pk_rows = pk_df.select("id", "hash_pk").collect()
    diff_rows = diff_df.select("id", "hash_diff").collect()

    assert [r["hash_pk"] for r in pk_rows] == [r["hash_diff"] for r in diff_rows], \
        "add_hash_pk and add_hash_diff must use the same hashing logic"

    print("test_add_hash_pk_and_add_hash_diff_share_logic passed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_add_hash_diff_missing_column_raises(spark):
    """
    Tests fail-early behaviour on a missing column.

    - **What**: Validates the column guard of 'add_hash_diff'.
    - **How**: Requests a column that is not part of the DataFrame.
    - **Expectations**: A ValueError is raised.
    """
    df = spark.createDataFrame([(1,)], ["id"])

    try:
        add_hash_diff(df, ["id", "does_not_exist"])
    except ValueError:
        print("test_add_hash_diff_missing_column_raises passed successfully")
        return

    raise AssertionError("missing column must raise ValueError")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_add_hash_pk_empty_column_list_raises(spark):
    """
    Tests fail-early behaviour on an empty column list.

    - **What**: Validates the input guard of 'add_hash_pk'.
    - **How**: Calls the wrapper with an empty list.
    - **Expectations**: A ValueError is raised.
    """
    df = spark.createDataFrame([(1,)], ["id"])

    try:
        add_hash_pk(df, [])
    except ValueError:
        print("test_add_hash_pk_empty_column_list_raises passed successfully")
        return

    raise AssertionError("empty column list must raise ValueError")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_derive_hash_diff_columns_excludes_keys_and_metadata(spark):
    """
    Tests payload column derivation for change detection.

    - **What**: Validates 'derive_hash_diff_columns'.
    - **How**: Excludes the primary key plus metadata columns from a DataFrame.
    - **Expectations**: Only the payload columns remain, in DataFrame order.
    """
    df = spark.createDataFrame(
        [(1, "a", "b", "c", "d", "e")],
        ["id", "hash_pk", "hash_diff", "_change_type", "_load_timestamp", "value"],
    )

    result = derive_hash_diff_columns(
        df, ["id", "hash_pk", "hash_diff", "_change_type", "_load_timestamp"]
    )

    assert result == ["value"], f"expected ['value'], got {result}"

    print("test_derive_hash_diff_columns_excludes_keys_and_metadata passed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test execution
test_generate_sha2_hash_is_deterministic(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_generate_sha2_hash_column_order_irrelevant(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_generate_sha2_hash_separator_prevents_collision(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_generate_sha2_hash_empty_column_list_raises(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_add_hash_pk_and_add_hash_diff_share_logic(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_add_hash_diff_missing_column_raises(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_add_hash_pk_empty_column_list_raises(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_derive_hash_diff_columns_excludes_keys_and_metadata(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
