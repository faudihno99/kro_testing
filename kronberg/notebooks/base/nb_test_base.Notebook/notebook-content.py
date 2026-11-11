# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Test notebook for the Base layer
# Focus: Unit tests for all utility functions from nb_utils_base and process_base()
# Manual test (no CI)

import json
from unittest.mock import patch, MagicMock
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, DoubleType
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run utils_delta_handler

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run utils_base

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_process_base_filters_update_preimage(spark):
    """
    Verifies that for CDC with update_preimage + update_postimage for the same PK,
    update_preimage is filtered out before row_number() performs the tie-break.
    """
    schema = StructType([
        StructField("voucher_no", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("_change_type", StringType(), True),
        StructField("_commit_timestamp", TimestampType(), True),
        StructField("_commit_version", LongType(), True),
    ])

    ts = datetime(2024, 1, 15, 10, 30, 0)

    data = [
        ("1128004", "100.00", "update_preimage",  ts, 5),
        ("1128004", "150.00", "update_postimage", ts, 5),
        ("1128005", "200.00", "insert",           ts, 5),
        ("1128006", "50.00",  "delete",           ts, 5),
    ]

    source_df = spark.createDataFrame(data, schema)

    config = {
        "source": {
            "lakehouse_name": "test_lh",
            "schema_name": "test_schema",
            "table_name": "source_table"
        },
        "target": {
            "lakehouse_name": "test_lh",
            "schema_name": "test_schema",
            "table_name": "target_table"
        },
        "target_primary_keys": ["voucher_no"],
        "column_mapping": {
            "voucher_no": "voucher_no",
            "amount": "amount"
        }
    }

    with patch('__main__.DeltaLakeHandler') as MockHandler:
        mock_handler = MagicMock()
        mock_handler.delta_table_exists.return_value = True

        target_df = spark.createDataFrame([("dummy",)], ["voucher_no"])
        mock_handler.target_delta_table.toDF.return_value = target_df
        mock_handler.target_delta_table.toDF().isEmpty.return_value = False

        mock_handler.get_last_commit_timestamp.return_value = ts
        mock_handler.read_delta_lake_table.return_value = source_df

        MockHandler.return_value = mock_handler

        result = process_base(spark, config, use_cdc=True)

        call_kwargs = mock_handler.write_to_delta_lake.call_args[1]
        written_df = call_kwargs["df"]

        change_types = [row._change_type for row in written_df.collect()]
        assert "update_preimage" not in change_types, \
            f"update_preimage must not be in final DataFrame, found: {change_types}"

        pk1_rows = written_df.filter(F.col("voucher_no") == "1128004").collect()
        assert len(pk1_rows) == 1, f"PK 1128004 must appear exactly once, found: {len(pk1_rows)}"
        assert pk1_rows[0]["amount"] == "150.00", \
            f"PK 1128004 must have amount='150.00' (postimage), was: {pk1_rows[0]['amount']}"

        total_rows = len(written_df.collect())
        assert total_rows == 3, \
            f"Expected 3 rows (postimage + insert + delete), was: {total_rows}"

        print("test_process_base_filters_update_preimage PASSED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_unnest_jira_comments(spark):
    """
    Tests the flattening of nested Jira comments from a JSON string into individual rows.

    - **What**: Validates the 'unnest_jira_comments' utility.
    - **How**: Mocks a DataFrame with a JSON string containing multiple comments (author, body, timestamps).
    - **Expectations**: The output DataFrame should contain one row per comment, with flattened
      metadata such as 'comments_author_name' and 'comments_body' accurately extracted.
    """
    sample_comments = [
        {
            "self": "https://example.com/rest/api/2/issue/111/comment/1",
            "id": "1",
            "author": {
                "self": "https://example.com/rest/api/2/user?username=user1",
                "name": "user1",
                "key": "user1",
                "emailAddress": "user1@example.com",
                "avatarUrls": {
                    "48x48": "url1",
                    "24x24": "url2",
                    "16x16": "url3",
                    "32x32": "url4"
                },
                "displayName": "User One",
                "active": True,
                "timeZone": "Europe/Berlin"
            },
            "body": "Sample body 1",
            "updateAuthor": {
                "self": "https://example.com/rest/api/2/user?username=user1",
                "name": "user1",
                "key": "user1",
                "emailAddress": "user1@example.com",
                "avatarUrls": {
                   "48x48": "url1",
                   "24x24": "url2",
                   "16x16": "url3",
                   "32x32": "url4"
                },
                "displayName": "User One",
                "active": True,
                "timeZone": "Europe/Berlin"
            },
            "created": "2024-01-01T10:00:00.000+0000",
            "updated": "2024-01-01T10:00:00.000+0000"
        },
        {
            "self": "https://example.com/rest/api/2/issue/111/comment/2",
            "id": "2",
            "author": {
                "self": "https://example.com/rest/api/2/user?username=user2",
                "name": "user2",
                "key": "user2",
                "emailAddress": "user2@example.com",
                "avatarUrls": {
                    "48x48": "url1",
                    "24x24": "url2",
                    "16x16": "url3",
                    "32x32": "url4"
                },
                "displayName": "User Two",
                "active": True,
                "timeZone": "Europe/Berlin"
            },
            "body": "Sample body 2",
            "updateAuthor": {
                "self": "https://example.com/rest/api/2/user?username=user2",
                "name": "user2",
                "key": "user2",
                "emailAddress": "user2@example.com",
                "avatarUrls": {
                   "48x48": "url1",
                   "24x24": "url2",
                   "16x16": "url3",
                   "32x32": "url4"
                },
                "displayName": "User Two",
                "active": True,
                "timeZone": "Europe/Berlin"
            },
            "created": "2024-01-02T10:00:00.000+0000",
            "updated": "2024-01-02T10:00:00.000+0000"
        }
    ]

    schema = StructType([
        StructField("issue_key", StringType(), True),
        StructField("comments", StringType(), True)
    ])

    data = [
        ("PROJECT-123", json.dumps(sample_comments))
    ]

    df = spark.createDataFrame(data, schema)
    result_df = unnest_jira_comments(df, ["comments"], {})

    assert result_df.count() == 2, f"Expected 2 rows, got {result_df.count()}"

    results = result_df.collect()

    row1 = [r for r in results if r["comments_id"] == "1"][0]
    assert row1["issue_key"] == "PROJECT-123", "Issue key mismatch"
    assert row1["comments_author_name"] == "user1", "Author name mismatch"
    assert row1["comments_author_displayName"] == "User One", "Display name mismatch"
    assert row1["comments_author_emailAddress"] == "user1@example.com", "Email mismatch"
    assert row1["comments_author_active"] == "true", "Active flag mismatch"
    assert row1["comments_body"] == "Sample body 1", "Body mismatch"

    row2 = [r for r in results if r["comments_id"] == "2"][0]
    assert row2["comments_body"] == "Sample body 2", "Body mismatch"

    print("test_unnest_jira_comments passed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_replace_values(spark):
    """
    Tests the standard key-value mapping utility.

    - **What**: Validates 'replace_values' for direct one-to-one replacements.
    - **How**: Creates a DataFrame with category codes and applies a mapping dictionary.
    - **Expectations**: A new column with the suffix '_mapped' is created. Mapped values (e.g., 'A' -> 'Apple')
      are updated, while unmapped values (e.g., 'C') are preserved from the source column.
    """
    data = [
        (101, "A"),
        (102, "B"),
        (103, "C")
    ]
    schema = ["id", "category"]
    df = spark.createDataFrame(data, schema)

    params = {
        "mapping": {
            "A": "Apple",
            "B": "Banana"
        }
    }

    result_df = replace_values(df, ["category"], params)

    assert "category_mapped" in result_df.columns, "Output suffix '_mapped' missing"

    results = result_df.sort("id").collect()
    assert results[0]["category_mapped"] == "Apple", "Value mapping failed for A"
    assert results[1]["category_mapped"] == "Banana", "Value mapping failed for B"
    assert results[2]["category_mapped"] == "C", "Default value preservation failed (coalesce)"

    print("test_replace_values passed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_regex_replace_values(spark):
    """
    Tests the sequential regular expression replacement utility.

    - **What**: Validates 'regex_replace_values' for pattern-based string cleaning.
    - **How**: Applies a sequence of regex mappings to a semicolon-separated tags column.
    - **Expectations**: Each matching pattern is replaced (e.g., 'College' -> 'Uni').
      The resulting '_mapped' column reflects the cumulative string transformations.
    """
    data = [
        (1, "College;DirectCustomer"),
        (2, "Competitor;College"),
        (3, "Unknown")
    ]
    schema = ["id", "tags"]
    df = spark.createDataFrame(data, schema)

    params = {
        "mapping": {
            "College": "Uni",
            "DirectCustomer": "Direct"
        }
    }

    result_df = regex_replace_values(df, ["tags"], params)

    assert "tags_mapped" in result_df.columns, "Output suffix '_mapped' missing"

    results = result_df.sort("id").collect()
    assert results[0]["tags_mapped"] == "Uni;Direct", "Regex mapping failed for Row 1"
    assert results[1]["tags_mapped"] == "Competitor;Uni", "Regex mapping failed for Row 2"
    assert results[2]["tags_mapped"] == "Unknown", "Value preservation failed for Unknown"

    print("test_regex_replace_values passed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_apply_dach_cost_center_reduction(spark):
    """
    Tests the DACH-specific cost center 464 reduction.

    - **What**: Validates that rows with cost_center_number == 464 have their
      amount columns reduced by 50 %.
    - **Expectations**: Other cost centers stay unchanged, NULL remains NULL,
      and zero remains zero.
    """
    schema = StructType([
        StructField("voucher_no", StringType(), True),
        StructField("cost_center_number", IntegerType(), True),
        StructField("debit_local", DoubleType(), True),
        StructField("credit_local", DoubleType(), True),
    ])

    data = [
        ("V001", 464, 100.0, 200.0),
        ("V002", 123, 100.0, 200.0),
        ("V003", 464, None, 50.0),
        ("V004", 464, 0.0, 0.0),
    ]

    df = spark.createDataFrame(data, schema)

    result_df = apply_dach_cost_center_reduction(
        df,
        column_names=["debit_local", "credit_local"],
        params={
            "cost_center_col": "cost_center_number",
            "cost_center_value": 464,
            "factor": 0.5,
        }
    )

    rows = {r["voucher_no"]: r.asDict() for r in result_df.collect()}

    assert rows["V001"]["debit_local"] == 50.0, "464 debit reduction failed"
    assert rows["V001"]["credit_local"] == 100.0, "464 credit reduction failed"

    assert rows["V002"]["debit_local"] == 100.0, "non-464 debit was changed"
    assert rows["V002"]["credit_local"] == 200.0, "non-464 credit was changed"

    assert rows["V003"]["debit_local"] is None, "NULL debit on 464 should remain NULL"
    assert rows["V003"]["credit_local"] == 25.0, "464 credit reduction failed"

    assert rows["V004"]["debit_local"] == 0.0, "zero debit on 464 should remain zero"
    assert rows["V004"]["credit_local"] == 0.0, "zero credit on 464 should remain zero"

    print("test_apply_dach_cost_center_reduction passed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_concat_columns(spark):
    """
    Tests the string concatenation utility for multiple columns.

    - **What**: Validates 'concat_columns' using a specified separator.
    - **How**: Combines 'first_name' and 'last_name' with a space separator.
    - **Expectations**: Creates 'full_name'. Successfully handles NULL values by ignoring
      them in the concatenation (e.g., 'Jane' + NULL -> 'Jane').
    """
    data = [("John", "Doe"), ("Jane", None)]
    schema = ["first_name", "last_name"]
    df = spark.createDataFrame(data, schema)

    params = {
        "input_cols": ["first_name", "last_name"],
        "separator": " ",
        "output_col": "full_name"
    }

    result_df = concat_columns(df, params)
    rows = result_df.collect()

    assert rows[0]["full_name"] == "John Doe", f"Expected 'John Doe', got {rows[0]['full_name']}"
    assert rows[1]["full_name"] == "Jane", f"Expected 'Jane', got {rows[1]['full_name']}"

    print("test_concat_columns passed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_apply_lookup(spark):
    """
    Tests the join-based lookup utility using external CSV data.

    - **What**: Validates 'apply_lookup' for mapping codes to names (e.g., ISO codes).
    - **How**: Writes a temporary CSV to the local environment and performs a left-join.
    - **Expectations**: Mapped fields (e.g., 'DE' -> 'Germany') are added to the result.
      The utility should drop temporary join columns and cleanup intermediate data.
    """
    import os
    import shutil

    path = "test_lookup_dir"
    if os.path.exists(path):
        shutil.rmtree(path)

    lookup_data = [("DE", "Germany"), ("US", "United States")]
    lookup_df = spark.createDataFrame(lookup_data, ["iso", "name"])
    lookup_df.coalesce(1).write.option("header", "true").option("sep", ";").csv(path)

    source_data = [("DE",), ("FR",)]
    source_df = spark.createDataFrame(source_data, ["country_code"])

    params = {
        "path": path,
        "join_col_source": "country_code",
        "join_col_lookup": "iso",
        "target_cols": {"name": "country_mapped"}
    }

    result_df = apply_lookup(source_df, params)
    rows = result_df.collect()

    de_row = [r for r in rows if r["country_code"] == "DE"][0]
    assert de_row["country_mapped"] == "Germany", f"Expected 'Germany', got {de_row['country_mapped']}"

    fr_row = [r for r in rows if r["country_code"] == "FR"][0]
    assert fr_row["country_mapped"] is None, f"Expected None, got {fr_row['country_mapped']}"

    if os.path.exists(path):
        shutil.rmtree(path)

    print("test_apply_lookup passed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_apply_delta_lookup_success(spark):
    """
    Tests the Delta table join-based lookup utility for column derivation.

    - **What**: Validates 'apply_delta_lookup' for mapping source keys to derived columns.
    - **How**: Mocks spark.read.format().table() to return an in-memory mapping DataFrame,
      avoiding any real Delta table dependency.
    - **Expectations**: The derived column 'account_group_name' is correctly populated for all rows
      that have a match. The assert inside the function passes without error.
    """

    source_data = [(1001, "VCH-001"), (1002, "VCH-002")]
    source_df = spark.createDataFrame(source_data, ["account_id", "voucher_number"])

    mapping_data = [(1001, "Gruppe A"), (1002, "Gruppe B")]
    mapping_df = spark.createDataFrame(mapping_data, ["account_id", "account_group_name"])

    params = {
        "database": "lh_fabric_1",
        "schema": "base",
        "table": "sharepoint_mapping_vietnam",
        "join_col_source": "account_id",
        "join_col_lookup": "account_id",
        "target_cols": {"account_group_name": "account_group_name"}
    }

    mock_reader = MagicMock()
    mock_reader.table.return_value = mapping_df
    mock_format = MagicMock(return_value=mock_reader)

    with patch.object(spark, "read", create=True) as mock_read:
        mock_read.format = mock_format
        result_df = apply_delta_lookup(source_df, params)

    assert "account_group_name" in result_df.columns, "Derived column 'account_group_name' missing"
    assert "account_id" not in result_df.columns, "Mapping join key 'account_id' should be dropped (left join keeps left key)"

    rows = result_df.sort("account_id").collect()
    assert rows[0]["account_group_name"] == "Gruppe A", f"Expected 'Gruppe A', got {rows[0]['account_group_name']}"
    assert rows[1]["account_group_name"] == "Gruppe B", f"Expected 'Gruppe B', got {rows[1]['account_group_name']}"

    print("test_apply_delta_lookup_success passed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_apply_delta_lookup_fails_on_unmatched(spark):
    """
    Tests that apply_delta_lookup raises an AssertionError when a source row has no mapping match.

    - **What**: Validates the NULL-check assert inside 'apply_delta_lookup'.
    - **How**: Source contains account 9999 which is absent from the mapping table.
    - **Expectations**: An AssertionError is raised describing the unmatched column and count.
    """

    source_data = [(1001, "VCH-001"), (9999, "VCH-999")]
    source_df = spark.createDataFrame(source_data, ["account_id", "voucher_number"])

    mapping_data = [(1001, "Gruppe A")]
    mapping_df = spark.createDataFrame(mapping_data, ["account_id", "account_group_name"])

    params = {
        "database": "lh_fabric_1",
        "schema": "base",
        "table": "sharepoint_mapping_vietnam",
        "join_col_source": "account_id",
        "join_col_lookup": "account_id",
        "target_cols": {"account_group_name": "account_group_name"}
    }

    mock_reader = MagicMock()
    mock_reader.table.return_value = mapping_df
    mock_format = MagicMock(return_value=mock_reader)

    error_raised = False
    with patch.object(spark, "read", create=True) as mock_read:
        mock_read.format = mock_format
        try:
            apply_delta_lookup(source_df, params)
        except AssertionError as e:
            error_raised = True
            assert "account_group_name" in str(e), "Error message should mention the affected column"
            assert "1 NULL" in str(e), "Error message should report 1 unmatched row"

    assert error_raised, "Expected AssertionError for unmatched mapping row, but none was raised"

    print("test_apply_delta_lookup_fails_on_unmatched passed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_map_array_values(spark):
    """
    Tests the array split/map/join utility.

    - **What**: Validates 'map_array_values' for splitting delimited strings, mapping elements,
      and optionally joining back to a string.
    - **How**: Creates DataFrames with various input formats (JSON arrays, plain strings, nulls).
    - **Expectations**: JSON brackets/quotes are stripped; mapped values are substituted;
      empty/whitespace-only results return null; duplicate values are deduplicated;
      finalColumn_string controls string vs array output.
    """
    test_mapping = {
        "101": "Allgemein",
        "102": "Automobilbau",
        "103": "Bergbau"
    }

    data1 = [(1, '["101","102"]')]
    df1 = spark.createDataFrame(data1, ["id", "industry"])
    result1 = map_array_values(df1, ["industry"], {"mapping": test_mapping}).collect()[0]
    assert result1["industry_mapped"] == "Allgemein, Automobilbau", \
        f"JSON array test failed: {result1['industry_mapped']}"

    data2 = [(1, "101,102")]
    df2 = spark.createDataFrame(data2, ["id", "industry"])
    result2 = map_array_values(df2, ["industry"], {"mapping": test_mapping}).collect()[0]
    assert result2["industry_mapped"] == "Allgemein, Automobilbau", \
        f"Plain string test failed: {result2['industry_mapped']}"

    data3 = [(1, "101,999")]
    df3 = spark.createDataFrame(data3, ["id", "industry"])
    result3 = map_array_values(df3, ["industry"], {"mapping": test_mapping}).collect()[0]
    assert result3["industry_mapped"] == "Allgemein, 999", \
        f"Mixed mapping test failed: {result3['industry_mapped']}"

    data4 = [(1, "101,102")]
    df4 = spark.createDataFrame(data4, ["id", "industry"])
    result4 = map_array_values(df4, ["industry"], {"mapping": test_mapping, "finalColumn_string": False}).collect()[0]
    assert result4["industry_mapped"] == ["Allgemein", "Automobilbau"], \
        f"Array output test failed: {result4['industry_mapped']}"

    data5 = [(1, "101,, 102")]
    df5 = spark.createDataFrame(data5, ["id", "industry"])
    result5 = map_array_values(df5, ["industry"], {"mapping": test_mapping}).collect()[0]
    assert result5["industry_mapped"] == "Allgemein, Automobilbau", \
        f"Empty element filter test failed: {result5['industry_mapped']}"

    data6 = [(1, ", ,")]
    df6 = spark.createDataFrame(data6, ["id", "industry"])
    result6 = map_array_values(df6, ["industry"], {"mapping": test_mapping}).collect()[0]
    assert result6["industry_mapped"] is None, \
        f"All-empty->null test failed: {result6['industry_mapped']}"

    data7 = [(1, None)]
    schema7 = StructType([
        StructField("id", IntegerType(), True),
        StructField("industry", StringType(), True)
    ])
    df7 = spark.createDataFrame(data7, schema7)
    result7 = map_array_values(df7, ["industry"], {"mapping": test_mapping}).collect()[0]
    assert result7["industry_mapped"] is None, \
        f"Null input test failed: {result7['industry_mapped']}"

    data8 = [(1, "101")]
    df8 = spark.createDataFrame(data8, ["id", "industry"])
    result8 = map_array_values(df8, ["industry"], {"mapping": test_mapping}).collect()[0]
    assert result8["industry_mapped"] == "Allgemein", \
        f"Single element test failed: {result8['industry_mapped']}"

    data9 = [(1, "101", "102")]
    df9 = spark.createDataFrame(data9, ["id", "col_a", "col_b"])
    result9 = map_array_values(df9, ["col_a", "col_b"], {"mapping": test_mapping}).collect()[0]
    assert result9["col_a_mapped"] == "Allgemein", \
        f"Multiple cols test failed (col_a): {result9['col_a_mapped']}"
    assert result9["col_b_mapped"] == "Automobilbau", \
        f"Multiple cols test failed (col_b): {result9['col_b_mapped']}"

    data10 = [(1, "101,102")]
    df10 = spark.createDataFrame(data10, ["id", "industry"])
    result10 = map_array_values(df10, ["industry"], {"mapping": {}}).collect()[0]
    assert "industry_mapped" not in result10, \
        "Empty mapping test failed: column should not exist"

    data11 = [(1, "101 , 102")]
    df11 = spark.createDataFrame(data11, ["id", "industry"])
    result11 = map_array_values(df11, ["industry"], {"mapping": test_mapping}).collect()[0]
    assert result11["industry_mapped"] == "Allgemein, Automobilbau", \
        f"Whitespace delimiter test failed: {result11['industry_mapped']}"

    data12 = [(1, "101,101,102")]
    df12 = spark.createDataFrame(data12, ["id", "industry"])
    result12 = map_array_values(df12, ["industry"], {"mapping": test_mapping}).collect()[0]
    assert result12["industry_mapped"] == "Allgemein, Automobilbau", \
        f"Deduplication string test failed: {result12['industry_mapped']}"

    data13 = [(1, "101,101,102")]
    df13 = spark.createDataFrame(data13, ["id", "industry"])
    result13 = map_array_values(df13, ["industry"], {"mapping": test_mapping, "finalColumn_string": False}).collect()[0]
    assert result13["industry_mapped"] == ["Allgemein", "Automobilbau"], \
        f"Deduplication array test failed: {result13['industry_mapped']}"

    print("test_map_array_values passed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_apply_date_format_standard(spark):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("date", StringType(), True),
    ])

    data = [
        (1, "13.02.2026"),
        (2, "2026-02-13"),
        (3, "2026-02-13 00:00:00"),
        (4, "2026-09-28 00:00:00"),
        (5, None),
    ]

    df = spark.createDataFrame(data, schema)

    params = {
        "source_column": "date",
        "target_column": "date_formatted",
        "formats": {
            "dd.MM.yyyy": "^\\d{2}\\.\\d{2}\\.\\d{4}$",
            "yyyy-MM-dd": "^\\d{4}-\\d{2}-\\d{2}$",
            "yyyy-MM-dd HH:mm:ss": "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$",
        },
    }

    result_df = apply_date_format(df, ["date_formatted"], params)

    rows = {r["id"]: r.asDict() for r in result_df.collect()}

    assert str(rows[1]["date_formatted"]) == "2026-02-13", f"dd.MM.yyyy parse failed: {rows[1]['date_formatted']}"
    assert str(rows[2]["date_formatted"]) == "2026-02-13", f"yyyy-MM-dd parse failed: {rows[2]['date_formatted']}"
    assert str(rows[3]["date_formatted"]) == "2026-02-13", f"yyyy-MM-dd HH:mm:ss parse failed: {rows[3]['date_formatted']}"
    assert str(rows[4]["date_formatted"]) == "2026-09-28", f"timestamp parse failed: {rows[4]['date_formatted']}"
    assert rows[5]["date_formatted"] is None, "Null source should give null target"

    print("test_apply_date_format_standard passed successfully")


def test_apply_date_format_aborts_on_invalid(spark):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("date", StringType(), True),
    ])

    data = [(1, "not-a-date")]
    df = spark.createDataFrame(data, schema)

    params = {
        "source_column": "date",
        "target_column": "date_formatted",
        "formats": {
            "dd.MM.yyyy": "^\\d{2}\\.\\d{2}\\.\\d{4}$",
            "yyyy-MM-dd": "^\\d{4}-\\d{2}-\\d{2}$",
        },
    }

    error_raised = False
    try:
        apply_date_format(df, ["date_formatted"], params)
    except ValueError as e:
        error_raised = True
        assert "not-a-date" in str(e), "Error must mention unparseable value"
        assert "date_formatted" in str(e), "Error must mention target column"

    assert error_raised, "Expected ValueError for unparseable date"

    print("test_apply_date_format_aborts_on_invalid passed successfully")


def test_apply_date_format_missing_source(spark):
    error_raised = False
    try:
        apply_date_format(spark.createDataFrame([], StructType([])), ["date_formatted"], {"target_column": "date_formatted", "formats": {"dd.MM.yyyy": "\\d{2}"}})
    except ValueError as e:
        error_raised = True
        assert "source_column" in str(e)
    assert error_raised, "Expected ValueError for missing source_column"
    print("test_apply_date_format_missing_source passed successfully")


def test_apply_date_format_missing_formats(spark):
    error_raised = False
    try:
        apply_date_format(spark.createDataFrame([], StructType([])), ["date_formatted"], {"source_column": "date"})
    except ValueError as e:
        error_raised = True
        assert "formats" in str(e)
    assert error_raised, "Expected ValueError for missing formats"
    print("test_apply_date_format_missing_formats passed successfully")


def test_apply_date_format_formats_not_dict(spark):
    error_raised = False
    try:
        apply_date_format(spark.createDataFrame([], StructType([])), ["date_formatted"], {"source_column": "date", "formats": ["dd.MM.yyyy"]})
    except ValueError as e:
        error_raised = True
        assert "formats" in str(e)
    assert error_raised, "Expected ValueError for non-dict formats"
    print("test_apply_date_format_formats_not_dict passed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test execution: process base filters update preimage
test_process_base_filters_update_preimage(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test execution: replace values
test_replace_values(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test execution: regex replace values
test_regex_replace_values(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test execution: DACH cost center 464 reduction
test_apply_dach_cost_center_reduction(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test execution: standard date format
test_apply_date_format_standard(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test execution: abort on invalid date format
test_apply_date_format_aborts_on_invalid(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test execution: missing source column in date format
test_apply_date_format_missing_source(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test execution: missing formats in date format
test_apply_date_format_missing_formats(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test execution: formats not dict in date format
test_apply_date_format_formats_not_dict(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Disabled tests
# test_unnest_jira_comments(spark)
# test_concat_columns(spark)
# test_apply_lookup(spark)
# test_apply_delta_lookup_success(spark)
# test_apply_delta_lookup_fails_on_unmatched(spark)
# test_map_array_values(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_apply_dach_cost_center_reduction(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
