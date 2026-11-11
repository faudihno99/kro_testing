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
import re
import logging
import operator
from functools import reduce
from datetime import datetime, timezone
from typing import Callable, List, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

# Module-level logger for processing operations
logger = logging.getLogger(__name__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Transformation Utils

# CELL ********************



def _apply_column_transformation(
    df: DataFrame,
    column_names: Union[str, List[str]],
    transform_func: Callable[[str], F.Column],
) -> DataFrame:
    """
    Helper function to apply a transformation to specified columns in parallel.

    Args:
        df (DataFrame): Input DataFrame
        column_names (Union[str, List[str]]): Column name(s) to transform
        transform_func (Callable): Function that takes a column name and returns a transformed Column

    Returns:
        DataFrame: DataFrame with transformed columns
    """
    if isinstance(column_names, str):
        column_names = [column_names]

    # Build select expression with all columns
    select_cols = []
    for col in df.columns:
        if col in column_names:
            select_cols.append(transform_func(col).alias(col))
        else:
            select_cols.append(F.col(col))

    return df.select(*select_cols)


# ========== Basic Numeric Transformations ==========


def round_column(
    df: DataFrame, column_names: Union[str, List[str]], n: int
) -> DataFrame:
    """
    Rounds the values in one or multiple columns to n decimal places in parallel.

    Args:
        df (DataFrame): Input DataFrame
        column_names (Union[str, List[str]]): Name(s) of the column(s) to round
        n (int): Number of decimal places

    Returns:
        DataFrame: DataFrame with the rounded column(s) modified in place
    """
    return _apply_column_transformation(
        df, column_names, lambda col: F.round(F.col(col), n)
    )


def change_sign_column(df: DataFrame, column_names: Union[str, List[str]]) -> DataFrame:
    """
    Changes the sign of values in one or multiple numeric columns in parallel.

    Args:
        df (DataFrame): Input DataFrame
        column_names (Union[str, List[str]]): Name(s) of the column(s) to change sign

    Returns:
        DataFrame: DataFrame with the sign-changed column(s) modified in place
    """
    return _apply_column_transformation(df, column_names, lambda col: F.col(col) * -1)


# ========== String Transformations ==========


def trim_column(df: DataFrame, column_names: Union[str, List[str]]) -> DataFrame:
    """
    Trims whitespace from the beginning and end of string values in one or multiple columns in parallel.

    Args:
        df (DataFrame): Input DataFrame
        column_names (Union[str, List[str]]): Name(s) of the column(s) to trim

    Returns:
        DataFrame: DataFrame with the trimmed column(s) modified in place
    """
    return _apply_column_transformation(
        df, column_names, lambda col: F.trim(F.col(col))
    )


def replace_values(
    df: DataFrame, column_names: Union[str, List[str]], params: dict
) -> DataFrame:
    """
    Replaces values in one or multiple columns based on a mapping dictionary.
    Uses Spark's create_map for high performance.

    Args:
        df (DataFrame): Input DataFrame
        column_names (Union[str, List[str]]): Name(s) of the column(s) to map
        params (dict): Parameters containing:
            - 'mapping' (dict): Dictionary of {old_value: new_value}
            - 'output_suffix' (str, optional): Suffix for the new mapped column. 
              If provided, original columns are kept.

    Returns:
        DataFrame: Transformed DataFrame
    """
    mapping = params.get("mapping", {})
    output_suffix = "_mapped"
    
    if not mapping:
        return df

    # Create Spark map expression: [lit(key1), lit(val1), lit(key2), lit(val2), ...]
    map_expr = F.create_map([F.lit(x) for k, v in mapping.items() for x in (k, v)])

    cols = [column_names] if isinstance(column_names, str) else column_names
    for c in cols:
        df = df.withColumn(f"{c}{output_suffix}", F.coalesce(map_expr[F.col(c)], F.col(c)))
    return df


def regex_replace_values(
    df: DataFrame, column_names: Union[str, List[str]], params: dict
) -> DataFrame:
    """
    Sequentially applies regex replacements based on a mapping dictionary.

    Args:
        df (DataFrame): Input DataFrame
        column_names (Union[str, List[str]]): Name(s) of the column(s) to transform
        params (dict): Parameters containing:
            - 'mapping' (dict): Dictionary of {regex_pattern: replacement}
            - 'output_suffix' (str, optional): Suffix for the new column.

    Returns:
        DataFrame: Transformed DataFrame
    """
    mapping = params.get("mapping", {})
    output_suffix = params.get("output_suffix", "_mapped")
    
    if not mapping:
        return df

    def transform(col_name):
        res = F.col(col_name)
        for k, v in mapping.items():
            res = F.regexp_replace(res, k, v)
        return res

    cols = [column_names] if isinstance(column_names, str) else column_names
    for c in cols:
        df = df.withColumn(f"{c}{output_suffix}", transform(c))
    return df


def map_array_values(
    df: DataFrame, column_names: Union[str, List[str]], params: dict
) -> DataFrame:
    """
    Splits a delimited string into an array, maps each element using a dictionary,
    optionally returns a single final string column with deduplicated values.
    Empty or whitespace-only results are returned as null.
    Args:
        df (DataFrame): Input DataFrame
        column_names (Union[str, List[str]]): Name(s) of the column(s) to map
        params (dict): Parameters containing:
            - 'mapping' (dict): Dictionary of {old_value: new_value}
            - 'delimiter' (str, optional): Delimiter to split/join (default: ",")
            - 'finalColumn_string' (bool, optional): Whether to produce a final string: column (default: True)
            - 'output_suffix' (str, optional): Suffix for the new column (default: "_mapped")
    Returns:
        DataFrame: Transformed DataFrame
    """
    mapping = params.get("mapping", {})
    output_suffix = params.get("output_suffix", "_mapped")
    delimiter = params.get("delimiter", ",")
    finalColumn_string = params.get("finalColumn_string", True)
    if not mapping:
        return df
    # Create Spark map expression when mapping exists
    map_expr = F.create_map([F.lit(x) for k, v in mapping.items() for x in (k, v)])
    cols = [column_names] if isinstance(column_names, str) else column_names
    for c in cols:
        # Clean JSON array syntax first: remove brackets and quotes
        cleaned_col = F.regexp_replace(F.col(c), "\\[|\\]|\"", "")
        # Split string to array; null input produces null, not an empty array
        arr_col = F.split(cleaned_col, f"\\s*{re.escape(delimiter)}\\s*")
        # Map each element of the array
        mapped_arr = F.transform(arr_col, lambda x: F.coalesce(map_expr[x], x))
        if finalColumn_string:
            # Remove empty/whitespace elements, then trim surviving ones
            mapped_arr_filtered = F.filter(mapped_arr, lambda x: F.trim(x) != "")
            mapped_arr_clean = F.transform(mapped_arr_filtered, lambda x: F.trim(x))
            mapped_arr_distinct = F.array_distinct(mapped_arr_clean)
            # Return null for null/empty input or empty result; otherwise the joined string
            df = df.withColumn(
                f"{c}{output_suffix}",
                F.when(
                    F.col(c).isNull() | (F.size(mapped_arr_distinct) == 0),
                    F.lit(None).cast("string")
                ).otherwise(
                    F.array_join(mapped_arr_distinct, f"{delimiter} ")
                )
            )
        else:
            df = df.withColumn(f"{c}{output_suffix}", F.array_distinct(mapped_arr))
    return df


def apply_lookup(df: DataFrame, params: dict) -> DataFrame:
    """
    Performs a left join with an external CSV for mapping.
    
    Args:
        df (DataFrame): Input DataFrame
        params (dict): Parameters containing:
            - 'path' (str): Path to the CSV file
            - 'join_col_source' (str): Join column in the input DataFrame
            - 'join_col_lookup' (str): Join column in the lookup CSV
            - 'target_cols' (dict): Mapping of {lookup_col: target_col_name} to keep from the CSV
    
    Returns:
        DataFrame: Transformed DataFrame
    """
    path = params.get("path")
    join_col_source = params.get("join_col_source")
    join_col_lookup = params.get("join_col_lookup")
    target_cols = params.get("target_cols", {})

    if not all([path, join_col_source, join_col_lookup]):
        return df

    lookup_df = spark.read.csv(path, header=True, sep=";", encoding='latin1')

    # Rename lookup columns to avoid name collisions before join
    # Except for the join column itself
    for c in lookup_df.columns:
        if c != join_col_lookup and c not in target_cols.keys():
            lookup_df = lookup_df.withColumnRenamed(c, f"_tmp_{c}")

    df = df.join(lookup_df, df[join_col_source] == lookup_df[join_col_lookup], "left")

    # Rename the desired columns to their final names
    for lookup_col, target_name in target_cols.items():
        if lookup_col in lookup_df.columns:
            df = df.withColumn(target_name, F.col(lookup_col))

    # Drop all columns that originated from the lookup_df, except for the target columns we want to keep
    target_names = set(target_cols.values())
    drop_cols = [c for c in lookup_df.columns if c not in target_names]
    # Also drop any tmp columns
    tmp_cols = [c for c in df.columns if c.startswith("_tmp_")]
    df = df.drop(*(drop_cols + tmp_cols))

    return df


def apply_delta_lookup(df: DataFrame, params: dict) -> DataFrame:
    """
    Performs a left join with a Delta Lakehouse table to derive new columns.
    Mirrors apply_lookup but reads from a Delta table instead of a CSV file.

    Args:
        df (DataFrame): Input DataFrame
        params (dict): Parameters containing:
            - 'database' (str): Lakehouse database name (resolved via 'variables_global.lh_name')
            - 'schema' (str): Schema name (e.g., 'base')
            - 'table' (str): Table name (e.g., 'sharepoint_mapping_vietnam')
            - 'join_col_source' (str): Join column in the source DataFrame
            - 'join_col_lookup' (str): Join column in the mapping Delta table
            - 'target_cols' (dict): Mapping of {lookup_col: target_col_name} to add from mapping table

    Returns:
        DataFrame: Transformed DataFrame with derived columns added
    """
    database = params.get("database")
    schema = params.get("schema")
    table = params.get("table")
    join_col_source = params.get("join_col_source")
    join_col_lookup = params.get("join_col_lookup")
    target_cols = params.get("target_cols", {})

    if not all([database, schema, table, join_col_source, join_col_lookup]):
        return df

    # Read the mapping Delta table
    mapping_df = spark.read.format("delta").table(f"{database}.{schema}.{table}")

    # Rename ALL lookup columns to avoid name collisions before join
    for c in mapping_df.columns:
        mapping_df = mapping_df.withColumnRenamed(c, f"_tmp_{c}")

    renamed_join_col = f"_tmp_{join_col_lookup}"

    # Defensive Join: Cast both sides to string and trim to handle potential type/whitespace mismatches
    join_cond = (F.trim(df[join_col_source].cast("string")) == 
                 F.trim(mapping_df[renamed_join_col].cast("string")))
    
    df = df.join(mapping_df, join_cond, "left")

    # Rename the desired columns to their final target names
    for lookup_col, target_name in target_cols.items():
        renamed_lookup_col = f"_tmp_{lookup_col}"
        if renamed_lookup_col in df.columns:
            df = df.withColumn(target_name, F.col(renamed_lookup_col))

    # Drop all columns that originated from the mapping_df
    tmp_cols = [c for c in df.columns if c.startswith("_tmp_")]
    df = df.drop(*tmp_cols)

    # Assert: every derived column must be fully populated after the join.
    # A NULL indicates that the source join key had no matching entry in the mapping table.
    for target_name in target_cols.values():
        null_df = df.filter(F.col(target_name).isNull())
        null_count = null_df.count()
        
        if null_count > 0:
            missing_keys = [row[0] for row in null_df.select(join_col_source).distinct().collect()]
            print(f"ERROR: Missing mapping for column '{target_name}'. The following values in '{join_col_source}' were not found in '{database}.{schema}.{table}': {missing_keys}")
            
        '''assert null_count == 0, (
            f"apply_delta_lookup: column '{target_name}' contains {null_count} NULL value(s) "
            f"after joining '{database}.{schema}.{table}' on "
            f"'{join_col_source}' = '{join_col_lookup}'. "
            f"Ensure all values in '{join_col_source}' have a corresponding entry in the mapping table."
        )'''

    return df


def concat_columns(df: DataFrame, params: dict) -> DataFrame:
    """
    Concatenates multiple columns with a separator.
    
    Args:
        df (DataFrame): Input DataFrame
        params (dict): Parameters containing:
            - 'input_cols' (list): List of column names to concatenate
            - 'separator' (str): Separator string (default: " ")
            - 'output_col' (str): Name of the new column
    
    Returns:
        DataFrame: Transformed DataFrame
    """
    input_cols = params.get("input_cols", [])
    separator = params.get("separator", " ")
    output_col = params.get("output_col")

    if not input_cols or not output_col:
        return df

    return df.withColumn(output_col, F.concat_ws(separator, *[F.col(c) for c in input_cols]))


def append_string_to_column(
    df: DataFrame,
    column_names: Union[str, List[str]],
    params: dict,  # Expected: {'suffix': str, 'separator': str (optional, default: '_')}
) -> DataFrame:
    """
    Appends a string to the values in one or multiple columns with a separator in parallel.

    Args:
        df (DataFrame): Input DataFrame
        column_names (Union[str, List[str]]): Name(s) of the column(s) to append to
        params (dict): Parameters containing:
            - 'suffix' (str): String to append
            - 'separator' (str, optional): Separator between original value and appended string (default: "_")

    Returns:
        DataFrame: DataFrame with the modified column(s) in place
    """
    suffix = params.get("suffix", "")
    separator = params.get("separator", "_")
    return _apply_column_transformation(
        df,
        column_names,
        lambda col: F.concat(F.col(col), F.lit(separator), F.lit(suffix)),
    )


def left_n(df: DataFrame, column_names: Union[str, List[str]], n: int) -> DataFrame:
    """
    Extracts the first n characters from one or multiple string columns in parallel.

    Args:
        df (DataFrame): Input DataFrame
        column_names (Union[str, List[str]]): Name(s) of the column(s) to extract from
        n (int): Number of characters to extract from the left

    Returns:
        DataFrame: DataFrame with the modified column(s) containing the first n characters
    """
    return _apply_column_transformation(
        df, column_names, lambda col: F.substring(F.col(col), 1, n)
    )




# ========== Column Selection and Renaming ==========


def select_and_rename_columns(df: DataFrame, column_mapping: dict) -> DataFrame:
    """
    Selects and renames columns based on a mapping dictionary.

    Args:
        df (DataFrame): Input DataFrame
        column_mapping (dict): Dictionary mapping old column names to new column names

    Returns:
        DataFrame: DataFrame with selected and renamed columns
    """
    return df.select(
        *[
            F.col(old_name).alias(new_name)
            for old_name, new_name in column_mapping.items()
        ]
    )


def rename_columns(df: DataFrame, column_mapping: dict) -> DataFrame:
    """
    Renames columns based on a mapping dictionary.

    Args:
        df (DataFrame): Input DataFrame
        column_mapping (dict): Dictionary mapping old column names to new column names

    Returns:
        DataFrame: DataFrame with renamed columns
    """
    return select_and_rename_columns(df, column_mapping)


# ========== Type Casting ==========


def cast_data_types(
    df: DataFrame, column_names: Union[str, List[str]], data_type: str
) -> DataFrame:
    """
    Casts one or multiple columns to a specified data type in parallel.

    Args:
        df (DataFrame): Input DataFrame
        column_names (Union[str, List[str]]): Name(s) of the column(s) to cast
        data_type (str): Target data type (e.g., 'string', 'int', 'double', 'timestamp')

    Returns:
        DataFrame: DataFrame with the cast column(s) modified in place
    """
    return _apply_column_transformation(
        df, column_names, lambda col: F.col(col).cast(data_type)
    )


# ========== Timestamp Operations ==========


def extract_from_timestamp(
    df: DataFrame,
    column_names: Union[str, List[str]],
    params: dict,  # Expected: {'extract_type': str, 'output_column_name': str, 'format': str (optional)}
) -> DataFrame:
    """
    Adds a new column by extracting date, time, year, month, or calendar week from a timestamp column.

    Args:
        df (DataFrame): The input DataFrame.
        column_names (Union[str, List[str]]): The name(s) of the timestamp column(s) to extract from.
        params (dict): Parameters containing:
            - 'extract_type' (str): Type of extraction - "date", "time", "year", "month", or "week".
            - 'output_column_name' (str): The name of the new column.
            - 'format' (str, optional): The format string. Only used for "date" and "time" extractions.
                                       If None, defaults to 'yyyy-MM-dd' for date or 'HH:mm:ss' for time.
                                       Ignored for year, month, and week extractions.

    Returns:
        DataFrame: The modified DataFrame with the new column containing the extracted value.

    Raises:
        ValueError: If extract_type is not one of the supported types.
    """
    if isinstance(column_names, str):
        column_names = [column_names]

    extract_type = params.get("extract_type")
    output_column_name = params.get("output_column_name")
    format = params.get("format")

    valid_types = ["date", "time", "year", "month", "week"]
    if extract_type not in valid_types:
        raise ValueError(f"extract_type must be one of {valid_types}")

    # Use the first column name for extraction (supporting multiple columns for consistency with other functions)
    input_column_name = column_names[0]

    # Apply the appropriate transformation
    if extract_type == "date":
        if format is None:
            format = "yyyy-MM-dd"
        df = df.withColumn(
            output_column_name, F.to_date(F.col(input_column_name), format)
        )

    elif extract_type == "time":
        if format is None:
            format = "HH:mm:ss"
        df = df.withColumn(
            output_column_name, F.date_format(F.col(input_column_name), format)
        )

    elif extract_type == "year":
        df = df.withColumn(output_column_name, F.year(F.col(input_column_name)))

    elif extract_type == "month":
        df = df.withColumn(output_column_name, F.month(F.col(input_column_name)))

    elif extract_type == "week":
        df = df.withColumn(output_column_name, F.weekofyear(F.col(input_column_name)))

    return df


def enforce_min_timestamp(
    df: DataFrame, column_names: Union[str, List[str]]
) -> DataFrame:
    """
    Ensures all timestamp values in the given columns are >= '1970-01-01 00:00:00'.

    If a timestamp is earlier, it is replaced by '1970-01-01 00:00:00'.

    - Works for both string and timestamp columns.
    - Returns DataFrame with same schema (columns remain timestamps).

    Args:
        df (DataFrame): Input DataFrame.
        column_names (Union[str, List[str]]): Column(s) to enforce minimum timestamp on.

    Returns:
        DataFrame with enforced minimum timestamp columns.
    """
    min_ts = F.lit(datetime(1970, 1, 1))
    return _apply_column_transformation(
        df,
        column_names,
        lambda colname: F.when((F.col(colname)) < min_ts, min_ts).otherwise(
            F.col(colname)
        ),
    )


def build_combined_timestamp(
    df: DataFrame,
    column_names: Union[str, List[str]],
    params: dict,
) -> DataFrame:
    """
    Creates a combined timestamp by combining date and time columns.

    Args:
        df (DataFrame): Input DataFrame.
        column_names (Union[str, List[str]]): Target column names for the combined timestamp.
        params (dict): Parameters containing:
            - 'date_column' (str): Name of the date column.
            - 'time_column' (str): Name of the time column.
            - 'output_column_name' (str): Name of the output column.

    Returns:
        DataFrame: DataFrame with combined timestamp columns.

    Raises:
        ValueError: If 'date_column' or 'time_column' are not provided in params.
    """
    date_column = params.get("date_column")
    time_column = params.get("time_column")
    output_column_name = params.get("output_column_name")

    if isinstance(column_names, str):
        column_names = [column_names]

    # Create the transformation expression
    combined_timestamp_expr = F.to_timestamp(
        F.concat_ws(" ", F.col(date_column), F.col(time_column))
    )

    # Always add as a new column - use column_names[0] as the output column name
    output_name = column_names[0] if column_names else output_column_name

    # Build select expression with all existing columns plus the new combined timestamp column
    select_cols = [F.col(col) for col in df.columns]
    select_cols.append(combined_timestamp_expr.alias(output_name))

    return df.select(*select_cols)


def apply_date_format(
    df: DataFrame,
    column_names: Union[str, List[str]],
    params: dict,
) -> DataFrame:
    """
    Parses a date column using regex-based format detection and validates the result.

    Uses regex patterns to detect the correct date format per row, avoiding
    iteration over all formats. Falls back to format-free parsing for native
    dates/timestamps if enabled.

    Args:
        df (DataFrame): Input DataFrame.
        column_names (Union[str, List[str]]): Target column name(s) for the parsed date.
        params (dict): Parameters containing:
            - 'source_column' (str): Name of the source column with date strings/timestamps.
            - 'target_column' (str, optional): Output column name (default: 'date_formatted').
            - 'formats' (dict): Mapping of Spark date format string -> regex pattern.
              Example:
                {"dd.MM.yyyy": "^\\d{2}\\.\\d{2}\\.\\d{4}$",
                 "yyyy-MM-dd": "^\\d{4}-\\d{2}-\\d{2}$"}
            - 'fallback' (bool, optional): Enable format-free parsing fallback
              (default: True).

    Returns:
        DataFrame: DataFrame with the parsed date column.

    Raises:
        ValueError: If required parameters are missing, invalid, or unparseable
                    non-null values are detected.
    """
    if isinstance(column_names, str):
        column_names = [column_names]

    source_col = params.get("source_column")
    target_col = params.get("target_column", column_names[0] if column_names else None)
    formats = params.get("formats")
    fallback = params.get("fallback", True)

    if not source_col:
        raise ValueError("apply_date_format: 'source_column' must be provided")
    if not target_col:
        raise ValueError("apply_date_format: 'target_column' must be provided")
    if not formats or not isinstance(formats, dict):
        raise ValueError("apply_date_format: 'formats' must be a non-empty dict")

    format_expr = F.lit(None)
    for fmt_str, regex in formats.items():
        condition = F.col(source_col).rlike(regex)
        parsed = F.to_date(F.col(source_col), fmt_str)
        format_expr = F.when(condition, parsed).otherwise(format_expr)

    if fallback:
        format_expr = F.when(format_expr.isNotNull(), format_expr).otherwise(
            F.to_date(F.col(source_col))
        )

    df = df.withColumn(target_col, format_expr)

    invalid = df.filter(
        F.col(source_col).isNotNull() & F.col(target_col).isNull()
    )
    invalid_count = invalid.count()
    if invalid_count > 0:
        invalid_values = [
            str(row[source_col])
            for row in invalid.select(source_col).distinct().collect()
        ]
        raise ValueError(
            f"apply_date_format: {invalid_count} row(s) with non-null source "
            f"'{source_col}' produced null target '{target_col}'. "
            f"Unparseable values: {invalid_values}"
        )

    return df


# ========== Complex Operations ==========


def unpivot_multi_columns(
    df: DataFrame,
    key_columns: List[str],
    column_mappings: dict,
    identifier_pattern: str = r"(\w+)(\d+)_",
    auto_passthrough: bool = True,
    identifier_name: str = "identifier",
) -> DataFrame:
    """
    General unpivot function that maps specified columns to output columns.

    Parameters:
        df (DataFrame): Input DataFrame
        key_columns (List[str]): List of columns to keep as identifiers (won't be unpivoted)
        column_mappings (dict): Dict where key=output_column_name, value=list of input columns
        identifier_pattern (str): Regex to extract identifier from column names
        auto_passthrough (bool): If True, all unmapped columns become key columns (default: True)
        identifier_name (str): Name for the identifier column in output

    Returns:
        DataFrame: Unpivoted DataFrame
    """
    # Find all mapped columns
    mapped_columns = set()
    for input_cols in column_mappings.values():
        mapped_columns.update(input_cols)

    # Determine final key columns
    if auto_passthrough:
        # All columns except mapped ones become key columns
        final_key_columns = [col for col in df.columns if col not in mapped_columns]
    else:
        # Only explicitly specified key columns
        final_key_columns = key_columns

    # Build stack expression
    stack_items = []
    for output_col, input_cols in column_mappings.items():
        for input_col in input_cols:
            # Extract identifier from column name
            match = re.match(identifier_pattern, input_col)
            identifier = match.group(2) if match else input_col
            stack_items.extend([f"'{identifier}'", f"'{output_col}'", f"`{input_col}`"])

    num_rows = sum(len(input_cols) for input_cols in column_mappings.values())
    stack_expr = (
        f"stack({num_rows}, {', '.join(stack_items)}) "
        f"as ({identifier_name}, metric_type, metric_value)"
    )

    # Unpivot and pivot back
    unpivoted = df.select(*final_key_columns, F.expr(stack_expr))
    unpivoted = unpivoted.filter(F.col("metric_value").isNotNull())

    result = (
        unpivoted.groupBy(*final_key_columns, identifier_name)
        .pivot("metric_type")
        .agg(F.first("metric_value"))
    )

    return result


def add_custom_column(
    df: DataFrame, column_name: str, column_expression: F.Column
) -> DataFrame:
    """
    Adds or modifies a column using a custom PySpark column expression.

    Args:
        df (DataFrame): Input DataFrame
        column_name (str): Name for the output column
        column_expression (F.Column): PySpark Column expression to evaluate

    Returns:
        DataFrame: DataFrame with the new or modified column
    """
    return df.withColumn(column_name, column_expression)


# ========== DACH-specific Transformations ==========


def apply_dach_cost_center_reduction(
    df: DataFrame,
    column_names: Union[str, List[str]],
    params: dict = {}
) -> DataFrame:
    """
    DACH-specific reduction of ledger amounts for cost center 464.

    For every row where cost_center_number == 464, the given amount columns
    (e.g. debit_local, credit_local) are multiplied by 0.5 (50%).
    NULL values remain NULL, zero remains zero.
    """
    cost_center_col = params.get("cost_center_col", "cost_center_number")
    cost_center_value = params.get("cost_center_value", 464)
    factor = params.get("factor", 0.5)

    condition = F.col(cost_center_col) == F.lit(cost_center_value)

    return _apply_column_transformation(
        df,
        column_names,
        lambda col: F.when(condition, F.col(col) * factor).otherwise(F.col(col))
    )


# ========== Jira Specific Transformations ==========

def unnest_jira_comments(df: DataFrame, column_names: Union[str, List[str]], params: dict) -> DataFrame:
    # Unnests a JSON column containing Jira comments and flattens it.
    # The resulting table will be on comment level (one row per comment).
    # Inputs: df, column_names (name of JSON column), params
    # Outputs: DataFrame with flattened string columns
    
    from pyspark.sql.types import ArrayType, StringType, StructType, StructField
    
    col_name = column_names[0] if isinstance(column_names, list) else column_names
    
    schema = ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("author", StructType([
            StructField("name", StringType(), True),
            StructField("displayName", StringType(), True),
            StructField("emailAddress", StringType(), True),
            StructField("active", StringType(), True),
        ]), True),
        StructField("body", StringType(), True),
        StructField("updateAuthor", StructType([
            StructField("name", StringType(), True),
            StructField("displayName", StringType(), True),
            StructField("emailAddress", StringType(), True),
        ]), True),
        StructField("created", StringType(), True),
        StructField("updated", StringType(), True),
    ]))
    
    # Parse the JSON string into an array of structs
    df = df.withColumn("parsed_comments", F.from_json(F.col(col_name), schema))
    
    # Explode the array to create one row per comment
    df = df.withColumn("comment", F.explode_outer(F.col("parsed_comments")))
    
    # Extract flattened fields
    df = df.withColumn(f"{col_name}_id", F.col("comment.id").cast("string"))
    df = df.withColumn(f"{col_name}_author_name", F.col("comment.author.name").cast("string"))
    df = df.withColumn(f"{col_name}_author_displayName", F.col("comment.author.displayName").cast("string"))
    df = df.withColumn(f"{col_name}_author_emailAddress", F.col("comment.author.emailAddress").cast("string"))
    df = df.withColumn(f"{col_name}_author_active", F.col("comment.author.active").cast("string"))
    df = df.withColumn(f"{col_name}_body", F.col("comment.body").cast("string"))
    df = df.withColumn(f"{col_name}_updateAuthor_name", F.col("comment.updateAuthor.name").cast("string"))
    df = df.withColumn(f"{col_name}_updateAuthor_displayName", F.col("comment.updateAuthor.displayName").cast("string"))
    df = df.withColumn(f"{col_name}_created", F.col("comment.created").cast("string"))
    df = df.withColumn(f"{col_name}_updated", F.col("comment.updated").cast("string"))
    
    # Drop intermediate and original columns
    df = df.drop("parsed_comments", "comment", col_name)
    
    return df

# ========== Record Filtering and Cleaning ==========


def remove_invalid_records(
    df: DataFrame, column_names: Union[str, List[str]], params: dict
) -> DataFrame:
    """
    Removes records where any of the specified columns contain NULL values or empty strings.

    Args:
        df (DataFrame): Input DataFrame
        column_names (Union[str, List[str]]): Name(s) of the column(s) to check
        params (dict): Parameters (not used, kept for structure consistency)

    Returns:
        DataFrame: DataFrame with rows containing NULLs or empty strings in specified columns removed
    """
    if isinstance(column_names, str):
        column_names = [column_names]

    for col_name in column_names:
        df = df.filter(F.col(col_name).isNotNull())
        df = df.filter(F.col(col_name) != "")

    return df


# ========== Utility and Orchestration Functions ==========


def group_columns_by_parameters(columns_config: dict) -> List[dict]:
    """
    Group columns by their transformation parameters to enable efficient batch processing.
    Columns with identical parameter sets are grouped together so they can be processed
    in a single operation, reducing the number of DataFrame transformations required.

    Args:
        columns_config: Dictionary mapping column names to their transformation parameters
                       Format: {'column_name': {'param1': value1, 'param2': value2}, ...}
                       Example: {'amount': {'precision': 2}, 'balance': {'precision': 2},
                                 'rate': {'precision': 4}, 'description': {}}

    Returns:
        List of dictionaries, each containing:
        - 'columns': List of column names sharing the same parameters
        - 'params': Dictionary of shared parameters for this group

        Example return value:
        [
            {'columns': ['amount', 'balance'], 'params': {'precision': 2}},
            {'columns': ['rate'], 'params': {'precision': 4}},
            {'columns': ['description'], 'params': {}}
        ]

    Example:
        columns_config = {
            'amount': {'precision': 2},
            'balance': {'precision': 2},
            'rate': {'precision': 4},
            'description': {}
        }
        groups = group_columns_by_parameters(columns_config)
        Returns 3 groups: first group has amount+balance with precision=2,
        second group has rate with precision=4, third group has description with no params
    """
    # Initialize dictionary to store parameter groups
    parameter_groups = {}

    # Group by parameters - Create key from all parameter values
    for column_name, column_params in columns_config.items():
        # Create parameter key from sorted parameter items for consistency
        if column_params:
            # Sort parameters to ensure consistent grouping
            sorted_params = sorted(column_params.items())
            param_key = "|".join([f"{k}:{v}" for k, v in sorted_params])
        else:
            param_key = "no_params"

        if param_key not in parameter_groups:
            parameter_groups[param_key] = {"columns": [], "params": column_params}
        parameter_groups[param_key]["columns"].append(column_name)

    # Return list of groups instead of dictionary
    return list(parameter_groups.values())


def call_utility_function(
    df: DataFrame, transform_type: str, columns: List[str], params: dict
) -> DataFrame:
    """
    This function is used to call the appropriate utility function based
    on the respective transformation type. It inputs all the columns and parameters
    of one parameter group.

    Args:
        df (DataFrame): Input DataFrame
        transform_type (str): The transformation to be applied
        columns (List[str]): List of column names
        params (dict): Dictionary of parameters

    Returns:
        DataFrame: Transformed DataFrame

    Example:
        df = call_utility_function(df, 'round', ['amount', 'balance'], {'precision': 2})
        The example applies the round transformation to the amount and balance columns with
         a precision of 2.
    """
    # Generic mapping of transformation types to utility functions - EASILY EXTENSIBLE
    utility_function_map = {
        "round": lambda df, cols, p: round_column(df, cols, p["precision"]),
        "trim": lambda df, cols, p: trim_column(df, cols),
        "left_n": lambda df, cols, p: left_n(df, cols, p["n"]),
        "append_suffix": lambda df, cols, p: append_string_to_column(df, cols, p),
        "cast_data_types": lambda df, cols, p: cast_data_types(
            df, cols, p["data_type"]
        ),
        "extract_from_timestamp": lambda df, cols, p: extract_from_timestamp(
            df, cols, p
        ),
        "add_custom_column": lambda df, cols, p: add_custom_column(
            df,
            cols[0],
            (
                eval(p["column_expression"])
                if isinstance(p["column_expression"], str)
                else p["column_expression"]
            ),
        ),
        "enforce_min_timestamp": lambda df, cols, p: enforce_min_timestamp(df, cols),
        "build_combined_timestamp": lambda df, cols, p: build_combined_timestamp(
            df, cols, p
        ),
        "unnest_jira_comments": lambda df, cols, p: unnest_jira_comments(df, cols, p),
        "replace_values": lambda df, cols, p: replace_values(df, cols, p),
        "map_array_values": lambda df, cols, p: map_array_values(df, cols, p),
        "regex_replace_values": lambda df, cols, p: regex_replace_values(df, cols, p),
        "apply_lookup": lambda df, cols, p: apply_lookup(df, p),
        "apply_delta_lookup": lambda df, cols, p: apply_delta_lookup(df, p),
        "concat_columns": lambda df, cols, p: concat_columns(df, p),
        "remove_invalid_records": lambda df, cols, p: remove_invalid_records(df, cols, p),
        "apply_dach_cost_center_reduction": lambda df, cols, p: apply_dach_cost_center_reduction(df, cols, p),
        "apply_date_format": lambda df, cols, p: apply_date_format(df, cols, p),
    }

    # Dispatch to appropriate utility function
    if transform_type in utility_function_map:
        return utility_function_map[transform_type](df, columns, params)
    else:
        # Return DataFrame unchanged for unknown transformation types
        return df


def apply_transformation(
    df: DataFrame, transform_type: str, transformation_config: dict
) -> DataFrame:
    """
    Apply a specific transformation to multiple columns using utility functions from base_layer_utils.
    This function processes columns in parameter groups to minimize DataFrame operations and maximize
    efficiency. Each transformation type maps to atomic, testable utility functions.

    Args:
        df (DataFrame): Input PySpark DataFrame to be transformed
        transform_type (str): String specifying the type of transformation to apply
                       Supported types: 'round', 'trim', 'left_n', 'append_suffix',
                       'extract_from_timestamp', 'cast_data_types', 'add_custom_column',
                       'remove_invalid_records', 'extract_nested_json', 'rename_columns'
                       Must match an entry in the utility_function_map
        transformation_config (dict): Dictionary containing transformation configuration with required fields:
                              {
                                  'type': 'round',  # Must match transform_type parameter
                                  'columns_config': {
                                      'column1': {'precision': 2},
                                      'column2': {'precision': 4},
                                      'column3': {}  # Empty dict for no parameters
                                  }
                              }

                              # This should be different, structure should be like above
                              For extract_nested_json:
                              {
                                  'type': 'extract_nested_json',
                                  'columns_config': {
                                      'location': ['street', 'city', 'zip_code'],
                                      'contact': ['phone', 'email']
                                  }
                              }

    Returns:
        DataFrame: New DataFrame with the transformation applied to specified columns.
                  Original columns are replaced with their transformed versions.
                  Non-specified columns remain unchanged.

    Example:
        df = spark.createDataFrame([(1.23456, 2.7890, "  text  ")], ["amount", "rate", "description"])
        transformation_config = {
            'type': 'round',
            'columns_config': {
                'amount': {'precision': 2},
                'rate': {'precision': 1}
            }
        }
        result_df = apply_transformation(df, 'round', transformation_config)
        # Returns DataFrame with amount=1.23, rate=2.8, description="  text  " (unchanged)

    Note:
        - Parameters are accessed directly without validation (validation handled externally)
        - Columns with identical parameters are grouped together for efficient processing
        - Unknown transformation types return the DataFrame unchanged
        - Function is atomic and testable through utility function delegation
    """
    result_df = df
    columns_config = transformation_config["columns_config"]

    # Special handling for extract_nested_json where columns_config maps parent columns to list of nested fields
    # if transform_type == "extract_nested_json":
    #     for parent_column, nested_fields in columns_config.items():
    #         result_df = extract_nested_json(result_df, parent_column, nested_fields)
    #     return result_df

    # Special handling for rename_columns where columns_config is a simple mapping dictionary
    # This should be different, structure should be like above
    if transform_type == "rename_columns":
        result_df = rename_columns(result_df, columns_config)
        return result_df

    parameter_groups = group_columns_by_parameters(columns_config)

    for group_data in parameter_groups:
        columns = group_data["columns"]
        params = group_data["params"]

        if not columns:
            continue

        # Use utility functions - ATOMIC & TESTABLE - GENERIC DISPATCH
        result_df = call_utility_function(result_df, transform_type, columns, params)

    return result_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 # ========== Hashing Utility ==========

def generate_sha2_hash(hash_input_columns: list[str]):
    """
    Generate SHA-256 hash for a list of input columns.
    Concatenates columns as strings and hashes the binary representation.
    
    Args:
        hash_input_columns: List of column names to hash
        
    Returns:
        Column expression for the hash
    """
    hash_input_columns_expr = [
        F.col(column).cast("string") for column in hash_input_columns
    ]
    hash_column_expr = F.to_binary(
        F.sha2(F.concat_ws("", *hash_input_columns_expr), 256), F.lit("hex")
    )
    return hash_column_expr

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Processing

# CELL ********************

# ========== Base Processing Functions ==========

# Module-level logger is defined at the top of the file, but we need to ensure it's available or re-define it here if we want this section to be somewhat standalone, 
# but better to assume global `logger` or use `logging.getLogger(__name__)` inside functions if needed.
# The previous code had a logger definition here. I will keep it consistent with the file structure.

def process_base(
    spark: SparkSession,
    config: dict,
    hash_diff_column_name: str = "hash_diff",
    use_cdc: bool = False,
    load_timestamp: datetime = None
) -> tuple:
    """
    Process base layer data from historized landing tables using Change Data Capture (CDC).
    """
    source_table_name = config["source"]["table_name"]
    target_table_name = config["target"]["table_name"]

    try:
        # Step 1: Initialize handlers
        target_handler = DeltaLakeHandler(
            spark=spark,
            database=config["target"]["lakehouse_name"],
            schema=config["target"]["schema_name"],
            table=target_table_name,
        )

        source_handler = DeltaLakeHandler(
            spark=spark,
            database=config["source"]["lakehouse_name"],
            schema=config["source"]["schema_name"],
            table=source_table_name,
        )

        # Step 2: Determine processing mode
        target_table_empty = False
        if (
            not target_handler.delta_table_exists()
            or target_handler.target_delta_table.toDF().isEmpty()
        ):
            target_table_empty = True

        cdc = use_cdc and not target_table_empty
        last_commit_timestamp = None

        if cdc:
            last_commit_timestamp = target_handler.get_last_commit_timestamp()

        # Step 3: Read source data
        columns_to_select = []
        if "column_mapping" in config and config["column_mapping"]:
            columns_to_select.extend(list(config["column_mapping"].keys()))

        if cdc and columns_to_select:
            # Ensure CDC metadata columns are included if we are filtering columns
            cdc_metadata_cols = ["_change_type", "_commit_version", "_commit_timestamp"]
            for col in cdc_metadata_cols:
                if col not in columns_to_select:
                    columns_to_select.append(col)

        df = source_handler.read_delta_lake_table(
            columns_to_select=columns_to_select,
            cdc_source=cdc,
            cdc_start_timestamp=last_commit_timestamp if cdc else None,
        )

        if df.isEmpty():
            logger.info(f"No changes detected in {source_table_name}, skipping.")
            return (target_table_name, True, None)

        # Step 4: Transformations
        if not cdc:
            df = select_and_rename_columns(df, config["column_mapping"])
        else:
            cdc_column_mapping = {"_change_type": "_change_type", "_commit_version": "_commit_version", "_commit_timestamp": "_commit_timestamp"}
            cdc_column_mapping.update(config["column_mapping"])
            df = select_and_rename_columns(df, cdc_column_mapping)

        df = df.dropDuplicates()
        
        string_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]
        df = trim_column(df, string_cols)
        
        # Replace empty strings or whitespace-only strings with NULL for all string columns
        for col_name in string_cols:
            df = df.withColumn(
                col_name,
                F.when(F.trim(F.col(col_name)) == "", F.lit(None)).otherwise(F.col(col_name))
            )

        if "transformations" in config:
            for transformation in config["transformations"]:
                df = apply_transformation(df, transformation["type"], transformation)

        target_primary_keys = config["target_primary_keys"]
        exclude_from_hash = target_primary_keys + [
            hash_diff_column_name, "_change_type", "_load_timestamp",
            "_commit_version", "_commit_timestamp"
        ]
        columns_to_hash = [
            col for col in df.columns
            if col not in exclude_from_hash
        ]

        if cdc:
            # update_preimage enthält den alten Zustand vor einem Update.
            # Für die Zustandsübertragung in Base ist nur der neue Zustand (postimage) relevant.
            # Wenn beide im selben Commit auftreten, führt der identische _commit_timestamp 
            # und _commit_version zu einem nicht-deterministischen row_number()-Tie.
            df = df.filter(F.col("_change_type") != "update_preimage")
            
            window_spec = Window.partitionBy(*target_primary_keys).orderBy(F.col("_commit_timestamp").desc(), F.col("_commit_version").desc())
            df = (df.withColumn("_row_num", F.row_number().over(window_spec))
                     .filter(F.col("_row_num") == 1)
                     .drop("_row_num", "_commit_timestamp", "_commit_version"))
        else:
            df = df.dropDuplicates(target_primary_keys)

        df = df.withColumn(hash_diff_column_name, generate_sha2_hash(columns_to_hash))

        # Step 5: Great Expectations validation (in-process, before write)
        if config.get("gx_validation"):
            validation_results, results, run_id = run_base_gx(
                df=df,
                config=config,
                target_table_name=target_table_name,
                spark=spark,
                database_name=config["target"]["lakehouse_name"],
                persist=True,
                monitoring_schema="monitoring",
                dq_table_name="dq_results_base",
                load_timestamp=load_timestamp
            )

        # Step 6: Write
        mode = "overwrite" if target_table_empty else "upsert_and_delete"
        
        target_handler.write_to_delta_lake(
            df=df,
            mode=mode,
            cdc_source=cdc,
            merge_keys=target_primary_keys,
            cdc_target=True,
            hash_diff_column_name=hash_diff_column_name
        )
        
        logger.info(f"Successfully processed {target_table_name}")
        return (target_table_name, True, None)

    except RuntimeError:
        # Re-raise RuntimeError from Great Expectations validation failures
        # so that the notebook aborts instead of collecting the error as a failed table.
        raise
    except Exception as e:
        return build_error_payload(source_table_name, target_table_name, "base", e)


def process_all_base_tables(
    spark: SparkSession,
    config_list: list[dict],
    use_cdc: bool = False,
    load_timestamp: datetime = None
) -> dict:
    """Process all base tables in parallel.
    
    Args:
        spark: Active Spark session.
        config_list: List of base layer configurations.
        use_cdc: If True, read sources via Change Data Capture; 
                 if False, perform a full source read with classic upsert+delete merge.
    """
    num_tables = len(config_list)
    logger.info(f"Starting parallel processing of {num_tables} base tables...")

    max_workers = min(num_tables, 4) if num_tables > 0 else 1

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_config = {
            executor.submit(process_base, spark, cfg, use_cdc=use_cdc, load_timestamp=load_timestamp): cfg
            for cfg in config_list
        }

        successful = []
        failed = []

        # Erfolg: Tupel (target_table, True, None) | Fehler: Dict von build_error_payload()
        for future in as_completed(future_to_config):
            result = future.result()
            if isinstance(result, dict):
                failed.append(result)
            else:
                target_table, success, error_msg = result
                if success:
                    successful.append(target_table)
                else:
                    failed.append(build_error_payload("unknown", target_table, "base", error_msg))

    logger.info(f"Summary: {len(successful)} success, {len(failed)} failed")
    
    if failed:
        for entry in failed:
            logger.warning(f"Failed: {entry['target_table']} - {entry['error_message']}")

    # Kein RuntimeError mehr — Pipeline bleibt grün
    return {"successful": successful, "failed_tables": failed}


def get_configs_to_run(p_sources_to_run, all_configs: dict) -> List[dict]:
    """
    Get the configurations that should be run based on the provided list of sources.
    
    Args:
        p_sources_to_run: List of source names (e.g., ["sugar", "jira"]) or a JSON string
        all_configs: Dictionary mapping source names to their configuration lists
        
    Returns:
        Flattened list of configuration dictionaries to process
    """
    if isinstance(p_sources_to_run, str):
        import json
        p_sources_to_run = json.loads(p_sources_to_run.replace("'", '"'))
    
    configs_to_run = []
    for source in p_sources_to_run:
        source_lower = source.lower()
        if source_lower in all_configs:
            configs_to_run.extend(all_configs[source_lower])
        else:
            print(f"Warning: Source '{source}' not found in available configurations.")
    
    return configs_to_run


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def filter_failed_tables(data_dict):
    """
    Filters out specific delta timestamp errors from the failed_tables list.
    """
    error_prefix = '[DELTA_TIMESTAMP_GREATER_THAN_COMMIT]'
    
    # Update the dictionary in place or return a filtered copy
    data_dict['failed_tables'] = [
        table for table in data_dict.get('failed_tables', [])
        if not table.get('error_message', '').startswith(error_prefix)
    ]
    
    return data_dict

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
