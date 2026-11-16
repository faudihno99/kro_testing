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
from pyspark.sql import DataFrame

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Hashing Utils

# MARKDOWN ********************

# ##### Config rule: metadata columns are never hashed
#
# Every config that feeds a hash calculation (histlanding `payload_columns`, base `hashdiff_cols`)
# must follow these rules:
#
# - `_source` holds the source of the row (`"sugar"`, `"sap"`, ...). It is created in histlanding
#   from `source_system` and carried through to base.
# - `_load_timestamp` holds the landing load timestamp.
# - Metadata columns (`_source`, `_load_ts`, `_load_timestamp`, `_change_type`, `_commit_version`,
#   `_commit_timestamp`) are **never** listed in `payload_columns` / `hashdiff_cols` and are
#   stripped automatically if they are.
# - `_source` and `_load_timestamp` are always part of the base `column_mapping` so they reach the
#   target table.
# - A column starting with `_` is only a metadata column if it appears in `_EXCLUDED_HASH_COLUMNS`.
#   Fields such as `_module` or `_link` are regular payload and stay part of the hash.

# CELL ********************

# Separator inserted between the string-cast input columns before hashing.
# Prevents value-boundary shifts ([“a”, “bc”] vs. [“ab”, “c”]) from colliding.
_HASH_SEPARATOR = "||"

# Columns that must NEVER be part of a change-detection hash (hash_diff),
# regardless of what the caller passes in. Central definition, single source
# of truth for every layer (histlanding, base, serving).
#   _source / _load_ts      : canonical metadata names (see config rule above)
#   _load_timestamp         : metadata column produced by landing for Sugar/SAP
#   _change_type/_commit_*  : Change Data Feed columns
_EXCLUDED_HASH_COLUMNS = (
    "_source",
    "_load_ts",
    "_load_timestamp",
    "_change_type",
    "_commit_version",
    "_commit_timestamp",
)


def _drop_metadata_columns(hash_columns: list[str]) -> list[str]:
    """
    Remove metadata columns from a hash column list.

    Args:
        hash_columns (list[str]): Requested hash column names.

    Returns:
        list[str]: hash_columns without every _EXCLUDED_HASH_COLUMNS entry.
    """
    return [column for column in hash_columns if column not in _EXCLUDED_HASH_COLUMNS]


def generate_sha2_hash(hash_input_columns: list[str]) -> F.Column:
    """
    Build the SHA-256 hash over one or more columns.

    Column names are sorted internally, so the caller's column order does not
    affect the resulting hash. Values are string-cast and joined with
    _HASH_SEPARATOR to avoid boundary-shift collisions. NULLs are skipped by
    concat_ws; there is no NULL vs. empty-string semantic.

    Args:
        hash_input_columns (list[str]): Names of the columns to hash. Must not be empty.

    Returns:
        F.Column: Binary SHA-256 hash over the string-concatenated input columns.

    Raises:
        ValueError: If hash_input_columns is empty.
    """
    if not hash_input_columns:
        raise ValueError("hash_input_columns must not be empty")

    hash_input_columns_expr = [
        F.col(column).cast("string") for column in sorted(hash_input_columns)
    ]
    return F.to_binary(
        F.sha2(F.concat_ws(_HASH_SEPARATOR, *hash_input_columns_expr), 256),
        F.lit("hex"),
    )


def _validate_hash_columns(df: DataFrame, hash_columns: list[str]) -> None:
    """
    Validate that a hash column list is non-empty and present in the DataFrame.

    Args:
        df (DataFrame): DataFrame the hash is computed on.
        hash_columns (list[str]): Column names requested for hashing.

    Returns:
        None

    Raises:
        ValueError: If hash_columns is empty or contains a column missing from df.
    """
    if not hash_columns:
        raise ValueError("hash_columns must not be empty")

    missing_columns = [column for column in hash_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"hash columns missing in DataFrame: {missing_columns}")


def add_hash_pk(
    df: DataFrame,
    pk_columns: list[str],
    target_col: str = "hash_pk",
) -> DataFrame:
    """
    Add a primary-key hash column to the DataFrame.

    Args:
        df (DataFrame): Input DataFrame.
        pk_columns (list[str]): Business key columns to hash.
        target_col (str): Name of the added hash column.

    Returns:
        DataFrame: Input DataFrame with target_col appended.

    Raises:
        ValueError: If pk_columns is empty or a column is missing from df.
    """
    _validate_hash_columns(df, pk_columns)
    return df.withColumn(target_col, generate_sha2_hash(pk_columns))


def add_hash_diff(
    df: DataFrame,
    payload_columns: list[str],
    target_col: str = "hash_diff",
) -> DataFrame:
    """
    Add a change-detection hash column to the DataFrame.

    Uses the same hashing logic as add_hash_pk. Metadata columns
    (_EXCLUDED_HASH_COLUMNS) are always stripped from payload_columns, even if
    the caller lists them explicitly.

    Args:
        df (DataFrame): Input DataFrame.
        payload_columns (list[str]): Payload columns to hash.
        target_col (str): Name of the added hash column.

    Returns:
        DataFrame: Input DataFrame with target_col appended.

    Raises:
        ValueError: If payload_columns is empty (after metadata stripping) or a
            remaining column is missing from df.
    """
    hash_columns = _drop_metadata_columns(payload_columns)
    _validate_hash_columns(df, hash_columns)
    return df.withColumn(target_col, generate_sha2_hash(hash_columns))


def derive_hash_diff_columns(
    df: DataFrame,
    exclude_columns: list[str],
) -> list[str]:
    """
    Derive the payload columns for change detection from a DataFrame.

    Returns every DataFrame column that is not listed in exclude_columns and is
    not a metadata column (_EXCLUDED_HASH_COLUMNS).

    Args:
        df (DataFrame): DataFrame to derive payload columns from.
        exclude_columns (list[str]): Columns to exclude (keys and hash columns).

    Returns:
        list[str]: Payload column names in DataFrame order.
    """
    candidates = [
        column for column in df.columns if column not in exclude_columns
    ]
    return _drop_metadata_columns(candidates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
