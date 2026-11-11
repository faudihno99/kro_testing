# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************


import logging
import os
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import BinaryType, StructType
from pyspark.sql.window import Window


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# CHANGELOG
# -----------------------------------------------------------------------------
# 2026-07-21  Fixed mssparkutils.fs.exists() / fs.ls() compatibility:
#             Absolute /lakehouse/default/... paths cause "400 Bad Request"
#             in Fabric notebooks. Strip prefix before mssparkutils calls,
#             keep original paths for spark.read.parquet().
#             Affected: read_landing_data, _find_newest_file_for_date,
#             _get_files_with_modification_time.
# =============================================================================

# ========== Landing Reader Classes ==========

class BaseLandingReader(ABC):
    """
    Base class for reading parquet data from landing zones.
    Provides common functionality for partition derivation and timestamp handling.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def get_partition_date_range(self, last_commit_timestamp: datetime) -> list:
        """
        Get all partition dates from last_commit_timestamp to today.
        Option B: Replaces get_read_partitions() — scans ALL future partitions,
        not just the commit-day + next day. MAX_DAYS_RANGE=90 prevents path explosion.
        
        Returns:
            list[datetime.date]: Dates from last_commit to today (inclusive)
        """
        MAX_DAYS_RANGE = 90
        
        if last_commit_timestamp is None:
            return []
        
        start_date = last_commit_timestamp.date()
        end_date = datetime.now().date()
        
        if start_date > end_date:
            logger.warning(
                f"last_commit_timestamp ({start_date}) liegt in der Zukunft. "
                f"Verwende heute ({end_date})."
            )
            return [end_date]
        
        date_range = (end_date - start_date).days + 1  # inclusive
        
        if date_range > MAX_DAYS_RANGE:
            logger.warning(
                f"Zeitspanne ({date_range} Tage) überschreitet Maximum ({MAX_DAYS_RANGE}). "
                f"Begrenze auf {MAX_DAYS_RANGE} Tage."
            )
            start_date = end_date - timedelta(days=MAX_DAYS_RANGE - 1)
        
        return [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    @abstractmethod
    def read_landing_data(
        self,
        source: str,
        tablename: str,
        timestamp_str: str,
        lakehouse_name: str = None,
        **kwargs,
    ) -> DataFrame:
        """
        Read parquet data from landing with partition filtering.
        This method should be implemented by platform-specific subclasses.
        """
        pass


class FabricLandingReader(BaseLandingReader):
    """
    Fabric implementation for reading landing parquet files.
    Uses path-based reading from /lakehouse/default/Files/landing/
    """

    def __init__(self, spark: SparkSession, verbose: bool = True):
        super().__init__(spark)
        self.verbose = verbose

    def read_landing_data(
        self,
        source: str,
        tablename: str,
        last_commit_timestamp,
        table_load_type: str,
        use_wildcard: bool,
    ) -> DataFrame:
        """Read parquet data from Fabric landing using path-based reading.
        
        Option B: Scans ALL partitions from last_commit_timestamp to today
        instead of just the commit-day + next day. Fallback to _find_latest_file
        was removed — full date-range coverage is exhaustive.
        """
        if last_commit_timestamp is None:
            return self._read_newest_files(source, tablename, use_wildcard)

        partition_dates = self.get_partition_date_range(last_commit_timestamp)
        if not partition_dates:
            return self._create_empty_df()

        path_parts = self._build_path_parts(source, tablename, use_wildcard)
        paths = self._build_partition_paths(path_parts, partition_dates)

        if not use_wildcard:
            from notebookutils import mssparkutils
            valid_paths = []
            for p in paths:
                # NOTE: mssparkutils.fs.exists() requires relative paths in Fabric.
                try:
                    if mssparkutils.fs.exists(p.replace("/lakehouse/default/", "")):
                        valid_paths.append(p)
                except Exception:
                    pass
            paths = valid_paths
            if not paths:
                return self._create_empty_df()

        if use_wildcard and table_load_type in ("full", "pseudo_incremental"):
            df = self._read_from_partitions_with_fallback(
                source, tablename, paths, last_commit_timestamp,
            )
        else:
            df = self._read_from_partitions(paths, last_commit_timestamp)

        return df

    def _read_newest_files(self, source: str, tablename: str, use_wildcard: bool) -> DataFrame:
        """Read the newest file from each subdirectory when last_commit_timestamp is None."""
        base_path = "/".join(self._build_path_parts(source))

        if use_wildcard:
            if not os.path.exists(base_path):
                return self._create_empty_df()

            sub_database_dirs = self._list_subdirectories(base_path)
            if not sub_database_dirs:
                return self._create_empty_df()

            dfs = []
            for sub_database_name in sub_database_dirs:
                sub_database_path = os.path.join(base_path, sub_database_name)
                latest_file = self._find_latest_file(sub_database_path, tablename)
                if self.verbose and latest_file:
                    logger.info(f"Latest file in {sub_database_path}: {latest_file}")
                if latest_file:
                    df = self._read_parquet_file(latest_file)
                    if df is not None:
                        dfs.append(df)

            if not dfs:
                return self._create_empty_df()
            return reduce(DataFrame.unionByName, dfs)
        else:
            latest_file = self._find_latest_file(base_path, tablename)
            if self.verbose and latest_file:
                logger.info(f"Latest file in {base_path}: {latest_file}")
            if latest_file:
                return self._read_parquet_file(latest_file) or self._create_empty_df()
            return self._create_empty_df()

    def _build_path_parts(self, source: str, tablename: str | None = None, use_wildcard: bool = False) -> list[str]:
        """Build path components for reading landing data."""
        path_parts = ["/lakehouse/default/Files", "landing", source]
        if use_wildcard:
            path_parts.append("*")
        if tablename:
            path_parts.append(tablename.lower())
        return path_parts

    def _build_partition_paths(self, path_parts: list[str], partition_dates: list) -> list[str]:
        """Build partition paths for all dates in the provided range.
        
        Option B: Now accepts a list of dates from get_partition_date_range()
        instead of just two day-tuples, enabling full-range partition scanning.
        """
        paths = []
        for date in partition_dates:
            partition_path = "/".join(path_parts + [str(date.year), f"{date.month:02d}", f"{date.day:02d}"])
            if partition_path not in paths:
                paths.append(partition_path)
        return paths

    def _build_day_path(self, base_path: str, tablename: str, check_date: datetime) -> str | None:
        """Build the day partition path for a given date."""
        try:
            parts = base_path.split("/") + [
                tablename.lower(), str(check_date.year),
                f"{check_date.month:02d}", f"{check_date.day:02d}",
            ]
            return "/".join(parts)
        except Exception:
            return None

    def _read_from_partitions(self, paths: list[str], last_commit_timestamp: datetime) -> DataFrame:
        """Read parquet files from partition paths and apply timestamp filter."""
        if not paths:
            return self._create_empty_df()

        try:
            spark_paths = [p.replace("/lakehouse/default/", "") for p in paths]
            df = self.spark.read.parquet(*spark_paths)
            if df.rdd.isEmpty():
                return self._create_empty_df()
            try:
                if "_load_timestamp" in df.columns:
                    filtered_df = df.filter(F.col("_load_timestamp") > last_commit_timestamp)
                    return filtered_df if not filtered_df.rdd.isEmpty() else self._create_empty_df()
                else:
                    return df
            except Exception:
                return self._create_empty_df()
        except Exception:
            dfs = []
            for path in paths:
                df = self._read_parquet_file(path)
                if df is not None:
                    filtered_df = df.filter(F.col("_load_timestamp") > last_commit_timestamp)
                    if not filtered_df.rdd.isEmpty():
                        dfs.append(filtered_df)

            if not dfs:
                return self._create_empty_df()
            return reduce(DataFrame.unionByName, dfs)

    def _read_from_partitions_with_fallback(self, source: str, tablename: str, paths: list[str],
                                           last_commit_timestamp: datetime) -> DataFrame:
        """Read from partitions, checking each subdirectory individually.
        
        Option B: Fallback to _find_latest_file removed — date-range scanning now
        covers all partitions exhaustively via get_partition_date_range().
        """
        base_path = "/".join(self._build_path_parts(source))
        dfs = []

        if not os.path.exists(base_path):
            return self._create_empty_df()

        sub_database_dirs = self._list_subdirectories(base_path)
        if not sub_database_dirs:
            return self._create_empty_df()

        for sub_database_name in sub_database_dirs:
            partition_df = self._read_partition_for_subdirectory(sub_database_name, paths, last_commit_timestamp)
            if partition_df is not None and not partition_df.rdd.isEmpty():
                dfs.append(partition_df)

        if not dfs:
            return self._create_empty_df()
        return reduce(DataFrame.unionByName, dfs)

    def _read_partition_for_subdirectory(self, sub_database_name: str, paths: list[str],
                                        last_commit_timestamp: datetime) -> DataFrame | None:
        """Read from expected partitions for a specific subdirectory."""
        valid_paths = []
        for path in paths:
            sub_path = path.replace("/*/", f"/{sub_database_name}/")
            if os.path.exists(sub_path):
                valid_paths.append(sub_path)

        if not valid_paths:
            return None

        try:
            spark_paths = [p.replace("/lakehouse/default/", "") for p in valid_paths]
            df = self.spark.read.parquet(*spark_paths)
            if df is None or df.rdd.isEmpty():
                return None
            filtered_df = df.filter(F.col("_load_timestamp") > last_commit_timestamp)
            return filtered_df if not filtered_df.rdd.isEmpty() else None
        except Exception:
            for sub_path in valid_paths:
                try:
                    df = self._read_parquet_file(sub_path)
                    if df is not None and not df.rdd.isEmpty():
                        filtered_df = df.filter(F.col("_load_timestamp") > last_commit_timestamp)
                        if not filtered_df.rdd.isEmpty():
                            return filtered_df
                except Exception:
                    pass
            return None

    def _find_latest_file(self, base_path: str, tablename: str, max_days_back: int = 365) -> str | None:
        """Find the latest file by searching through recent days."""
        current_date = datetime.now()

        for days_back in range(max_days_back):
            check_date = current_date - timedelta(days=days_back)
            newest_file = self._find_newest_file_for_date(base_path, tablename, check_date)
            if newest_file:
                return newest_file

        return None

    def _find_newest_file_for_date(self, base_path: str, tablename: str, check_date: datetime) -> str | None:
        """Find the newest file for a specific date in a single directory."""
        day_path = self._build_day_path(base_path, tablename, check_date)
        if not day_path:
             return None
        
        from notebookutils import mssparkutils
        # NOTE: Same fs.exists path-prefix issue — strip /lakehouse/default/.
        if not mssparkutils.fs.exists(day_path.replace("/lakehouse/default/", "")):
            return None

        files_with_mtime = self._get_files_with_modification_time(day_path)
        if files_with_mtime:
            files_with_mtime.sort(key=lambda x: x[1], reverse=True)
            newest_file = files_with_mtime[0][0]
            if self.verbose:
                logger.info(f"Found newest file for {check_date.date()}: {newest_file}")
            return newest_file

        return None

    def _read_parquet_file(self, path: str) -> DataFrame | None:
        """Read a parquet file, returning None if it doesn't exist or is empty."""
        read_path = path
        if "/lakehouse/default/" in path:
            read_path = path.replace("/lakehouse/default/", "")
        
        if self.verbose:
            logger.info(f"Reading parquet file: {read_path}")
        
        try:
            df = self.spark.read.parquet(read_path)
            if df.rdd.isEmpty():
                 return None
            return df
        except Exception:
            return None

    def _list_subdirectories(self, base_path: str) -> list[str]:
        """List all subdirectories in the given base path."""
        try:
            return [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]
        except Exception:
            return []

    def _get_files_with_modification_time(self, day_path: str) -> list[tuple[str, float]]:
        """Get all files in a directory with their modification times."""
        from notebookutils import mssparkutils
        files_with_mtime = []

        try:
            # NOTE: Same fs.ls path-prefix issue — strip /lakehouse/default/.
            entries = mssparkutils.fs.ls(day_path.replace("/lakehouse/default/", ""))
            for entry in entries:
                if not entry.name.endswith(".parquet"):
                    continue
                try:
                    # Entry Modification Time required
                    mtime = entry.modifyTime / 1000.0
                    #mtime = entry.modificationTime / 1000.0
                    files_with_mtime.append((entry.path, mtime))
                except (OSError, ValueError):
                    continue

        except Exception:
            pass

        return files_with_mtime

    def _create_empty_df(self) -> DataFrame:
        """Create an empty DataFrame with the required schema."""
        return self.spark.createDataFrame([], schema=StructType().add("_load_timestamp", "timestamp"))


# Create alias for backward compatibility
LandingReader = FabricLandingReader


# ========== Logging Utility ==========

class PlatformLogger:
    """
    Provides a standard way to get logger instances with basic configuration.
    """

    def __init__(self, name: str):
        self.name = name
        self._logger = None

    def get_logger(self) -> logging.Logger:
        if self._logger is None:
            self._logger = logging.getLogger(self.name)
            if not self._logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
                handler.setFormatter(formatter)
                self._logger.addHandler(handler)
                self._logger.setLevel(logging.INFO)
        return self._logger


# Initialize logger for histlanding
logger = PlatformLogger(name="hist_landing").get_logger()


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


def generate_zero_hash_for_nulls():
    """
    Generate a zero hash for null values.
    
    Returns:
        Column expression for zero hash
    """
    zero_hash_hex = "0" * 64  # 64 hex characters (256 bits)
    return F.lit(zero_hash_hex)


# ========== Spark Session Creation ==========

def create_spark_session() -> SparkSession:
    """
    Create and configure Spark session with legacy parquet rebase modes.
    
    Returns:
        SparkSession: Configured Spark session
    """
    return (
        SparkSession.builder.config(
            "spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED"
        )
        .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )


# ========== Landing Data Reading ==========

def read_from_landing(
    spark: SparkSession,
    source_system: str,
    tablename: str,
    table_load_type: str,
    last_commit_timestamp,
    use_wildcard: bool = False,
    verbose: bool = True,
) -> DataFrame:
    """
    Read data from landing zone using the LandingReader.
    Cleans column names by removing 'records.' prefix and replacing dots with underscores.
    
    Args:
        spark: SparkSession instance
        source_system: Source system name
        tablename: Table name to read
        table_load_type: Load type (full, pseudo_incremental, incremental)
        last_commit_timestamp: Last commit timestamp for filtering
        use_wildcard: Whether to use wildcard pattern matching
        verbose: Whether to log parquet file paths
        
    Returns:
        DataFrame: Source data from landing zone with cleaned column names
    """
    # Use LandingReader defined in this module
    landing_reader = LandingReader(spark, verbose=verbose)
    
    source_df = landing_reader.read_landing_data(
        source=source_system,
        tablename=tablename,
        table_load_type=table_load_type,
        last_commit_timestamp=last_commit_timestamp,
        use_wildcard=use_wildcard,
    )

    # Clean column names: remove 'records.' prefix and replace dots with underscores
    # Clean column names: remove 'records.', 'd.results.', and 'd.' prefixes and replace dots with underscores
    cleaned_columns = []
    for col_name in source_df.columns:
        new_name = col_name
        # Remove known prefixes
        for prefix in ["records.", "d.results.", "d."]:
            if new_name.startswith(prefix):
                new_name = new_name.replace(prefix, "", 1)
                break
        
        # Replace any remaining dots with underscores
        new_name = new_name.replace(".", "_")
        cleaned_columns.append(new_name)
    
    # Rename all columns
    for old_name, new_name in zip(source_df.columns, cleaned_columns):
        if old_name != new_name:
            source_df = source_df.withColumnRenamed(old_name, new_name)

    return source_df


# ========== Historized Landing Writing ==========

def write_to_historized_landing(
    spark: SparkSession,
    database_name: str,
    source_system: str,
    table_name: str,
    source_df: DataFrame,
    table_load_type: str,
    primary_keys: list[str],
    payload_columns: list[str],
    last_commit_timestamp=None,
    write_mode: str = None,
) -> None:
    """
    Write data to historized landing with hash calculation.
    
    Args:
        spark: SparkSession
        database_name: Database name
        source_system: Source system name (e.g., 'zvoove')
        table_name: Table name (e.g., 'stunden')
        source_df: Source DataFrame to process
        table_load_type: Load type (e.g., 'full', 'pseudo_incremental', 'incremental')
        primary_keys: List of primary key columns for merge
        payload_columns: List of columns to include in hash calculation
        last_commit_timestamp: Optional timestamp string for filter condition
        write_mode: Optional explicit write mode override (e.g., 'upsert').
                    If not provided, derived from table_load_type.
    """
    # DeltaLakeHandler is already in namespace after %run master/utils_delta_handler
    
    # Calculate hash for change detection
    df_hashed = source_df.withColumn(
        "hash_diff", generate_sha2_hash(hash_input_columns=payload_columns)
    )

    # Determine write mode:
    # - If explicitly provided (e.g. "upsert"), use it directly.
    # - Otherwise derive from table_load_type.
    if write_mode is None:
        write_mode = (
            "upsert_and_delete"
            if table_load_type in ("full", "pseudo_incremental")
            else "append_new_rows"
        )

    # Write to lakehouse with the respective write mode
    target_table = DeltaLakeHandler(
        spark=spark,
        database=database_name,
        schema="historized_landing",
        table=f"{source_system.lower()}_{table_name.lower()}",
    )

    # Build filter condition only when needed.
    # For upsert mode the DataFrame is already pre-filtered by _load_timestamp,
    # so the default "1 = 1" is sufficient and avoids redundant predicates.
    filter_condition = (
        f"source._load_timestamp > '{last_commit_timestamp}'"
        if last_commit_timestamp and write_mode != "upsert"
        else None
    )

    write_kwargs = dict(
        df=df_hashed,
        mode=write_mode,
        merge_keys=primary_keys,
    )
    if filter_condition:
        write_kwargs["filter_condition"] = filter_condition

    # Enable Change Data Feed for new historized landing tables.
    target_table.write_to_delta_lake(**write_kwargs, cdc_target=True)


# ========== Table Processing ==========

def aggregate_multi_select_rows(
    df: DataFrame, primary_keys: list[str], multi_select_columns: list[str]
) -> DataFrame:
    """
    Aggregates rows by primary keys, concatenating multi-select values.
    Uses the first available value for all other columns.
    """
    # Sort by load timestamp descending to ensure first() picks the newest record
    df_sorted = df.orderBy(F.col("_load_timestamp").desc())
    
    other_columns = [c for c in df.columns if c not in primary_keys and c not in multi_select_columns]
    
    agg_exprs = []
    for col in multi_select_columns:
        # Collect distinct values and concat with separator
        agg_exprs.append(F.concat_ws(", ", F.collect_set(col)).alias(col))
        
    for col in other_columns:
        # Pick the first value (which is the newest due to sorting)
        agg_exprs.append(F.first(col).alias(col))
        
    return df_sorted.groupBy(primary_keys).agg(*agg_exprs)


def extract_fields_from_json(
    df: DataFrame, json_column: str, fields: list[str], json_extraction_paths: dict = None
) -> DataFrame:
    """
    Extracts specific fields from a JSON string column and adds them as new columns.
    
    Args:
        df: Input DataFrame
        json_column: Name of the column containing JSON strings
        fields: List of field names to extract
        
    Returns:
        DataFrame with extracted columns
    """
    json_extraction_paths = json_extraction_paths or {}
    for field in fields:
        # Use custom extraction path if defined, otherwise use default
        path = json_extraction_paths.get(field, f"$.{field}")
        df = df.withColumn(field, F.get_json_object(F.col(json_column), path))
    return df


def process_historized_table(
    spark: SparkSession,
    database_name: str,
    source_system: str,
    tablename: str,
    table_load_type: str,
    primary_keys: list[str],
    payload_columns: list[str],
    use_wildcard: bool = False,
    multi_select_columns: list[str] = None,
    json_payload_column: str = None,
    json_extraction_paths: dict = None,
    verbose: bool = True,
    write_mode: str = None,
) -> tuple:
    """
    Process a single table configuration for historized landing.
    
    Args:
        spark: SparkSession instance
        database_name: Database name for the target table
        source_system: Source system name (e.g., 'zvoove')
        tablename: Table name to process
        table_load_type: Load type (full, pseudo_incremental, incremental)
        primary_keys: List of primary key columns for merge
        payload_columns: List of columns to include in hash calculation
        use_wildcard: Whether to use wildcard pattern matching
        verbose: Whether to log parquet file paths
        
    Returns:
        tuple: (source_system, tablename, success_status, error_message)
    """
    try:
        target_table = DeltaLakeHandler(
            spark=spark,
            database=database_name,
            schema="historized_landing",
            table=f"{source_system.lower()}_{tablename.lower()}",
        )
        
        if target_table.delta_table_exists():
            last_commit_timestamp = target_table.get_last_commit_timestamp()
        else:
            last_commit_timestamp = None

        source_df = read_from_landing(
            spark=spark,
            source_system=source_system,
            tablename=tablename,
            table_load_type=table_load_type,
            last_commit_timestamp=last_commit_timestamp,
            use_wildcard=use_wildcard,
            verbose=verbose,
        )

        if source_df.rdd.isEmpty():
            logger.warning(
                f"No data found for table {source_system}_{tablename.lower()}"
            )
            return (source_system, tablename, True, None)

# Extract from JSON payload if configured
        if json_payload_column:
            if json_payload_column not in source_df.columns:
                logger.info(
                    f"json_payload_column '{json_payload_column}' not found in schema for "
                    f"{source_system}_{tablename.lower()}, proceeding with flat columns"
                )
            else:
                # Check if all values in json_payload_column are NULL
                non_null_count = source_df.filter(F.col(json_payload_column).isNotNull()).count()
                if non_null_count == 0:
                    logger.warning(
                        f"No valid records found for table {source_system}_{tablename.lower()} "
                        f"({json_payload_column} column contains only NULL values)"
                    )
                    return (source_system, tablename, True, None)
                
                # Check if the column contains a JSON array and explode it
                first_val = source_df.select(json_payload_column).first()[0]
                if first_val and first_val.strip().startswith("["):
                    # Use ARRAY<STRING> to parse the array of objects as strings
                    source_df = source_df.withColumn(
                        "_exploded", 
                        F.explode(F.from_json(F.col(json_payload_column), "ARRAY<STRING>"))
                    )
                    extraction_column = "_exploded"
                else:
                    extraction_column = json_payload_column

                # Extract missing columns from JSON payload
                cols_to_extract = list(set(primary_keys + payload_columns))
                source_df = extract_fields_from_json(source_df, extraction_column, cols_to_extract, json_extraction_paths)
                
                # Drop the JSON column and intermediate exploded column to avoid schema bloat
                source_df = source_df.drop(json_payload_column, "_exploded")

        # Handle multi-select aggregation or simple deduplication
        if multi_select_columns:
            source_df = aggregate_multi_select_rows(source_df, primary_keys, multi_select_columns)
        else:
            # Remove duplicates by keeping the newest row per primary key based on _load_timestamp
            window_spec = Window.partitionBy(primary_keys).orderBy(
                F.col("_load_timestamp").desc()
            )
            source_df = (
                source_df.withColumn("_row_num", F.row_number().over(window_spec))
                .filter(F.col("_row_num") == 1)
                .drop("_row_num")
            )

        write_to_historized_landing(
            spark,
            database_name,
            source_system,
            tablename,
            source_df,
            table_load_type,
            primary_keys,
            payload_columns,
            last_commit_timestamp,
            write_mode=write_mode,
        )
        return (source_system, tablename, True, None)

    except Exception as e:
        # Fehler-Dict für strukturierten Notebook-Exit (kein RuntimeError)
        return build_error_payload(source_system, tablename, "histlanding", e)


# ========== Parallel Processing ==========

def process_all_historized_tables(
    spark: SparkSession, 
    table_configs: list[dict],
    verbose: bool = True,
) -> dict:
    """
    Process all historized landing tables in parallel.
    
    Args:
        spark: SparkSession instance
        table_configs: List of table configuration dictionaries
        verbose: Whether to log parquet file paths
        
    Returns:
        dict: Summary with 'successful' and 'failed' table lists
    """
    num_tables = len(table_configs)

    logger.info(
        f"Starting parallel processing of {num_tables} historized landing tables..."
    )

    # Use ThreadPoolExecutor for parallel processing (I/O bound operations)
    # Handle empty config list case
    max_workers = min(num_tables, 4) if num_tables > 0 else 1
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all table processing tasks
        future_to_config = {
            executor.submit(process_historized_table, spark, verbose=verbose, **cfg): cfg
            for cfg in table_configs
        }

        successful_tables = []
        failed_tables = []

        # Erfolg: Tupel (source, table, True, None) | Fehler: Dict von build_error_payload()
        for future in as_completed(future_to_config):
            cfg = future_to_config[future]
            try:
                result = future.result()
                if isinstance(result, dict):
                    # Fehler-Dict von build_error_payload() direkt übernehmen
                    failed_tables.append(result)
                else:
                    source_system, tablename, success, error_msg = result
                    if success:
                        successful_tables.append(f"{source_system}_{tablename.lower()}")
                    else:
                        # Fallback für unerwartete Tupel-Fehler (Sicherheitsnetz)
                        failed_tables.append(
                            build_error_payload(source_system, tablename, "histlanding", error_msg)
                        )
            except Exception as e:
                # Use config dict to get table info since future.result() failed
                src_sys = cfg.get("source_system", "unknown")
                tbl_name = cfg.get("tablename", "unknown")
                failed_tables.append(
                    build_error_payload(src_sys, tbl_name, "histlanding", str(e))
                )

    # Log summary
    logger.info("=== Historized Landing Processing Summary ===")
    logger.info(f"Successfully processed: {len(successful_tables)} tables")
    logger.info(f"Failed to process: {len(failed_tables)} tables")

    if successful_tables:
        logger.info(f"Successful tables: {', '.join(successful_tables)}")

    if failed_tables:
        logger.warning("Failed tables:")
        for entry in failed_tables:
            logger.warning(f"  - {entry['source_table']}/{entry['target_table']}: {entry['error_message']}")

    # Kein RuntimeError mehr — Pipeline bleibt grün, Fehler werden im Exit-Payload zurückgegeben
    return {"successful": successful_tables, "failed_tables": failed_tables}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### JIRA UTILS (Historized Landing)

# CELL ********************

# List all subdirectories to identify project keys.
def get_jira_project_keys(base_path):
    if not os.path.exists(base_path):
        return []
    return [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]

# Find the latest yyyy/mm/dd folder for a given project path.
def get_latest_daily_folder_for_project(project_path):
    latest_folder = None
    latest_date_str = ""
    
    if not os.path.exists(project_path):
        return None
        
    for year_dir in os.listdir(project_path):
        year_path = os.path.join(project_path, year_dir)
        if not os.path.isdir(year_path) or not year_dir.isdigit() or len(year_dir) != 4:
            continue
            
        for month_dir in os.listdir(year_path):
            month_path = os.path.join(year_path, month_dir)
            if not os.path.isdir(month_path) or not month_dir.isdigit() or len(month_dir) != 2:
                continue
                
            for day_dir in os.listdir(month_path):
                day_path = os.path.join(month_path, day_dir)
                if not os.path.isdir(day_path) or not day_dir.isdigit() or len(day_dir) != 2:
                    continue
                    
                date_str = f"{year_dir}-{month_dir}-{day_dir}"
                if date_str > latest_date_str:
                    latest_date_str = date_str
                    latest_folder = day_path
                    
    return latest_folder

# Get all parquet files in a specific directory.
def get_parquet_files_in_folder(folder_path):
    if not folder_path or not os.path.exists(folder_path):
        return []
    return [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".parquet")]

# Read multiple parquet files into a unioned DataFrame with schema-safe merging.
def read_and_union_parquet_files(spark, file_paths, logger=None):
    if not file_paths:
        return None
    spark_paths = [f.replace("/lakehouse/default/", "") for f in file_paths]

    # OPTIMIZED: Single distributed read replaces per-file loop + unionByName
    result_df = spark.read.parquet(*spark_paths)

    if logger:
        final_cf = [c for c in result_df.columns if "customfield" in c.lower()]
        logger.info(f"Final custom fields in unioned data: {final_cf}")

    return result_df

def normalize_column_names(df):
    """
    Simplest possible normalization: 
    Replaces literal dots in column names with underscores and converts to lowercase.
    Handles the flat schema provided by the Fabric Copy activity.
    """
    import pyspark.sql.functions as F
    
    # Get all current column names
    current_columns = df.columns
    # Create the list of new column names
    new_columns = [c.replace(".", "_").lower() for c in current_columns]
    
    # Return the DataFrame with the new column names applied
    return df.toDF(*new_columns)


def unnest_jira_comments(df: DataFrame, unnest_config: dict) -> DataFrame:
    """
    Unnest a JSON array column into multiple rows based on metadata configuration.
    Nested fields are automatically aliased by replacing dots with underscores.
    
    Args:
        df: Input DataFrame
        unnest_config: Configuration dict with 'source_column' and 'nested_columns' list
    """
    import pyspark.sql.functions as F
    from pyspark.sql.types import (ArrayType, BooleanType, StringType,
                                   StructType)

    source_col = unnest_config.get("source_column")
    nested_cols = unnest_config.get("nested_columns")

    if not source_col or source_col not in df.columns or not nested_cols:
        return df

    # Define schema for Jira comments based on the nested structure
    # This remains robust to ensure correct parsing of the Jira JSON format
    comment_schema = ArrayType(StructType()
        .add("id", StringType())
        .add("body", StringType())
        .add("created", StringType())
        .add("updated", StringType())
        .add("self", StringType())
        .add("author", StructType()
            .add("name", StringType())
            .add("key", StringType())
            .add("displayName", StringType())
        )
        .add("updateAuthor", StructType()
            .add("name", StringType())
            .add("key", StringType())
            .add("displayName", StringType())
        )
        .add("jsdPublic", BooleanType())
    )

    # Parse JSON and explode
    df = df.withColumn("c_array", F.from_json(F.col(source_col), comment_schema))
    df = df.withColumn("c", F.explode(F.col("c_array")))

    # Dynamic selection and auto-aliasing based on nested_columns list
    unnested_aliases = {path.replace(".", "_").lower() for path in nested_cols}
    original_cols = [
        F.col(c) for c in df.columns
        if c not in unnested_aliases and c not in {source_col, "c_array", "c"}
    ]

    select_exprs = original_cols
    for path in nested_cols:
        alias = path.replace(".", "_").lower()
        select_exprs.append(F.col(f"c.{path}").alias(alias))

    df = df.select(*select_exprs)

    return df

# Write Jira data to Lakehouse using DeltaLakeHandler for idempotency.
def write_jira_data_to_lakehouse(spark, df, database, schema, table, merge_keys, payload_cols=None):
    if not payload_cols:
        payload_cols = [c for c in df.columns if c not in ["_load_timestamp"]]
    
    df_hashed = df.withColumn(
        "hash_diff", generate_sha2_hash(hash_input_columns=payload_cols)
    )

    handler = DeltaLakeHandler(
        spark=spark,
        database=database,
        schema=schema,
        table=table
    )

    handler.write_to_delta_lake(
        df=df_hashed,
        mode="upsert",
        merge_keys=merge_keys,
        hash_diff_column_name="hash_diff"
    )


def _get_histlanding_watermark(spark, source_system, tablename):
    try:
        query = f"SELECT MAX(_dlt_commit_timestamp) AS last_commit FROM lh_fabric_1.historized_landing.{source_system}_{tablename}"
        result = spark.sql(query).collect()[0][0]
        if result:
            return result.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    except Exception:
        pass
    return None


def get_jira_landing_files_since_timestamp(project_path: str, jira_module: str, project_key: str, since_timestamp: str = None) -> list:
    import os
    import re
    from datetime import datetime, timedelta

    suffix = ".parquet"
    iso_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}T[\d:.-]+Z?$")
    comment_ts_pattern = re.compile(r"_(\d{8}_\d{6})" + re.escape(suffix) + r"$")
    project_lower = project_key.lower()

    since_dt = None
    if since_timestamp:
        ts_clean = since_timestamp.replace("Z", "")
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H-%M-%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"):
            try:
                since_dt = datetime.strptime(str(ts_clean), fmt)
                break
            except ValueError:
                continue

    matching_files = []

    if not os.path.exists(project_path):
        return matching_files

    if since_dt is not None:
        # OPTIMIZED: Only scan day folders from since_dt.date() through today
        start_date = since_dt.date()
        end_date = datetime.now().date()
        current = start_date
        while current <= end_date:
            day_path = os.path.join(project_path, str(current.year), f"{current.month:02d}", f"{current.day:02d}")
            if os.path.exists(day_path):
                for f_name in os.listdir(day_path):
                    if not f_name.endswith(suffix):
                        continue

                    if jira_module == "issues":
                        prefix = f"{project_lower}_"
                        if f_name.startswith(prefix) and f_name.endswith(suffix):
                            ts = f_name[len(prefix):-len(suffix)]
                            if iso_pattern.match(ts):
                                ts_cmp = ts.replace("Z", "")
                                if "T" in ts_cmp:
                                    d, t = ts_cmp.split("T", 1)
                                    t = t.replace("-", ":")
                                    ts_cmp = f"{d}T{t}"
                                try:
                                    file_dt = datetime.fromisoformat(ts_cmp)
                                    if file_dt > since_dt:
                                        matching_files.append(os.path.join(day_path, f_name))
                                except ValueError:
                                    pass
                    else:
                        if not (f_name.startswith("comments_") and f_name.endswith(suffix)):
                            continue
                        ts_match = comment_ts_pattern.search(f_name)
                        if ts_match:
                            ts_raw = ts_match.group(1)
                            ts = f"{ts_raw[:4]}-{ts_raw[4:6]}-{ts_raw[6:8]}T{ts_raw[9:11]}:{ts_raw[11:13]}:{ts_raw[13:15]}"
                            file_dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")
                            if file_dt > since_dt:
                                matching_files.append(os.path.join(day_path, f_name))
                        else:
                            prefix = f"comments_{project_lower}_"
                            if f_name.startswith(prefix):
                                ts = f_name[len(prefix):-len(suffix)]
                                if iso_pattern.match(ts):
                                    ts_cmp = ts.replace("Z", "")
                                    if "T" in ts_cmp:
                                        d, t = ts_cmp.split("T", 1)
                                        t = t.replace("-", ":")
                                        ts_cmp = f"{d}T{t}"
                                    try:
                                        file_dt = datetime.fromisoformat(ts_cmp)
                                        if file_dt > since_dt:
                                            matching_files.append(os.path.join(day_path, f_name))
                                    except ValueError:
                                        pass
            current += timedelta(days=1)
    else:
        # Full load fallback: scan entire tree
        for year_dir in sorted(os.listdir(project_path)):
            year_path = os.path.join(project_path, year_dir)
            if not os.path.isdir(year_path):
                continue
            for month_dir in sorted(os.listdir(year_path)):
                month_path = os.path.join(year_path, month_dir)
                if not os.path.isdir(month_path):
                    continue
                for day_dir in sorted(os.listdir(month_path)):
                    day_path = os.path.join(month_path, day_dir)
                    if not os.path.isdir(day_path):
                        continue
                    for f_name in os.listdir(day_path):
                        if not f_name.endswith(suffix):
                            continue

                        if jira_module == "issues":
                            prefix = f"{project_lower}_"
                            if f_name.startswith(prefix) and f_name.endswith(suffix):
                                ts = f_name[len(prefix):-len(suffix)]
                                if iso_pattern.match(ts):
                                    matching_files.append(os.path.join(day_path, f_name))
                        else:
                            if not (f_name.startswith("comments_") and f_name.endswith(suffix)):
                                continue
                            ts_match = comment_ts_pattern.search(f_name)
                            if ts_match:
                                matching_files.append(os.path.join(day_path, f_name))
                            else:
                                prefix = f"comments_{project_lower}_"
                                if f_name.startswith(prefix):
                                    ts = f_name[len(prefix):-len(suffix)]
                                    if iso_pattern.match(ts):
                                        matching_files.append(os.path.join(day_path, f_name))

    return matching_files


def process_jira_table(
    spark: SparkSession,
    database_name: str,
    source_system: str,
    tablename: str,
    table_load_type: str,
    primary_keys: list[str] | str,
    payload_columns: list[str],
    verbose: bool = True,
    since_timestamp: str = None,
    project_keys: list[str] = None,
    **kwargs
) -> tuple:
    """
    Process a single Jira table for historized landing.
    
    Args:
        spark: SparkSession
        database_name: Target database
        source_system: Source system name (jira)
        tablename: Table name (issues, comments)
        table_load_type: Load type
        primary_keys: Primary keys for merge
        payload_columns: Columns for hash calculation
        project_keys: Optional list of project keys to process (default: scan all from landing)
    """
    import os
    import pyspark.sql.functions as F
    
    try:
        base_path = f"/lakehouse/default/Files/landing/jira/{tablename}/"
        target_schema = "historized_landing"
        target_table = f"{source_system}_{tablename}"
        
        # Ensure primary_keys is a list and includes _load_timestamp for historization
        if isinstance(primary_keys, str):
            pk_list = [primary_keys]
        else:
            pk_list = primary_keys.copy()
            
        if "_load_timestamp" not in pk_list:
            pk_list.append("_load_timestamp")
            
        # Get project keys (use explicit list or scan landing directory)
        projects = project_keys if project_keys is not None else get_jira_project_keys(base_path)
        
        latest_files = []
        for project_key in projects:
            project_path_candidates = [
                os.path.join(base_path, project_key),
                os.path.join(base_path, project_key.lower()),
                os.path.join(base_path, project_key.upper())
            ]
            
            project_path = None
            for p in project_path_candidates:
                if os.path.exists(p):
                    project_path = p
                    break
            
            if project_path:
                project_files = get_jira_landing_files_since_timestamp(
                    project_path=project_path,
                    jira_module=tablename,
                    project_key=project_key,
                    since_timestamp=since_timestamp
                )
                latest_files.extend(project_files)
                if verbose:
                    logger.info(f"Jira {tablename}: project={project_key}, files={len(project_files)}")
        
        if not latest_files:
            logger.warning(f"No valid parquet files found for Jira {tablename}")
            return (source_system, tablename, True, None)

        if verbose:
            logger.info(f"Jira {tablename}: total files={len(latest_files)}")
            
        df = read_and_union_parquet_files(spark, latest_files, logger=logger if verbose else None)
        
        if df:
            # Normalize and add _load_timestamp
            df = normalize_column_names(df)
            
            # Remove redundant complex root column after API flattening
            if "issues" in df.columns:
                df = df.drop("issues")
            
            # Dynamic schema detection for Jira comments:
            # Two possible source schemas exist:
            #   Schema 1 (Initial / Full Load): Flat structure, no 'comments' column.
            #     Columns like author_name, author_display_name are already flat.
            #   Schema 2 (Pipeline / Daily): Nested JSON string in a 'comments' column.
            #     The JSON must be parsed and unnested.
            # Detection is data-driven: if the 'comments' column exists, it's Schema 2.
            unnest_config = kwargs.get("unnest_config")
            if "comments" in df.columns and unnest_config:
                logger.info("Nested Jira comments schema detected (Schema 2) – applying unnest.")
                df = unnest_jira_comments(df, unnest_config)
                # Drop metadata columns that are only present in Schema 2 and not needed in the target.
                metadata_cols = [c for c in ["startat", "maxresults", "total"] if c in df.columns]
                if metadata_cols:
                    df = df.drop(*metadata_cols)
            elif unnest_config:
                logger.info("Flat Jira comments schema detected (Schema 1) – skipping unnest. "
                            "Missing payload columns (e.g. author_key, jsdpublic) will be filled with NULL later.")
            
            # Map primary keys to normalized names if needed
            # In Jira, 'key' becomes 'issues_key' or similar in some cases? 
            # Actually, the notebook manually used "issues_key" or "issue_key".
            # I will assume the config primary_keys are correct AFTER normalization.
            
            # Add _load_timestamp dynamically based on the source filename
            df = df.withColumn(
                "filename", F.element_at(F.split(F.input_file_name(), "/"), -1)
            ).withColumn(
                "timestamp_str", F.element_at(F.split(F.replace(F.col("filename"), F.lit(".parquet"), F.lit("")), "_"), -1)
            ).withColumn(
                "_load_timestamp", F.to_timestamp(F.col("timestamp_str"))
            ).drop("filename", "timestamp_str")

            # Remove duplicates by keeping the first row per primary key and timestamp
            # This is necessary because unioning across project folders can introduce duplicates
            window_spec = Window.partitionBy(pk_list).orderBy(F.lit(1))
            df = (
                df.withColumn("_row_num", F.row_number().over(window_spec))
                .filter(F.col("_row_num") == 1)
                .drop("_row_num")
            )

            # Ensure all configured payload columns exist in the DataFrame.
            # If a custom field or sparsely populated field is missing from the Parquet files,
            # add it with NULL values to prevent hash function resolution errors.
            for col_name in payload_columns:
                if col_name not in df.columns:
                    df = df.withColumn(col_name, F.lit(None).cast("string"))

            # Merge into target
            write_jira_data_to_lakehouse(
                spark=spark,
                df=df,
                database=database_name,
                schema=target_schema,
                table=target_table,
                merge_keys=pk_list,
                payload_cols=payload_columns
            )
            logger.info(f"Jira {tablename}: completed successfully")
            return (source_system, tablename, True, None)
        else:
            logger.info(f"Jira {tablename}: completed — no data to process")
            return (source_system, tablename, True, "No data to process")
            
    except Exception as e:
        logger.error(f"Jira {tablename}: failed — {e}")
        return build_error_payload(source_system, tablename, "histlanding", e)


def process_all_jira_tables(
    spark: SparkSession, 
    table_configs: list[dict],
    verbose: bool = True,
) -> dict:
    """
    Process all Jira historized landing tables.
    """
    num_tables = len(table_configs)
    logger.info(f"Starting processing of {num_tables} Jira histlanding tables...")

    successful_tables = []
    failed_tables = []

    for cfg in table_configs:
        try:
            cfg["since_timestamp"] = _get_histlanding_watermark(spark, cfg["source_system"], cfg["tablename"])
            result = process_jira_table(spark, verbose=verbose, **cfg)
            if isinstance(result, dict):
                # Fehler-Dict von build_error_payload()
                failed_tables.append(result)
            else:
                source_system, tablename, success, error_msg = result
                if success:
                    successful_tables.append(f"{source_system}_{tablename}")
                else:
                    failed_tables.append(build_error_payload(source_system, tablename, "histlanding", error_msg))
        except Exception as e:
            full_table_name = f"jira_{cfg.get('tablename', 'unknown')}"
            logger.error(f"Unexpected error processing {full_table_name}: {str(e)}", exc_info=True)
            failed_tables.append(build_error_payload("jira", cfg.get('tablename', 'unknown'), "histlanding", str(e)))

    # Log summary
    logger.info("=== Jira Histlanding Processing Summary ===")
    logger.info(f"Successfully processed: {len(successful_tables)} tables")
    if failed_tables:
        logger.warning(f"Failed to process: {len(failed_tables)} tables")
        for entry in failed_tables:
            logger.warning(f"  - {entry['source_table']}/{entry['target_table']}: {entry['error_message']}")

    return {"successful": successful_tables, "failed_tables": failed_tables}

# Ensures consistent timestamp metadata across all historized landing tables.
def add_load_timestamp(df, logging_date):
    return df.withColumn("_load_timestamp", F.to_timestamp(F.lit(logging_date)))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
