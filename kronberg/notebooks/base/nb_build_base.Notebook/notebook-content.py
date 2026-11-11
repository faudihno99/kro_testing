# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "31bbed25-ed5e-4417-9f59-612f87ec15cb",
# META       "default_lakehouse_name": "lh_fabric_dev",
# META       "default_lakehouse_workspace_id": "54f879c8-8ddb-4e05-ad22-1e9d10ef836a",
# META       "known_lakehouses": [
# META         {
# META           "id": "31bbed25-ed5e-4417-9f59-612f87ec15cb"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "d028deaf-a1f1-4960-b98e-54c8e1796cef",
# META       "workspaceId": "54f879c8-8ddb-4e05-ad22-1e9d10ef836a"
# META     }
# META   }
# META }

# CELL ********************

# Import required packages
import sys
import os
import json
from datetime import datetime, timezone

# Activate CDC
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")

# Set consistent Date Handling in Spark
# Correctly handling reads and write for older timestamps
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# Revert to pre-Spark 3.0 datetime parsing to prevent the job from crashing 
# on structurally invalid, out-of-bounds dates in the source data (e.g., year '10013').
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# PARAMETERS CELL ********************

# Define parameters
p_sources_to_run = ["sharepoint", "sugar", "sap", "jira"]
p_business_units_to_run = ["Korea", "Taiwan", "USA", "France", "China", "Benelux", "Iberia", "HQ", "DACH", "config"]
pLoadTimestamp = datetime.now(timezone.utc)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define global Variables
DATABASE_NAME = notebookutils.variableLibrary.getLibrary("variables_global").lh_name
SOURCE_SCHEMA = "historized_landing"
TARGET_SCHEMA = "base"

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
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

%run utils_error_handling

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

%run nb_utils_great-expectations

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Configs
# - **SAP**: Auftrag, Produkt, Rechnung, Vertrag
# - **Sugar**: Accounts, Meetings, Calls, Contacts, Leads, Opportunities, Emails, Cases
# - **Jira**: Issues, Comments

# CELL ********************

%run nb_config_sap_base

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_config_sugar_base

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_config_jira_base

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_config_sharepoint_base

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Process Tables

# CELL ********************

# Parse p_business_units_to_run (may arrive as JSON string from pipeline)
if isinstance(p_business_units_to_run, str):
    try:
        p_business_units_to_run = json.loads(p_business_units_to_run.replace("'", '"'))
    except json.JSONDecodeError as e:
        print(f"Failed to parse p_business_units_to_run: {e}")

if not isinstance(p_business_units_to_run, list):
    print(f"Warning: p_business_units_to_run is not a list, got {type(p_business_units_to_run).__name__}. Resetting to default.")
    p_business_units_to_run = ["Taiwan"]

# Filter SharePoint configs by Business Units
filtered_sharepoint_configs = [
    cfg for cfg in SHAREPOINT_CONFIG
    if cfg.get("business_unit", "") in p_business_units_to_run
]

# Map all available configs
all_configs = {
    "sap": SAP_CONFIG,
    "sugar": SUGAR_CONFIG,
    "jira": JIRA_CONFIG,
    "sharepoint": filtered_sharepoint_configs
}

# Filter configs to run
print(f"p_sources_to_run type: {type(p_sources_to_run).__name__}, value: {p_sources_to_run}")
if isinstance(p_sources_to_run, str):
    try:
        p_sources_to_run = json.loads(p_sources_to_run.replace("'", '"'))
    except json.JSONDecodeError as e:
        print(f"Failed to parse p_sources_to_run: {e}")

if not isinstance(p_sources_to_run, list):
    print(f"Warning: p_sources_to_run is not a list, got {type(p_sources_to_run).__name__}. Resetting to default.")
    p_sources_to_run = ["sap", "sugar", "jira"]

config_list = get_configs_to_run(p_sources_to_run, all_configs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Process all base tables
results = process_all_base_tables(
    spark=spark,
    config_list=config_list,
    use_cdc=False,
    load_timestamp=pLoadTimestamp
)

# Print results
print(f"Successfully processed {len(results.get('successful', []))} tables")
print(f"Failed to process {len(results.get('failed_tables', []))} tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exit-Payload: Pipeline bleibt grün (kein RuntimeError), Fehler werden als strukturiertes JSON zurückgegeben
payload = {
    "status": "success" if not results.get("failed_tables") else "completed",
    "processed_count": len(results.get("successful", [])),
    "failed_tables": results.get("failed_tables", [])
}
payload_filtered = filter_failed_tables(payload)
mssparkutils.notebook.exit(json.dumps(payload_filtered))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
