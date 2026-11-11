# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ### Sugar Config
# Called by notebook `build_histlanding`

# CELL ********************

# Define Config for Sugar
DATABASE_NAME = notebookutils.variableLibrary.getLibrary("variables_global").lh_name
SOURCE_SYSTEM = "sugar"
TABLE_LOAD_TYPE = "full"
PRIMARY_KEYS = ["id"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Nested dictionary config for multiple sources and tables
TABLE_CONFIGS_SUGAR = [
    {
        "database_name": DATABASE_NAME,
        "source_system": SOURCE_SYSTEM,
        "tablename": "accounts",
        "table_load_type": TABLE_LOAD_TYPE,
        "primary_keys": PRIMARY_KEYS,
        "payload_columns": [
            "abc_classification_c", "billing_address_city", "billing_address_state", "billing_address_postalcode", "billing_address_country",
            "billing_address_street", "billing_address_street2_c", "name1_c","name", "application_area_reporting_c", "customer_status_c",
            "region", "subregion", "icc_main_distributor_account_id", "sap_id_c", "distributorflag_c",
            "industry", "industry_multi_c", "main_type_c", "sub_type_c", "website",
            "cust_role_c",
            "icc_main_distributor_account_name",
            "assigned_user_name",
            "status_c",
            "date_entered",
            "date_modified",
            "customer_since_c"
        ],
        "use_wildcard": False,
        "multi_select_columns": ["sub_type_c", "industry_multi_c", "application_area_reporting_c"],
        "json_payload_column": "records",
        "write_mode": "upsert",
    },
    
    {
        "database_name": DATABASE_NAME,
        "source_system": SOURCE_SYSTEM,
        "tablename": "meetings",
        "table_load_type": TABLE_LOAD_TYPE,
        "primary_keys": PRIMARY_KEYS,
        "payload_columns": [
            "name", "description", "date_start", "date_end", "location", "status", "tag",
            "date_entered", "locked_fields", "_module", "date_modified"
        ],
        "use_wildcard": False,
        "json_payload_column": "records",
        "json_extraction_paths": {"tag": "$.tag[0].name"},
        "write_mode": "upsert",
    },

    {
        "database_name": DATABASE_NAME,
        "source_system": SOURCE_SYSTEM,
        "tablename": "calls",
        "table_load_type": TABLE_LOAD_TYPE,
        "primary_keys": PRIMARY_KEYS,
        "payload_columns": [
            "name", "description", "date_start", "date_end", "status", "tag", "date_entered", "date_modified",
            "locked_fields", "_module"
        ],
        "use_wildcard": False,
        "json_payload_column": "records",
        "json_extraction_paths": {"tag": "$.tag[0].name"},
        "write_mode": "upsert",
    },

    {
        "database_name": DATABASE_NAME,
        "source_system": SOURCE_SYSTEM,
        "tablename": "contacts",
        "table_load_type": TABLE_LOAD_TYPE,
        "primary_keys": PRIMARY_KEYS,
        "payload_columns": [
            "first_name", "last_name", "email1", "phone_work",
            "phone_mobile", "title", "gender_c", "account_id", "date_entered", 'date_modified',
            '_module'
        ],
        "use_wildcard": False,
        "json_payload_column": "records",
        "write_mode": "upsert",
    },

    {
        "database_name": DATABASE_NAME,
        "source_system": SOURCE_SYSTEM,
        "tablename": "leads",
        "table_load_type": TABLE_LOAD_TYPE,
        "primary_keys": PRIMARY_KEYS,
        "payload_columns": [
            "first_name", "last_name", "email1", "phone_work",
            "phone_mobile", "title", "account_id", "status", "gespraechsinhalt_c",
            "gender_c", "subregion", "region", "qualifizierung_c", "lead_source",
            "lead_source_description", "funktion_c", "department", "status_description",
            "next_offset", "date_entered", "date_modified"
        ],
        "use_wildcard": False,
        "json_payload_column": "records",
        "write_mode": "upsert",
    },

    {
        "database_name": DATABASE_NAME,
        "source_system": SOURCE_SYSTEM,
        "tablename": "opportunities",
        "table_load_type": TABLE_LOAD_TYPE,
        "primary_keys": PRIMARY_KEYS,
        "payload_columns": [
            "name", "sales_stage", "date_closed", "opportunity_type", "account_id",
            "amount", "base_rate", "currency_id", "date_won",
            "description", "next_step", "probability", "renewal", "sales_status",
            "date_entered", "date_modified",
        ],
        "use_wildcard": False,
        "json_payload_column": "records",
        "write_mode": "upsert",
    },

    {
        "database_name": DATABASE_NAME,
        "source_system": SOURCE_SYSTEM,
        "tablename": "cases",
        "table_load_type": TABLE_LOAD_TYPE,
        "primary_keys": PRIMARY_KEYS,
        "payload_columns": [
            "name", "status", "priority", "type", "account_id", "date_entered",
            "description", "resolution", "jira_ticket_url_c", "jiraticketnumber_c",
            "jira_account_ids_c", "icc_jira_call_ids",
            "jira_lead_ids_c", "icc_jira_opportunity_ids",
            # "icc_jira_meetings_id",
            "date_modified",
        ],
        "use_wildcard": False,
        "json_payload_column": "records",
        "write_mode": "upsert",
    },

    {
        "database_name": DATABASE_NAME,
        "source_system": SOURCE_SYSTEM,
        "tablename": "emails",
        "table_load_type": TABLE_LOAD_TYPE,
        "primary_keys": PRIMARY_KEYS,
        "payload_columns": [
            "name", "description", "from_addr_name", "to_addrs_names", "cc_addrs_names", "bcc_addrs_names", 
            "date_sent", "status", "type",
            "date_entered", "date_modified",
        ],
        "use_wildcard": False,
        "json_payload_column": "records",
        "write_mode": "upsert",
    },
    {
        "database_name": DATABASE_NAME,
        "source_system": SOURCE_SYSTEM,
        "tablename": "producttemplates",
        "table_load_type": TABLE_LOAD_TYPE,
        "primary_keys": PRIMARY_KEYS,
        "payload_columns": [
            "vendor_part_num", "name", "calc_prod_group_c", "type_name",
            "date_entered", "date_modified",
        ],
        "use_wildcard": False,
        "json_payload_column": "records",
        "write_mode": "upsert",
    },
    {
        "database_name": DATABASE_NAME,
        "source_system": SOURCE_SYSTEM,
        "tablename": "key_words",
        "table_load_type": TABLE_LOAD_TYPE,
        "primary_keys": PRIMARY_KEYS,
        "payload_columns": [
            "name", "description", "assigned_user_id", "assigned_user_name",
            "date_entered", "date_modified"
        ],
        "use_wildcard": False,
        "json_payload_column": "records",
        "write_mode": "upsert",
    },
]

# List of link tables to process
LINK_TABLES_SUGAR = [
    "link_accounts_assigned_user",
    "link_meetings_invitees",
    "link_meetings_accounts_meetings_1",
    "link_calls_invitees",
    "link_calls_accounts_calls_1",
    "link_leads_assigned_user",
    "link_opportunities_assigned_user",
    "link_cases_cases_accounts_1",
    "link_emails_assigned_user",
    "link_key_words_key_words_accounts",
    "link_key_words_key_words_calls",
    "link_key_words_key_words_contacts_1",
    "link_key_words_key_words_leads",
    "link_key_words_key_words_meetings",
    "link_key_words_key_words_opportunities",
]

# Dynamically add link configurations
for link in LINK_TABLES_SUGAR:
    payload_cols = ["date_modified", "name", "parent_id"]
    if link in ["link_calls_invitees", "link_meetings_invitees"]:
        payload_cols.append("_link")

    TABLE_CONFIGS_SUGAR.append({
        "database_name": DATABASE_NAME,
        "source_system": SOURCE_SYSTEM,
        "tablename": link,
        "table_load_type": TABLE_LOAD_TYPE,
        "primary_keys": PRIMARY_KEYS,
        "payload_columns": payload_cols,
        "use_wildcard": False,
        "write_mode": "upsert",
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
