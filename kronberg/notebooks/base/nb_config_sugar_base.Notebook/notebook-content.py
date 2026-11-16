# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ### Sugar Config (base)
# Called by notebook `nb_build_base`.
#
# **Config rule — metadata columns**
#
# - `_source` (source system, e.g. `"sugar"`) and `_load_timestamp` are metadata and must be
#   listed in every `column_mapping` so they reach the target table.
# - They are never part of `hashdiff_cols` / `payload_columns` and are stripped automatically by
#   `nb_utils_general_hashing` if they are.
# - Only names in `_EXCLUDED_HASH_COLUMNS` count as metadata. Regular fields such as `_module`
#   or `_link` stay payload and belong in `hashdiff_cols`.

# CELL ********************

# Sugar Config
PRIMARY_KEYS_SUGAR = ["id"]

SUGAR_CONFIG = [
    {
        "source": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": SOURCE_SCHEMA,
            "table_name": "sugar_accounts",
        },
        "target": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": TARGET_SCHEMA,
            "table_name": "sugar_account",
        },
        "source_primary_keys": PRIMARY_KEYS_SUGAR,
        "target_primary_keys": PRIMARY_KEYS_SUGAR,
        "column_mapping": {
            "id": "id",
            "_source": "_source",
            "_load_timestamp": "_load_timestamp",
            "abc_classification_c": "abc_classification_c",
            "billing_address_city": "billing_address_city",
            "billing_address_state": "billing_address_state",
            "billing_address_postalcode": "billing_address_postalcode",
            "billing_address_country": "billing_address_country",
            "billing_address_street": "billing_address_street",
            "billing_address_street2_c": "billing_address_street2_c",
            "name": "name_full",
            "name1_c": "name_short",
            "application_area_reporting_c": "application_area_reporting_c",
            "customer_status_c": "customer_status_c",
            "region": "region",
            "subregion": "subregion",
            "icc_main_distributor_account_id": "icc_main_distributor_account_id",
            "sap_id_c": "sap_id_c",
            "distributorflag_c": "is_sales_partner",
            "industry": "industry",
            "industry_multi_c": "industry_multi_c",
            "main_type_c": "main_type_c",
            "sub_type_c": "sub_type_c",
            "website": "website",
            "cust_role_c": "cust_role",
            "icc_main_distributor_account_name": "icc_main_distributor_account_name",
            "assigned_user_name": "assigned_user_name",
            "status_c": "status",
            "date_entered": "date_entered",
            "date_modified": "date_modified",
            "customer_since_c": "customer_since_c",
        },
        "hashdiff_cols": [
            "abc_classification_c", "billing_address_city", "billing_address_state", "billing_address_postalcode",
            "billing_address_country", "billing_address_street", "billing_address_street2_c", "name_full","name_short", "application_area_reporting_c",
            "customer_status_c", "region", "subregion", "icc_main_distributor_account_id", "sap_id_c",
            "is_sales_partner", "industry", "industry_multi_c", "main_type_c", "sub_type_c",
            "website",
            "cust_role",
            "icc_main_distributor_account_name",
            "assigned_user_name",
            "status",
            "industry_mapped", "industry_multi_c_mapped", "subregion_mapped",
            "abc_classification_c_mapped", "customer_status_c_mapped", "application_area_reporting_c_mapped", "sub_type_c_mapped",
            "country_mapped", "street",
            "cust_role_mapped",
            "status_mapped",
            "date_entered", "date_modified","customer_since_c"
        ],
        "transformations": [
            {
                "type": "cast_data_types",
                "columns_config": {
                    "id": {"data_type": "string"},
                    "abc_classification_c": {"data_type": "string"},
                    "billing_address_city": {"data_type": "string"},
                    "billing_address_state": {"data_type": "string"},
                    "billing_address_postalcode": {"data_type": "string"},
                    "billing_address_country": {"data_type": "string"},
                    "billing_address_street": {"data_type": "string"},
                    "billing_address_street2_c": {"data_type": "string"},
                    "name_short": {"data_type": "string"},
                    "name_full": {"data_type": "string"},
                    "application_area_reporting_c": {"data_type": "string"},
                    "customer_status_c": {"data_type": "string"},
                    "region": {"data_type": "string"},
                    "subregion": {"data_type": "string"},
                    "icc_main_distributor_account_id": {"data_type": "string"},
                    "sap_id_c": {"data_type": "string"},
                    "is_sales_partner": {"data_type": "boolean"},
                    "industry": {"data_type": "string"},
                    "industry_multi_c": {"data_type": "string"},
                    "main_type_c": {"data_type": "string"},
                    "sub_type_c": {"data_type": "string"},
                    "website": {"data_type": "string"},
                    "cust_role": {"data_type": "string"},
                    "icc_main_distributor_account_name": {"data_type": "string"},
                    "assigned_user_name": {"data_type": "string"},
                    "status": {"data_type": "string"},
                    "date_entered": {"data_type": "timestamp"},
                    "date_modified": {"data_type": "timestamp"},
                    "customer_since_c": {"data_type": "integer"},
                }
            },
            {
                "type": "trim",
                "columns_config": {
                    "id": {},
                    "abc_classification_c": {},
                    "billing_address_city": {},
                    "billing_address_state": {},
                    "billing_address_postalcode": {},
                    "billing_address_country": {},
                    "billing_address_street": {},
                    "billing_address_street2_c": {},
                    "name_short": {},
                    "name_full": {},
                    "application_area_reporting_c": {},
                    "customer_status_c": {},
                    "region": {},
                    "subregion": {},
                    "icc_main_distributor_account_id": {},
                    "industry": {},
                    "industry_multi_c": {},
                    "main_type_c": {},
                    "sub_type_c": {},
                    "website": {},
                    "cust_role": {},
                    "icc_main_distributor_account_name": {},
                    "assigned_user_name": {},
                    "status": {},
                    "date_entered": {},
                    "date_modified": {},
                }
            },
            {
                "type": "replace_values",
                "columns_config": {
                    "abc_classification_c": {
                        "mapping": {
                            "A": "A-Kunde",
                            "2": "A+-Kunde",
                            "P": "potentieller A-Kunde",
                            "1": "B-Kunde",
                            "4": "C-Kunde",
                            "5": "D-Kunde"
                        }
                    },
                    "subregion": {
                        "mapping": {
                            "SE_Europe": "SE Europe",
                            "CN_SG_MY": "CN/SG/MY",
                            "APAC_Plus": "APAC Plus",
                            "USCA": "US/CA",
                            "NW_Europe": "NW Europe",
                            "DACH": "DACH",
                            "Japan": "Japan"
                        }
                    },
                    "region": {
                        "mapping": {
                            "USCA": "US/CA"
                        }
                    },
                    "customer_status_c": {
                        "mapping": {
                            "Active": "Aktiv", 
                            "Inactive": "Inaktiv", 
                            "Dead": "Tot"
                        }
                    },
                    "cust_role": {
                        "mapping": {
                            "101": "Standard Integrator",
                            "102": "Advanced Integrator",
                            "103": "In-house Integrator",
                            "104": "OEM"
                        }
                    },
                    "status": {
                        "mapping": {
                            "2": "Aktiv",
                            "3": "Gesperrt",
                            "4": "Veraltet"
                        }
                    }
                }
            },

            {
                "type": "map_array_values",
                "columns_config": {
                    "industry": {
                        "mapping": {
                            "101": "Allgemein", "102": "Automobilbau und Zulieferindustrie", "103": "Bergbau",
                            "104": "Chemie", "105": "Druck", "106": "Eisen, Stahl und Metallverarbeitung",
                            "107": "Eisenbahn", "108": "Elektronik, Elektroindustrie", "109": "Forst- und Holzwirtschaft",
                            "110": "Gesundheitswesen und Biotechnologie, Umwelt", "111": "Glasproduktion und -verarbeitung",
                            "112": "Gummi, Kunststoff, Folien", "113": "Halbleiter", "114": "Keramik",
                            "115": "Landwirtschaft, Nahrungsmittel und Getränke", "116": "Luft- und Raumfahrt",
                            "117": "Maschinenbau", "118": "Medizinbedarf", "119": "Optik und Feinmechanik",
                            "120": "Papierverarbeitung", "121": "Pharmazie", "122": "Photogrammetrie und Fernerkundung",
                            "123": "Schiffsbau", "124": "Solar, Erneuerbare Energie, Recycling", "125": "Telekommunikation",
                            "126": "Transport, Logistik, Handel", "127": "Überwachung und Sicherheit",
                            "128": "Verpackung", "129": "Verwaltung und Militär", "130": "Lehre und Forschung",
                            "131": "Andere Industrien", "132": "Batterieproduktion"
                        },
                        "delimiter": ",",
                        "finalColumn_string": True
                    },
                    "industry_multi_c": {
                        "mapping": {
                            "101": "Allgemein", "102": "Automobilbau und Zulieferindustrie", "103": "Bergbau",
                            "104": "Chemie", "105": "Druck", "106": "Eisen, Stahl und Metallverarbeitung",
                            "107": "Eisenbahn", "108": "Elektronik, Elektroindustrie", "109": "Forst- und Holzwirtschaft",
                            "110": "Gesundheitswesen und Biotechnologie, Umwelt", "111": "Glasproduktion und -verarbeitung",
                            "112": "Gummi, Kunststoff, Folien", "113": "Halbleiter", "114": "Keramik",
                            "115": "Landwirtschaft, Nahrungsmittel und Getränke", "116": "Luft- und Raumfahrt",
                            "117": "Maschinenbau", "118": "Medizinbedarf", "119": "Optik und Feinmechanik",
                            "120": "Papierverarbeitung", "121": "Pharmazie", "122": "Photogrammetrie und Fernerkundung",
                            "123": "Schiffsbau", "124": "Solar, Erneuerbare Energie, Recycling", "125": "Telekommunikation",
                            "126": "Transport, Logistik, Handel", "127": "Überwachung und Sicherheit",
                            "128": "Verpackung", "129": "Verwaltung und Militär", "130": "Lehre und Forschung",
                            "131": "Andere Industrien", "132": "Batterieproduktion"
                        },
                        "delimiter": ",",
                        "finalColumn_string": True
                    },
                    "sub_type_c": {
                        "mapping": {
                            "College": "Campusteilnehmer",
                            "CIP": "CIP",
                            "Consultant": "Consultant",       
                            "PurchaseProvider": "Einkaufsdienstleister",                            
                            "NonProfit": "Non-Profit",                            
                            "IAP": "TP",
                            "USReseller": "US-Reseller",                            
                            "Competitor": "Wettbewerber"
                        },
                        "delimiter": ",",
                        "finalColumn_string": True
                    },
                    "application_area_reporting_c": {
                        "mapping": {
                            "101": "2D-Lageerkennung",
                            "102": "2D-Objekterkennung",
                            "103": "3D-Lageerkennung",
                            "104": "3D-Objekterkennung",
                            "105": "Alignment",
                            "106": "Allgemein",
                            "107": "Druckbildkontrolle",
                            "108": "Farbprüfung",
                            "109": "Identifikation mittels Barcodes",
                            "110": "Identifikation mittels Datacodes",
                            "111": "Identifikation mittels OCR",
                            "112": "Oberflächeninspektion",
                            "113": "Robot Vision",
                            "114": "Sicherheitssysteme",
                            "115": "Texturinspektion",
                            "116": "Verkehrsüberwachung und Fahrerassistenz",
                            "117": "Vermessen und Vergleichen 2D",
                            "118": "Vermessen und Vergleichen 3D",
                            "119": "Vollständigkeitskontrolle"
                        },
                        "delimiter": ",",
                        "finalColumn_string": True
                    }
                }
            },
            {
                "type": "apply_lookup",
                "columns_config": {
                    "billing_address_country": {
                        "path": "Files/iso_3166_laendercode.csv",
                        "join_col_source": "billing_address_country",
                        "join_col_lookup": "iso_code",
                        "target_cols": {"country": "country_mapped"}
                    }
                }
            },
            {
                "type": "concat_columns",
                "columns_config": {
                    "street": {
                        "input_cols": ["billing_address_street", "billing_address_street2_c"],
                        "separator": " ",
                        "output_col": "street"
                    }
                }
            },
            {
                "type": "remove_invalid_records",
                "columns_config": {
                    "sap_id_c": {}
                }
            }
        ]
    },
    {
        "source": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": SOURCE_SCHEMA,
            "table_name": "sugar_meetings",
        },
        "target": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": TARGET_SCHEMA,
            "table_name": "sugar_meeting",
        },
        "source_primary_keys": PRIMARY_KEYS_SUGAR,
        "target_primary_keys": PRIMARY_KEYS_SUGAR,
        "column_mapping": {
            "id": "id",
            "_source": "_source",
            "_load_timestamp": "_load_timestamp",
            "date_start": "date_start",
            "date_end": "date_end",
            "name": "name",
            "description": "description",
            "location": "location",
            "status": "status",
            "tag": "tag",
            "date_entered": "date_entered",
            "date_modified": "date_modified"
        },
        "hashdiff_cols": [
            "date_start", "date_end", "name", "description", "location", "status", "tag",
            "date_entered", "date_modified"
        ],
        "transformations": [
            {
                "type": "cast_data_types",
                "columns_config": {
                    "id": {"data_type": "string"},
                    "date_start": {"data_type": "timestamp"},
                    "date_end": {"data_type": "timestamp"},
                    "name": {"data_type": "string"},
                    "description": {"data_type": "string"},
                    "location": {"data_type": "string"},
                    "status": {"data_type": "string"},
                    "tag": {"data_type": "string"},
                    "date_entered": {"data_type": "timestamp"},
                    "date_modified": {"data_type": "timestamp"},
                }
            },
            {
                "type": "trim",
                "columns_config": {
                    "id": {},
                    "name": {},
                    "description": {},
                    "location": {},
                    "status": {},
                    "tag": {},
                    "date_entered": {},
                    "date_modified": {},
                }
            }
        ]
    },
    {
        "source": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": SOURCE_SCHEMA,
            "table_name": "sugar_calls",
        },
        "target": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": TARGET_SCHEMA,
            "table_name": "sugar_call",
        },
        "source_primary_keys": PRIMARY_KEYS_SUGAR,
        "target_primary_keys": PRIMARY_KEYS_SUGAR,
        "column_mapping": {
            "id": "id",
            "_source": "_source",
            "_load_timestamp": "_load_timestamp",
            "date_start": "date_start",
            "date_end": "date_end",
            "name": "name",
            "description": "description",
            "status": "status",
            "tag": "tag",
            "date_entered": "date_entered",
            "date_modified": "date_modified",
        },
        "hashdiff_cols": [
            "date_start", "date_end", "name", "description", "status", "tag",
            "date_entered", "date_modified"
        ],
        "transformations": [
            {
                "type": "cast_data_types",
                "columns_config": {
                    "id": {"data_type": "string"},
                    "date_start": {"data_type": "timestamp"},
                    "date_end": {"data_type": "timestamp"},
                    "name": {"data_type": "string"},
                    "description": {"data_type": "string"},
                    "status": {"data_type": "string"},
                    "tag": {"data_type": "string"},
                    "date_entered": {"data_type": "timestamp"},
                    "date_modified": {"data_type": "timestamp"},
                }
            },
            {
                "type": "trim",
                "columns_config": {
                    "id": {},
                    "name": {},
                    "description": {},
                    "status": {},
                    "tag": {},
                    "date_entered": {},
                    "date_modified": {},
                }
            }
        ]
    },
    {
        "source": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": SOURCE_SCHEMA,
            "table_name": "sugar_contacts",
        },
        "target": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": TARGET_SCHEMA,
            "table_name": "sugar_contact",
        },
        "source_primary_keys": PRIMARY_KEYS_SUGAR,
        "target_primary_keys": PRIMARY_KEYS_SUGAR,
        "column_mapping": {
            "id": "id",
            "_source": "_source",
            "_load_timestamp": "_load_timestamp",
            "first_name": "first_name",
            "last_name": "last_name",
            "email1": "email1",
            "phone_work": "phone_work",
            "phone_mobile": "phone_mobile",
            "title": "title",
            "account_id": "account_id",
            "gender_c": "gender_c",
            "date_entered": "date_entered",
            "date_modified": "date_modified",
        },
        "hashdiff_cols": [
            "first_name", "last_name", "email1",
            "phone_work", "phone_mobile", "title", "account_id", "gender_c",
            "gender_c_mapped", "full_name",
            "date_entered", "date_modified"
        ],
        "transformations": [
            {
                "type": "cast_data_types",
                "columns_config": {
                    "id": {"data_type": "string"},
                    "first_name": {"data_type": "string"},
                    "last_name": {"data_type": "string"},
                    "email1": {"data_type": "string"},
                    "phone_work": {"data_type": "string"},
                    "phone_mobile": {"data_type": "string"},
                    "title": {"data_type": "string"},
                    "account_id": {"data_type": "string"},
                    "gender_c": {"data_type": "string"},
                    "date_entered": {"data_type": "timestamp"},
                    "date_modified": {"data_type": "timestamp"},
                }
            },
            {
                "type": "trim",
                "columns_config": {
                    "id": {},
                    "first_name": {},
                    "last_name": {},
                    "email1": {},
                    "phone_work": {},
                    "phone_mobile": {},
                    "title": {},
                    "account_id": {},
                    "gender_c": {},
                    "date_entered": {},
                    "date_modified": {},
                }
            },
            {
                "type": "concat_columns",
                "columns_config": {
                    "full_name": {
                        "input_cols": ["first_name", "last_name"],
                        "separator": " ",
                        "output_col": "full_name"
                    }
                }
            },
            {
                "type": "replace_values",
                "columns_config": {
                    "gender_c": {
                        "mapping": {
                            "0": "Unbekannt",
                            "1": "Herr",
                            "2": "Frau"
                        }
                    }
                }
            }
        ]
    },
    {
        "source": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": SOURCE_SCHEMA,
            "table_name": "sugar_leads",
        },
        "target": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": TARGET_SCHEMA,
            "table_name": "sugar_lead",
        },
        "source_primary_keys": PRIMARY_KEYS_SUGAR,
        "target_primary_keys": PRIMARY_KEYS_SUGAR,
        "column_mapping": {
            "id": "id",
            "_source": "_source",
            "_load_timestamp": "_load_timestamp",
            "first_name": "first_name",
            "last_name": "last_name",
            "email1": "email1",
            "phone_work": "phone_work",
            "phone_mobile": "phone_mobile",
            "title": "title",
            "account_id": "account_id",
            "status": "status",
            "gespraechsinhalt_c": "gespraechsinhalt_c",
            "gender_c": "gender_c",
            "subregion": "subregion",
            "region": "region",
            "qualifizierung_c": "qualifizierung_c",
            "lead_source": "lead_source",
            "lead_source_description": "lead_source_description",
            "funktion_c": "funktion_c",
            "department": "department",
            "status_description": "status_description",
            "date_entered": "date_entered",
            "date_modified": "date_modified",
        },
        "hashdiff_cols": [
            "first_name", "last_name", "email1",
            "phone_work", "phone_mobile", "title", "account_id", "status",
            "gespraechsinhalt_c", "gender_c", "subregion", "region", "qualifizierung_c",
            "lead_source", "lead_source_description", "funktion_c", "department", "status_description",
            "subregion_mapped", "full_name",
            "date_entered", "date_modified"
        ],
        "transformations": [
            {
                "type": "cast_data_types",
                "columns_config": {
                    "id": {"data_type": "string"},
                    "first_name": {"data_type": "string"},
                    "last_name": {"data_type": "string"},
                    "email1": {"data_type": "string"},
                    "phone_work": {"data_type": "string"},
                    "phone_mobile": {"data_type": "string"},
                    "title": {"data_type": "string"},
                    "account_id": {"data_type": "string"},
                    "status": {"data_type": "string"},
                    "gespraechsinhalt_c": {"data_type": "string"},
                    "gender_c": {"data_type": "string"},
                    "subregion": {"data_type": "string"},
                    "region": {"data_type": "string"},
                    "qualifizierung_c": {"data_type": "string"},
                    "lead_source": {"data_type": "string"},
                    "lead_source_description": {"data_type": "string"},
                    "funktion_c": {"data_type": "string"},
                    "department": {"data_type": "string"},
                    "status_description": {"data_type": "string"},
                    "date_entered": {"data_type": "timestamp"},
                    "date_modified": {"data_type": "timestamp"},
                }
            },
            {
                "type": "trim",
                "columns_config": {
                    "id": {},
                    "first_name": {},
                    "last_name": {},
                    "email1": {},
                    "phone_work": {},
                    "phone_mobile": {},
                    "title": {},
                    "account_id": {},
                    "status": {},
                    "gespraechsinhalt_c": {},
                    "gender_c": {},
                    "subregion": {},
                    "region": {},
                    "qualifizierung_c": {},
                    "lead_source": {},
                    "lead_source_description": {},
                    "funktion_c": {},
                    "department": {},
                    "status_description": {},
                    "date_entered": {},
                    "date_modified": {},
                }
            },
            {
                "type": "concat_columns",
                "columns_config": {
                    "full_name": {
                        "input_cols": ["first_name", "last_name"],
                        "separator": " ",
                        "output_col": "full_name"
                    }
                }
            },
            {
                "type": "replace_values",
                "columns_config": {
                    "subregion": {
                        "mapping": {
                            "SE_Europe": "SE Europe",
                            "CN_SG_MY": "CN/SG/MY",
                            "APAC_Plus": "APAC Plus",
                            "USCA": "US/CA",
                            "NW_Europe": "NW Europe",
                            "DACH": "DACH",
                            "Japan": "Japan"
                        }
                    }
                }
            }
        ]
    },
    {
        "source": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": SOURCE_SCHEMA,
            "table_name": "sugar_opportunities",
        },
        "target": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": TARGET_SCHEMA,
            "table_name": "sugar_opportunity",
        },
        "source_primary_keys": PRIMARY_KEYS_SUGAR,
        "target_primary_keys": PRIMARY_KEYS_SUGAR,
        "column_mapping": {
            "id": "id",
            "_source": "_source",
            "_load_timestamp": "_load_timestamp",
            "sales_stage": "sales_stage",
            "name": "name",
            "opportunity_type": "opportunity_type",
            "account_id": "account_id",
            "amount": "amount",
            "base_rate": "base_rate",
            "currency_id": "currency_id",
            "date_won": "date_closed",
            "date_closed": "date_closed_planned",
            "description": "description",
            "next_step": "next_step",
            "probability": "probability",
            "renewal": "renewal",
            "sales_status": "sales_status",
            "date_entered": "date_entered",
            "date_modified": "date_modified",
        },
        "hashdiff_cols": [
            "sales_stage", "name", "date_closed", "date_closed_planned", "opportunity_type", "account_id",
            "amount", "base_rate", "currency_id",
            "description", "next_step", "probability", "renewal",
            "sales_status",
            "date_entered", "date_modified"
        ],
        "transformations": [
            {
                "type": "cast_data_types",
                "columns_config": {
                    "id": {"data_type": "string"},
                    "name": {"data_type": "string"},
                    "sales_stage": {"data_type": "string"},
                    "date_closed": {"data_type": "date"},
                    "date_closed_planned": {"data_type": "date"},
                    "opportunity_type": {"data_type": "string"},
                    "account_id": {"data_type": "string"},
                    "amount": {"data_type": "double"},
                    "base_rate": {"data_type": "double"},
                    "currency_id": {"data_type": "string"},
                    "description": {"data_type": "string"},
                    "next_step": {"data_type": "string"},
                    "probability": {"data_type": "double"},
                    "renewal": {"data_type": "string"},
                    "sales_status": {"data_type": "string"},
                    "date_entered": {"data_type": "timestamp"},
                    "date_modified": {"data_type": "timestamp"},
                }
            },
            {
                "type": "trim",
                "columns_config": {
                    "id": {},
                    "name": {},
                    "sales_stage": {},
                    "opportunity_type": {},
                    "account_id": {},
                    "currency_id": {},
                    "description": {},
                    "next_step": {},
                    "renewal": {},
                    "sales_status": {},
                    "date_entered": {},
                    "date_modified": {},
                }
            }
        ]
    },
    {
        "source": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": SOURCE_SCHEMA,
            "table_name": "sugar_cases",
        },
        "target": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": TARGET_SCHEMA,
            "table_name": "sugar_case",
        },
        "source_primary_keys": PRIMARY_KEYS_SUGAR,
        "target_primary_keys": PRIMARY_KEYS_SUGAR,
        "column_mapping": {
            "id": "id",
            "_source": "_source",
            "_load_timestamp": "_load_timestamp",
            "name": "name",
            "status": "status",
            "priority": "priority",
            "type": "type",
            "account_id": "account_id",
            "date_entered": "date_entered",
            "description": "description",
            "resolution": "resolution",
            "jira_ticket_url_c": "jira_ticket_url_c",
            "jiraticketnumber_c": "jiraticketnumber_c",
            "jira_account_ids_c": "jira_account_ids_c",
            "icc_jira_call_ids": "icc_jira_call_ids",
            "jira_lead_ids_c": "jira_lead_ids_c",
            "icc_jira_opportunity_ids": "icc_jira_opportunity_ids",
            # "icc_jira_meetings_id": "icc_jira_meetings_id",
            "date_modified": "date_modified",
        },
        "hashdiff_cols": [
            "name", "status", "priority", "type", "account_id",
            "date_entered", "description", "resolution", "jira_ticket_url_c", "jiraticketnumber_c",
            "jira_account_ids_c", "icc_jira_call_ids",
            "jira_lead_ids_c", "icc_jira_opportunity_ids",
            # "icc_jira_meetings_id",
            "date_modified",
        ],
        "transformations": [
            {
                "type": "cast_data_types",
                "columns_config": {
                    "id": {"data_type": "string"},
                    "name": {"data_type": "string"},
                    "status": {"data_type": "string"},
                    "priority": {"data_type": "string"},
                    "type": {"data_type": "string"},
                    "account_id": {"data_type": "string"},
                    "date_entered": {"data_type": "timestamp"},
                    "description": {"data_type": "string"},
                    "resolution": {"data_type": "string"},
                    "jira_ticket_url_c": {"data_type": "string"},
                    "jiraticketnumber_c": {"data_type": "string"},
                    "jira_account_ids_c": {"data_type": "string"},
                    "icc_jira_call_ids": {"data_type": "string"},
                    "jira_lead_ids_c": {"data_type": "string"},
                    "icc_jira_opportunity_ids": {"data_type": "string"},
                    # "icc_jira_meetings_id": {"data_type": "string"},
                    "date_modified": {"data_type": "timestamp"},
                }
            },
            {
                "type": "trim",
                "columns_config": {
                    "id": {},
                    "name": {},
                    "status": {},
                    "priority": {},
                    "type": {},
                    "account_id": {},
                    "date_entered": {},
                    "description": {},
                    "resolution": {},
                    "jira_ticket_url_c": {},
                    "jiraticketnumber_c": {},
                    "jira_account_ids_c": {},
                    "icc_jira_call_ids": {},
                    "jira_lead_ids_c": {},
                    "icc_jira_opportunity_ids": {},
                    # "icc_jira_meetings_id": {},
                    "date_modified": {},
                }
            }
        ]
    },
    {
        "source": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": SOURCE_SCHEMA,
            "table_name": "sugar_emails",
        },
        "target": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": TARGET_SCHEMA,
            "table_name": "sugar_email",
        },
        "source_primary_keys": PRIMARY_KEYS_SUGAR,
        "target_primary_keys": PRIMARY_KEYS_SUGAR,
        "column_mapping": {
            "id": "id",
            "_source": "_source",
            "_load_timestamp": "_load_timestamp",
            "name": "name",
            "description": "description",
            "from_addr_name": "from_addr_name",
            "to_addrs_names": "to_addrs_names",
            "cc_addrs_names": "cc_addrs_names",
            "bcc_addrs_names": "bcc_addrs_names",
            "date_sent": "date_sent",
            "status": "status",
            "type": "type",
            "date_entered": "date_entered",
            "date_modified": "date_modified"
        },
        "hashdiff_cols": [
             "name", "description", "from_addr_name", "to_addrs_names", "cc_addrs_names",
             "bcc_addrs_names","date_sent", "status", "type",
             "status_mapped", "type_mapped",
             "date_entered", "date_modified"
            ],
        "transformations": [
            {
                "type": "cast_data_types",
                "columns_config": {
                    "id":              {"data_type": "string"},
                    "name":            {"data_type": "string"},
                    "description":     {"data_type": "string"},
                    "from_addr_name":  {"data_type": "string"},
                    "to_addrs_names":  {"data_type": "string"},
                    "cc_addrs_names":  {"data_type": "string"},
                    "bcc_addrs_names": {"data_type": "string"},
                    "date_sent":       {"data_type": "date"},
                    "status":          {"data_type": "string"},
                    "type":            {"data_type": "string"},
                    "date_entered": {"data_type": "timestamp"},
                    "date_modified": {"data_type": "timestamp"}
                }
            },
            {
                "type": "trim",
                "columns_config": {
                    "id":              {},
                    "name":            {},
                    "description":     {},
                    "from_addr_name":  {},
                    "to_addrs_names":  {},
                    "cc_addrs_names":  {},
                    "bcc_addrs_names": {},
                    "status":          {},
                    "type":            {},
                    "date_entered":    {},
                    "date_modified":   {}
                }
            },
            {
                "type": "replace_values",
                "columns_config": {
                    "status": {
                        "mapping": {
                            "sent": "Sent",
                            "archived": "Archived"
                        }
                    },
                    "type": {
                        "mapping": {
                            "out": "Outbound",
                            "in": "Inbound"
                        }
                    }
                }
            }
        ]
    },
    {
        "source": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": SOURCE_SCHEMA,
            "table_name": "sugar_producttemplates",
        },
        "target": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": TARGET_SCHEMA,
            "table_name": "sugar_product",
        },
        "source_primary_keys": PRIMARY_KEYS_SUGAR,
        "target_primary_keys": ["sugar_id"],
        "column_mapping": {
            "id": "sugar_id",
            "_source": "_source",
            "_load_timestamp": "_load_timestamp",
            "vendor_part_num": "product_id",
            "name": "name",
            "calc_prod_group_c": "product_group",
            "type_name": "product_category",
            "date_entered": "date_entered",
            "date_modified": "date_modified",
        },
        "hashdiff_cols": [
            "product_id", "name", "product_group", "product_category",
            "date_entered", "date_modified"
        ],
        "transformations": [
            {
                "type": "cast_data_types",
                "columns_config": {
                    "sugar_id": {"data_type": "string"},
                    "product_id": {"data_type": "integer"},
                    "name": {"data_type": "string"},
                    "product_group": {"data_type": "string"},
                    "product_category": {"data_type": "string"},
                    "date_entered": {"data_type": "timestamp"},
                    "date_modified": {"data_type": "timestamp"},
                }
            },
            {
                "type": "trim",
                "columns_config": {
                    "sugar_id": {},
                    "product_id": {},
                    "name": {},
                    "product_group": {},
                    "product_category": {},
                    "date_entered": {},
                    "date_modified": {},
                }
            }
        ]
    },
    {
        "source": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": SOURCE_SCHEMA,
            "table_name": "sugar_key_words",
        },
        "target": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": TARGET_SCHEMA,
            "table_name": "sugar_key_word",
        },
        "source_primary_keys": PRIMARY_KEYS_SUGAR,
        "target_primary_keys": PRIMARY_KEYS_SUGAR,
        "column_mapping": {
            "id": "id",
            "_source": "_source",
            "_load_timestamp": "_load_timestamp",
            "name": "name",
            "description": "description",
            "assigned_user_id": "assigned_user_id",
            "assigned_user_name": "assigned_user_name",
            "date_entered": "date_entered",
            "date_modified": "date_modified",
        },
        "hashdiff_cols": [
            "name", "description", "assigned_user_id", "assigned_user_name",
            "date_entered", "date_modified"
        ],
        "transformations": [
            {
                "type": "cast_data_types",
                "columns_config": {
                    "id": {"data_type": "string"},
                    "name": {"data_type": "string"},
                    "description": {"data_type": "string"},
                    "assigned_user_id": {"data_type": "string"},
                    "assigned_user_name": {"data_type": "string"},
                    "date_entered": {"data_type": "timestamp"},
                    "date_modified": {"data_type": "timestamp"},
                }
            },
            {
                "type": "trim",
                "columns_config": {
                    "id": {},
                    "name": {},
                    "description": {},
                    "assigned_user_id": {},
                    "assigned_user_name": {}
                }
            }
        ]
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

# Dynamically add link configurations for the Base layer
for link in LINK_TABLES_SUGAR:
    col_mapping = {
        "id": "id",
        "_source": "_source",
        "_load_timestamp": "_load_timestamp",
        "date_modified": "date_modified",
        "name": "name",
        "parent_id": "parent_id",
    }
    hashdiff_cols = ["date_modified", "name", "parent_id"]
    cast_config = {
        "id": {"data_type": "string"},
        "date_modified": {"data_type": "timestamp"},
        "name": {"data_type": "string"},
        "parent_id": {"data_type": "string"},
    }
    trim_config = {
        "id": {},
        "name": {},
        "parent_id": {},
    }
    
    if link in ["link_calls_invitees", "link_meetings_invitees"]:
        col_mapping["_link"] = "_link"
        hashdiff_cols.append("_link")
        cast_config["_link"] = {"data_type": "string"}
        trim_config["_link"] = {}

    SUGAR_CONFIG.append({
        "source": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": SOURCE_SCHEMA,
            "table_name": f"sugar_{link}",
        },
        "target": {
            "lakehouse_name": DATABASE_NAME,
            "schema_name": TARGET_SCHEMA,
            "table_name": f"sugar_{link}",
        },
        "source_primary_keys": PRIMARY_KEYS_SUGAR,
        "target_primary_keys": PRIMARY_KEYS_SUGAR,
        "column_mapping": col_mapping,
        "hashdiff_cols": hashdiff_cols,
        "transformations": [
            {
                "type": "cast_data_types",
                "columns_config": cast_config
            },
            {
                "type": "trim",
                "columns_config": trim_config
            }
        ]
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
