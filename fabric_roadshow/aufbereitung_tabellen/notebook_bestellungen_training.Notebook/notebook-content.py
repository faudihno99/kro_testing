# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1df0f3cd-75d3-4520-805a-c4cb67fbe687",
# META       "default_lakehouse_name": "lh_shortcut",
# META       "default_lakehouse_workspace_id": "98c91999-02e5-4d24-8f5f-5b95d409608e"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Datenaufbereitung mit dem Notebook

# MARKDOWN ********************

# #### **1) Default-Lakehouse**
# Stelle dein Lakehouse als Standard ein, um Abfragen mit relativen Pfaden zu vereinfachen. Verwende dazu die Explorerleiste auf der linken Seite.

# MARKDOWN ********************

# #### **2) Packages** - Notwendige Bibliotheken importieren
# 
# In diesem Abschnitt werden alle erforderlichen Bibliotheken und Module importiert, die für die Durchführung der Analysen und Berechnungen im Notebook notwendig sind.

# CELL ********************

# Import packages
from pyspark.sql.functions import col, to_date, regexp_replace, datediff

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **3) Tabelle laden**
# Importiere die Tabelle "adventureworks_bestellungen_roadshow" aus deinem Lakehouse in das Notebook. Passe gegebenenfalls den Variablennamen "tabellenname" an, falls ein anderer Tabellenname verwendet wird.


# CELL ********************

# Read table "bestellung" from the lakehouse
tabellenname = "adventureworks_bestellungen_roadshow"
df_bestellungen = spark.read.format("delta").load(f"Tables/dbo/{tabellenname}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 4. **Anpassung Datentyp**
# Ändere die Datentypen der Spalten, um eine konsistente und korrekte Datenverarbeitung sicherzustellen.

# CELL ********************

# Cast columns to their respective data types.
df_bestellungen = df_bestellungen.withColumn("KundenID", col("KundenID").cast("integer")) \
       .withColumn("Bestelldatum", to_date(col("Bestelldatum"), "dd.MM.yyyy")) \
       .withColumn("ProduktID", col("ProduktID").cast("integer")) \
       .withColumn("Bestellnummer", col("Bestellnummer").cast("string")) \
       .withColumn("VertriebsgebietID", col("VertriebsgebietID").cast("integer")) \
       .withColumn("Bestellmenge", col("Bestellmenge").cast("integer")) \
       .withColumn("Rücksendemenge", col("Rücksendemenge").cast("integer")) \
       .withColumn("Umsatz", regexp_replace(col("Umsatz"), ",", ".").cast("float")) \
       .withColumn("Lieferdatum", to_date(col("Lieferdatum"), "dd.MM.yyyy")) \
       .withColumn("Rücksendedatum", to_date(col("Rücksendedatum"), "dd.MM.yyyy"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 5. **Berechnung Lieferzeit** 
# Berechne die Lieferzeit als Differenz zwischen dem Lieferdatum und dem Bestelldatum.

# CELL ********************

# Calculate 'lieferzeit' = 'bestelldatum - lieferdatum
df_bestellungen = df_bestellungen.withColumn("lieferzeit", datediff(col("Lieferdatum"), col("Bestelldatum")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 6. **Tabelle schreiben** 
# Speichere den verarbeiteten DataFrame in deinem Lakehouse ab.

# CELL ********************

df_bestellungen.write.format("delta").mode("overwrite").save("Tables/dbo/df_bestellungen_gold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
