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

# ## **Übung 3 - Datenaufbereitung mit dem Notebook**

# MARKDOWN ********************

# #### **3c) Default-Lakehouse**
# Stelle dein Lakehouse als Standard ein, um Abfragen mit relativen Pfaden zu vereinfachen. Verwende dazu die Explorerleiste auf der linken Seite.

# MARKDOWN ********************

# ####  **3d) Importieren der Bibliotheken**
# In diesem Abschnitt werden alle erforderlichen Bibliotheken und Module importiert, die für die Durchführung der Analysen und Berechnungen im Notebook notwendig sind.

# CELL ********************

from pyspark.sql.functions import col, when, initcap, concat_ws, lit, to_date
from pyspark.sql.types import IntegerType, FloatType, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### **3e) Tabelle laden**
# Importiere die Tabelle "adventureworks_bestellungen_roadshow" aus deinem Lakehouse in das Notebook. Passe gegebenenfalls den Variablennamen "tabellenname" an, falls ein anderer Tabellenname verwendet wird.

# CELL ********************

# Read table "kundentabelle" from lakehouse
tabellenname = "Tables/dbo/xxx"
df_kunden = spark.read.format("delta").load(tabellenname)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ##### **Wie sieht die derzeitige Tabelle aus?**

# CELL ********************

# Print table
display(df_kunden)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **3f) Anpassung Datentyp**
# Ändere die Datentypen der Spalten, um eine konsistente und korrekte Datenverarbeitung sicherzustellen.

# CELL ********************

# Datentypen anpassen
df_kunden = df_kunden.withColumn("geburtsdatum", to_date(col("geburtsdatum"), "dd.MM.yyyy")) \
       .withColumn("prefix", df_kunden["prefix"].cast(StringType())) \
       .withColumn("vorname", df_kunden["vorname"].cast(StringType())) \
       .withColumn("nachname", df_kunden["nachname"].cast(StringType())) \
       .withColumn("familienstand", df_kunden["familienstand"].cast(StringType())) \
       .withColumn("geschlecht", df_kunden["geschlecht"].cast(StringType())) \
       .withColumn("bildungslevel", df_kunden["bildungslevel"].cast(StringType())) \
       .withColumn("beruf", df_kunden["beruf"].cast(StringType())) \
       .withColumn("hausbesitzer_in", df_kunden["hausbesitzer_in"].cast(StringType())) \
       .withColumn("kundenid", df_kunden["kundenid"].cast(IntegerType())) \
       .withColumn("anzahl_kinder", df_kunden["anzahl_kinder"].cast(IntegerType())) \
       .withColumn("einkommen", df_kunden["einkommen"].cast(FloatType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ##### **Sind die Datentypen nun korrekt?**

# CELL ********************

# Print current table schema
df_kunden.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **3g) Datenaufbereitung**

# MARKDOWN ********************

# ##### 1. Präfix, Vorname und Nachname

# CELL ********************

# Datentypen anpassen und Text formatieren
df_kunden = df_kunden.withColumn("prefix", initcap(df_kunden["prefix"])) \
       .withColumn("vorname", initcap(df_kunden["vorname"])) \
       .withColumn("nachname", initcap(df_kunden["nachname"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2. Geschlecht

# CELL ********************

# Leere Werte in der Spalte "Geschlecht" durch NaN ersetzen
df_kunden = df_kunden.withColumn("Geschlecht", 
                    when(df_kunden["Geschlecht"] == "", lit(float('nan')))
                    .otherwise(df_kunden["Geschlecht"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3. Hausbesitzer:in

# CELL ********************

# Replace 'J' with 'Ja' and 'N' with 'Nein' in the 'hausbesitzer_in' column
df_kunden = df_kunden.withColumn(
    "hausbesitzer_in", 
    when(col("hausbesitzer_in") == "J", "Ja")
    .when(col("hausbesitzer_in") == "N", "Nein")
    .otherwise(col("hausbesitzer_in")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Wie sieht die finale Tabelle aus?**

# CELL ********************

# Print table
display(df_kunden)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **3h) Tabelle in das Lakehouse schreiben**
# Speichere die verarbeitete Tabelle in deinem Lakehouse.

# CELL ********************

# Write df into the lakehouse
df_kunden.write.format("delta").mode("overwrite").save("Tables/dbo/kunden")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
