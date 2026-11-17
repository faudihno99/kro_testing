# Storage Layers

Alle Layer liegen als **Schemas in einem zentralen Lakehouse je Umgebung**
(`lh_dwh_dev` / `lh_dwh_prod`). Tabellennamen tragen kein Layer-Präfix; die Zuordnung erfolgt
ausschließlich über das Schema. Jede Tabelle ist eine Entität in `snake_case`.

## Landing

Rohdaten, unverändert. Der Zugriff erfolgt **Zero-ETL über Shortcuts** auf SharePoint, direkt in
die Files-Section des zentralen Lakehouse. Ein Shortcut synchronisiert in Echtzeit und macht eine
separate Ingestion-Logik entbehrlich. Excel- und CSV-Dateien landen so im Original im Lakehouse,
ohne Transformation.

Eine Shortcut-Transformation ist eine Option, sofern die Quelle eine klare Tabelle ist (CSV, XLSX).
Ob sie gegenüber der pandas-Ingestion zum Einsatz kommt, ist **offen** (`AGENTS.md` §9).

## Histlanding

Schema `histlanding`. Ziel ist die **Historisierung** der Landing-Daten in Delta-Tabellen; es ist
die **einzige Historie** der Plattform.

**Metadatenbasiert als Standard.** Standarddateien aus Quellsystemen (Parquet, JSON) werden
config-getrieben verarbeitet; Verhalten pro Quelle und Entität steht ausschließlich in der Config.

**Excel über Custom-Notebook.** Excel-Dateien lassen sich nicht sinnvoll rein über Config
abbilden und werden in einem eigenen Notebook gelesen. Dieses läuft auf einer **Single-Node-Engine**
statt auf einem Spark-Cluster, um das Hochfahren der Spark-Session zu vermeiden und auf der F2
schnell zu sein. Präferiert ist **DuckDB + delta-rs**; Alternativen sind **Polars + delta-rs** sowie
DuckDB oder Polars mit Konvertierung in einen Spark-DataFrame und Merge über den `DeltaHandler`.
Die Wahl ist **offen**, siehe [ADR 0003](../adr/0003-single-node-excel-ingestion.md). Entscheidend:
Die Engine muss **MERGE** unterstützen, damit die Historie erhalten bleibt (kein `overwrite`).
Packages kommen ausschließlich aus einem **Custom Environment**, nie per `pip install` in der Session.

Das Schema ist variabel, daher werden Excel-Spalten als **String** geführt. Vor dem Schreiben prüft
ein **Schema-Check** Spaltenstruktur und Datentypen gegen das Soll-Schema aus der Config; eine
Abweichung ist eine Exception und wird nie still angepasst. Casting findet erst in `base` statt.

## Base

Schema `base`. Hier erfolgt die fachlich-technische Aufbereitung: **Renaming, Datentyp-Casting,
Deduplizierung und Formatierung**. Es ist die einzige Stelle, an der gecastet wird. Business Keys
kommen ausschließlich aus der Config.

**Dubletten** mit gleichem Business Key werden **geplant** bereits in `nb_build_histlanding` vor dem
Schreiben in die Delta-Tabelle bereinigt. Der Umgang mit in der Quelle gelöschten Zeilen ist
**offen**.

## Mart

Pro Fachbereich existiert ein eigenes Schema `mart_<fachbereich>` im zentralen Lakehouse. Es
enthält das **dimensionale Modell** des Fachbereichs (Dimensionen und Faktentabellen in separaten
Materialized Lake Views).

Die Technik richtet sich danach, ob historisiert wird:

- **Ohne SCD2** → Materialized Lake Views (Spark SQL). Vorteile: Constraints und perspektivisch
  inkrementelle Aktualisierung; der Refresh muss angesteuert werden (siehe Orchestrierung).
- **Mit SCD2** → PySpark.

Ob eine Historisierung (SCD2) überhaupt gewünscht ist, ist **offen**. Ebenso ist das konkrete
Datenmodell noch **offen**.

## Bereitstellung an die Fachbereiche

Daten werden vorerst **nicht** über ein eigenes Lakehouse oder einen eigenen Workspace
bereitgestellt. Erst nach Rücksprache mit dem jeweiligen Fachbereich, ob direkter Zugriff auf die
Daten erforderlich ist, wird eine Bereitstellung (voraussichtlich per Shortcut, ohne Kopie, Rechte
über OneLake Security) entschieden. Bis dahin ist sie **offen**, siehe
[ADR 0002](../adr/0002-mart-layer.md).
