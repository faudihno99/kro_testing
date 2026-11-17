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
die **einzige Historie** der Plattform. Die Verarbeitung erfolgt in PySpark-Notebooks.

Besonderheit: Excel-Dateien werden über **pandas** gelesen und direkt in Spark-DataFrames
überführt. Das Schema ist variabel, daher werden landing-seitig **alle Spalten als String**
geführt. Vor dem Schreiben prüft ein **Schema-Check** Spaltenstruktur und Datentypen gegen das
Soll-Schema aus der Config; eine Abweichung ist eine Exception und wird nie still angepasst.
Casting findet erst in `base` statt.

Sonderfall Excel: Ob Shortcut-Transformationen Infrage kommen, ist **offen**.

## Base

Schema `base`. Hier erfolgt die fachlich-technische Aufbereitung: **Renaming, Datentyp-Casting,
Deduplizierung und Formatierung**. Es ist die einzige Stelle, an der gecastet wird. Business Keys
kommen ausschließlich aus der Config.

**Dubletten** mit gleichem Business Key werden **geplant** bereits in `nb_build_histlanding` vor dem
Schreiben in die Delta-Tabelle bereinigt. Der Umgang mit in der Quelle gelöschten Zeilen ist
**offen**.

## Serving

Pro Fachbereich existiert ein eigenes Schema `serving_<fachbereich>` im zentralen Lakehouse. Es
enthält das **dimensionale Modell** des Fachbereichs (Dimensionen und Faktentabellen in separaten
Materialized Lake Views).

Die Technik richtet sich danach, ob historisiert wird:

- **Ohne SCD2** → Materialized Lake Views (Spark SQL). Vorteile: Constraints und perspektivisch
  inkrementelle Aktualisierung; der Refresh muss angesteuert werden (siehe Orchestrierung).
- **Mit SCD2** → PySpark.

Ob eine Historisierung (SCD2) überhaupt gewünscht ist, ist **offen**. Ebenso ist das konkrete
Datenmodell noch **offen**.

## Bereitstellung an die Fachbereiche

Die Serving-Daten werden **nicht physisch kopiert**. Stattdessen liegt in einem eigenen Lakehouse
`lh_serving_<fachbereich>` (Workspace `ws_serving_<fachbereich>`) ein **Shortcut** auf das
jeweilige `serving_<fachbereich>`-Schema des zentralen Lakehouse. Die Rechte werden über
**OneLake Security** gesteuert.
