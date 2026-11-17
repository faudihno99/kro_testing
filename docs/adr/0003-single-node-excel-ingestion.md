# ADR 0003 – Single-Node-Engine für Excel-Ingestion

- **Status:** Accepted (Ziel); Engine-Wahl offen
- **Datum:** 2026-11-17

## Kontext

Excel-Dateien lassen sich nicht rein config-getrieben lesen. Bisher war pandas + PySpark
vorgesehen. Das Hochfahren eines Spark-Clusters kostet auf der Capacity F2 Zeit und Kapazität,
obwohl die Datenmengen klein sind. `histlanding` ist die einzige Historie und darf nur per MERGE
geschrieben werden, nie per `overwrite`.

## Entscheidung

Excel wird in einem **Custom-Notebook** gelesen, das auf einer **Single-Node-Engine** läuft. Ziel:
kein Spark-Start, schnelle Verarbeitung. Standarddateien (Parquet, JSON) bleiben metadatenbasiert
und config-getrieben; Custom-Lösungen sind die Ausnahme.

Optionen in Reihenfolge der Präferenz:

1. **DuckDB + delta-rs** (präferiert).
2. **Polars + delta-rs**.
3. **DuckDB oder Polars + Konvertierung in einen Spark-DataFrame**, Merge über den `DeltaHandler`
   (startet Spark, widerspricht dem Ziel teilweise).

**Vor der Umsetzung zu prüfen:** Unterstützt delta-rs MERGE in dem Umfang, den die
Historisierung braucht (Business Key, Change-Detection, kein Write bei leerem Change-Set), und ist
es kompatibel mit den Delta-Table-Features der bestehenden Fabric-Tabellen? Fällt die Prüfung
negativ aus, gilt Option 3.

Packages kommen ausschließlich aus einem **Custom Environment**; **kein `pip install` in der
Session**.

## Konsequenzen

- Die Engine-Wahl ist erst nach der Merge-Prüfung final; die Paketliste des Environments folgt.
- Das Excel-Notebook ist eine Ausnahme von „nur Config und Utils“; Name und Einbindung sind offen.
- Ob die Engine auch für `base` genutzt wird, ist offen.
