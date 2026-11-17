# Datenarchitektur DWH – Soll

Leitprinzip: **Pragmatismus und Stabilität.**

Diese Dokumentation beschreibt die **Zielarchitektur** der DWH-Plattform. Sie ist aus dem
Konzept (Notion „Konzeption Datenarchitektur") und den Regeln in `AGENTS.md` abgeleitet und
gilt als Sollbeschreibung, nicht als Beschreibung eines bereits vollständig umgesetzten Stands.
Der aktuell im Repository liegende Code ist **nicht** die Quelle dieser Beschreibung.

## Status-Legende

Jede Aussage in dieser Doku gehört in eine der drei Kategorien. Wo nichts anderes steht, gilt
sie als **entschieden**.

| Status | Bedeutung |
|---|---|
| **entschieden** | Fachlich oder technisch festgelegt, in `AGENTS.md` verankert. |
| **geplant** | Entschieden, aber noch nicht umgesetzt. |
| **offen** | Bewusst unentschieden; Entscheidung ist im Agent-Betrieb untersagt (siehe `AGENTS.md` §9). |

## Gesamtbild

Alle Layer liegen als Schemas in einem zentralen Lakehouse je Umgebung. Die Daten durchlaufen
vier Stufen; die Daten werden im Mart je Fachbereich bereitgestellt. Eine separate Bereitstellung per Shortcut ist zurückgestellt, bis der Fachbereich direkten Datenzugriff bestätigt.

```mermaid
flowchart LR
  Q["Quellsysteme<br/>SharePoint, DATEV, theasoft, Eventim"] --> L["landing<br/>Files-Section, Shortcut"]
  L --> H["histlanding<br/>Delta, PySpark"]
  H --> B["base<br/>PySpark"]
  B --> S["mart_fachbereich<br/>MLV / PySpark"]
  P["Fabric Data Pipelines"] -.->|"orchestriert"| H
  P -.-> B
  P -.-> S
```

## Layer-Übersicht

| Layer | Ort | Technik |
|---|---|---|
| landing | Files-Section des zentralen Lakehouse | OneLake-Shortcut auf SharePoint, keine Logik |
| histlanding | Schema `histlanding` | Standardformate (Parquet, JSON) config-getrieben; Excel über Custom-Notebook auf Single-Node-Engine, alle Spalten als String |
| base | Schema `base` | PySpark: Renaming, Casting, Dedup, Formate |
| mart_&lt;fachbereich&gt; | eigenes Schema je Fachbereich | Materialized Lake Views (Spark SQL), sonst PySpark |

## Dokumente

- [Architecture · Overview](architecture/overview.md) – Gesamtbild: Datenfluss, Workspaces, Building Blocks
- [Architecture · Storage Layers](architecture/storage-layers.md) – Layer, Orte, Techniken, Fluss
- [Architecture · Sources](architecture/sources.md) – Quellen, Kadenz, zentraler Schlüssel
- [Architecture · Formats](architecture/formats.md) – Dateiformate
- [Architecture · Naming Conventions](architecture/naming-conventions.md) – Workspaces, Lakehouses, Schemas, Tabellen, Notebooks
- [Architecture · Lifecycle & Retention](architecture/lifecycle-retention.md) – Historie, Wartung
- [Architecture · Orchestration](architecture/orchestration.md) – Pipelines, Parameter, Alerting
- [Architecture · Data Quality](architecture/data-quality.md) – Schema-Check, Great Expectations
- [Architecture · Deployment](architecture/deployment.md) – CI/CD, Umgebungen, Capacity
- [Security · Access Control](security/access-control.md) – Workspaces, Entra-Gruppen, OneLake Security
- [ADR 0001 · Storage Zone Layout](adr/0001-storage-zone-layout.md)
- [ADR 0002 · Mart-Layer ohne Serving-Lakehouse](adr/0002-mart-layer.md)
- [ADR 0003 · Single-Node-Engine für Excel-Ingestion](adr/0003-single-node-excel-ingestion.md)
- [CHANGELOG](CHANGELOG.md)

## Offene Punkte (Überblick)

Die folgenden Themen sind bewusst unentschieden. Sie sind hier gesammelt, damit sie auffindbar
bleiben:

- Datenmodell für `mart_*` (dimensionales Modell noch unbekannt).
- Ob eine Historisierung (SCD2) in `mart_*` gewünscht ist und damit MLV oder PySpark.
- Bereitstellung an Fachbereiche (`lh_`/`ws_`-Shortcut): erst nach Rücksprache, ob direkter Zugriff nötig ist.
- Engine-Wahl für die Excel-Ingestion (DuckDB + delta-rs präferiert) und Prüfung der delta-rs-Merge-Fähigkeit.
- Paketliste des Custom Environment.
- CI-Details: Notebook→Python-Konvertierung für Unit-Tests, ruff-Konfiguration (Abstimmung mit Simon R., taod).
- Umgang mit in der Quelle gelöschten Zeilen.
- Retention-Werte und Wartungsregel (VACUUM / Table Maintenance) für `histlanding`.
- Feingranulares RBAC-Konzept.
- Ablage der Great-Expectations-Regeln und -Ergebnisse.
- Variable Library: Name, Variablen, Value Sets.
- Ob die Single-Node-Engine über die Excel-Ingestion hinaus auch für base eingesetzt wird.
- F0: prüfen, sobald verfügbar.
- Katalog-Sichtbarkeit pfadgeschriebener Tabellen für MLVs.
- Lakehouse-Bindung der MLV-Notebooks beim Deployment.
