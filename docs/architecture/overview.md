# Architecture Overview

Einstiegsseite zur Zielarchitektur. Sie zeigt das Gesamtbild und verlinkt in die Detaildokumente;
Details und Begründungen stehen nur dort. Wie überall in dieser Doku gilt der **Soll-Zustand**,
nicht der Stand des Codes. Status-Legende siehe [Index](../index.md).

In den Diagrammen sind **geplante** Elemente gestrichelt, **offene** gepunktet dargestellt.

## 1. Kontext und Datenfluss

Quellen liefern kurzfristig Excel/CSV über SharePoint. Die Daten durchlaufen vier Layer im
zentralen Lakehouse, das im Workspace `ws_dwh_dev` bzw. `ws_dwh_prod` liegt. Die Fachbereiche
erhalten die Serving-Daten in einem eigenen Workspace `ws_serving_<fachbereich>` per Shortcut,
ohne Kopie. Dort liegen auch das semantische Modell und die Reports.

```mermaid
flowchart LR
  subgraph SRC["Quellen"]
    D["DATEV<br/>monatlich"]
    T["theasoft / thea.order"]
    E["Eventim<br/>wöchentlich"]
  end
  SP["SharePoint<br/>Excel / CSV"]

  subgraph WSDWH["Workspace ws_dwh_dev / ws_dwh_prod"]
    subgraph LH["Zentrales Lakehouse lh_dwh_dev / lh_dwh_prod"]
      L["landing<br/>Files-Section"]
      H["histlanding<br/>Delta, einzige Historie"]
      B["base<br/>Renaming, Casting, Dedup"]
      S["serving_fachbereich<br/>dimensionales Modell"]
    end
  end

  subgraph WSS["Workspace ws_serving_fachbereich"]
    SL["lh_serving_fachbereich<br/>Shortcut"]
    SM["Semantisches Modell"]
    R["Report"]
  end

  D --> SP
  T --> SP
  E --> SP
  SP -- "Shortcut" --> L
  L --> H --> B --> S
  S -- "Shortcut, keine Kopie" --> SL
  SL --> SM --> R

  classDef open stroke-dasharray: 2 4
  class S,SM,R open
```

Das Datenmodell von `serving_*` ist **offen**. Semantisches Modell und Reports sind in den
übrigen Dokumenten noch nicht beschrieben. Details: [Storage Layers](storage-layers.md),
[Sources](sources.md), [ADR 0001](../adr/0001-storage-zone-layout.md).

## 2. Workspace-Struktur

Workspaces folgen der Separation of Concerns: wer verwaltet, wer sieht. `ws_dwh_dev` ist der
Test-Workspace, in dem Änderungen aus den Feature-Branches zusammenkommen. Entwickelt wird in
`ws_feature_*` auf Basis von GitHub. Von `ws_dwh_dev` nach `ws_dwh_prod` läuft die Promotion über
GitHub Actions. `ws_dwh_prod` ist die einzige Umgebung, die per Shortcut mit `ws_serving_*`
verbunden ist.

```mermaid
flowchart TB
  DEV["<b>ws_dwh_dev</b> (Test)<br/>lh_dwh_dev<br/>Pipelines + Notebooks"]

  subgraph FEAT["Feature-Entwicklung (Grundlage: GitHub)"]
    FW["ws_feature_*"]
    FB["Feature-Branch"]
  end

  GHA["<b>GitHub</b><br/>GitHub Actions"]
  PROD["<b>ws_dwh_prod</b> (Produktion)<br/>lh_dwh_prod<br/>Pipelines + Notebooks"]
  SERV["<b>ws_serving_fachbereich</b><br/>lh_serving_fachbereich"]

  DEV ---|"Review + Merge"| FEAT
  FW --- FB
  DEV --> GHA
  GHA --> PROD
  PROD -- "Shortcut + OneLake Security" --> SERV
```

Details: [Naming Conventions](naming-conventions.md), [Deployment](deployment.md).

## 3. Building Blocks

| Baustein | Technik | Aufgabe | Doku |
|---|---|---|---|
| Speicher | Fabric Lakehouse (OneLake, Delta) | Ein Lakehouse je Umgebung, Layer als Schemas | [Storage Layers](storage-layers.md) |
| Orchestrierung | Fabric Data Pipelines | Master + Subpipelines, Parameter, MLV-Refresh, E-Mail-Alerting | [Orchestration](orchestration.md) |
| Verarbeitung histlanding / base | PySpark-Notebooks; Python-Notebooks (delta-rs) **zu diskutieren** | Ingestion, Schema-Check, Casting, Dedup | [Deployment](deployment.md) |
| Verarbeitung serving | Materialized Lake Views (Spark SQL); PySpark bei SCD2 | Dimensionales Modell je Fachbereich | [Storage Layers](storage-layers.md) |
| Konfiguration | Variable Library, Environments | Umgebungswerte (abfs-Pfade), Spark-Settings | [Deployment](deployment.md) |
| Deployment | GitHub Actions + fabric-cicd | Einziger Weg in die Workspaces | [Deployment](deployment.md) |
| Datenqualität | Schema-Check, Great Expectations | `info` loggt, `error` bricht ab | [Data Quality](data-quality.md) |
| Zugriff | Entra-ID-Gruppen, Workspace-Rollen, OneLake Security (RBAC) | Rechte auf Serving-Daten | [Access Control](../security/access-control.md) |
| Secrets | Azure Key Vault | Keine Secrets im Code | [Access Control](../security/access-control.md) |
| Compute | Fabric Capacity F2 | Rahmen für alle Workloads | [Deployment](deployment.md) |

## Offene Punkte mit Architekturbezug

Die vollständige Liste steht im [Index](../index.md). Für das Gesamtbild sind relevant:

- Config/Metadaten-Drive vs. Custom-Notebooks.
- Standard-Utils?
- Python-Notebooks (delta-rs) statt PySpark für histlanding
- Sinn Histlanding
- Datenbereitstellung: Custom Notebooks, MLVs, dbt?
- Tipps fabric cicd:  Unit-Testing PyTest, Sonstige Tipps
