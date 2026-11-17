# Orchestration

Die Orchestrierung läuft über **Fabric Data Pipelines**: eine Master-Pipeline ruft modulare
Subpipelines auf. Notebooks werden in **High Concurrency** ausgeführt, um auf der Capacity F2
Session-Starts zu sparen.

## Steuerung über Parameter

| Parameter | Wirkung |
|---|---|
| `pRunHistlanding` | Lauf der histlanding-Stufe an/aus |
| `pRunBase` | Lauf der base-Stufe an/aus |
| `pSourcesToRun` | Auswahl der Quellen für diesen Lauf, default ["sharepoint"] |

Die **Kadenz** (monatlich DATEV, wöchentlich Eventim usw.) steuert ausschließlich `pSourcesToRun`
über die Pipeline-Schedules / Trigger-Notebooks. Kein Notebook und keine Config entscheidet nach Datum oder Frequenz.

## Build-Notebooks

`nb_build_histlanding` und `nb_build_base` sind dünne Einstiegspunkte: Configs per `%run` laden,
nach `pSourcesToRun` filtern, abfs-Pfade einmal aus der Variable Library auflösen, Preflight für
alle gewählten Quellen vor dem ersten Schreibzugriff, dann Verarbeitung über die Utils.

Notebook-Aktivitäten mit High-Concurreny und Session-Tag optimieren.

## Serving-Refresh

Die Materialized Lake Views in `serving_*` werden über die Pipeline-Aktivität **„Refresh MLV“**
aktualisiert (alternativ per Notebook, mit bekannten Limitationen).

## Fehlerbehandlung und Alerting

Notebooks werfen bei jedem Fehler eine Exception und brechen ab (fail early). Das Alerting liegt
ausschließlich in der Pipeline und erfolgt per **E-Mail**.

Die Teams-Aktivität der Data Pipeline wird **nicht** genutzt: Ihre Connection lässt sich nicht
teilen, jede Person, die die Pipeline ausführt, müsste die Connection selbst neu anlegen.
