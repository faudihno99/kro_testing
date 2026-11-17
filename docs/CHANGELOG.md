# Changelog

Alle nennenswerten Änderungen an dieser Dokumentation. Format angelehnt an „Keep a Changelog",
Versionierung über Git-Tags (`docs-vX.Y`).

## [Unreleased]

### Added
- Initiale Sollbeschreibung der Datenarchitektur unter `docs/` (Index, Architecture, Security, ADR).
- Architecture Overview (`architecture/overview.md`): Datenfluss, Workspace-Struktur, Building Blocks.
- ADR 0001 – Storage Zone Layout (Accepted).
- Architecture: Sources, Orchestration, Deployment, Data Quality.
- ADR 0002 – Mart-Layer ohne Serving-Lakehouse (Accepted).
- ADR 0003 – Single-Node-Engine für Excel-Ingestion (Accepted; Engine-Wahl offen).
- Deployment: CI-Workflow (Unit-Tests per Notebook→Python-Konvertierung, ruff, Variable Libraries) und Abschnitt Packages (kein `pip install`, Custom Environment).

### Changed
- Layer `serving_<fachbereich>` → `mart_<fachbereich>` in der gesamten Doku und in `AGENTS.md`.
- Keine Bereitstellung über `lh_serving_*` / `ws_serving_*`; erst nach Rücksprache mit dem Fachbereich (ADR 0002).
- Excel-Ingestion: Custom-Notebook auf Single-Node-Engine statt pandas + PySpark (ADR 0003); Standardformate config-getrieben.
- `AGENTS.md`: Regel „kein `pip install`“, CI-/Lint-Commands, offene Punkte aktualisiert.
- Alerting ausschließlich per E-Mail aus der Pipeline; Teams-Aktivität verworfen (Connection nicht teilbar).
- Data Quality: Great Expectations mit Severity `info` (Log) / `error` (Abbruch).
- Workspace-Rechte vereinfacht: zentrale Viewer-Gruppe, Verwaltung durch Engineering-Gruppe.
- Dubletten werden in `nb_build_histlanding` vor dem Schreiben bereinigt (geplant).
- `formats-partitioning.md` → `formats.md`.

### Removed
- ADR 0002 – Partitioning Strategy; Partitionierung ist aktuell kein Thema.
- Abschnitt „Tiers und Löschfristen“ in Lifecycle & Retention.

### Notes
- Diese Struktur ersetzt den früheren Ansatz einer flachen Sammlung von Layer-Dateien
  (`landing.md`, `histlanding.md`, `base.md`, `serving_*.md`, `data catalog for base`).
- Doku beschreibt den Soll-Zustand aus Konzept und `AGENTS.md`; der Repo-Code ist nicht die
  Quelle.

<!--
Nach Review und Freigabe:
- Abschnitt `[Unreleased]` nach `[1.0]` umziehen, Git-Tag `docs-v1.0` setzen.
-->
