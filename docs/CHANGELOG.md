# Changelog

Alle nennenswerten Änderungen an dieser Dokumentation. Format angelehnt an „Keep a Changelog",
Versionierung über Git-Tags (`docs-vX.Y`).

## [Unreleased]

### Added
- Initiale Sollbeschreibung der Datenarchitektur unter `docs/` (Index, Architecture, Security, ADR).
- ADR 0001 – Storage Zone Layout (Accepted).
- Architecture: Sources, Orchestration, Deployment, Data Quality.

### Changed
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
