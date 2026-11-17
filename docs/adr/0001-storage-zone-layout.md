# ADR 0001 – Storage Zone Layout

- **Status:** Accepted
- **Datum:** 2026-11-17

## Kontext

Die Plattform muss mehrere Verarbeitungsstufen (Rohdaten, Historisierung, Aufbereitung, Mart)
trennen, gleichzeitig aber den Betriebsaufwand und die Komplexität gering halten. Frühere
Überlegungen sahen eigene Lakehouses pro Layer vor.

## Entscheidung

Es gibt **ein zentrales Lakehouse je Umgebung** (`lh_dwh_dev` / `lh_dwh_prod`). Die Layer sind
**Schemas** innerhalb dieses Lakehouse: `landing` (Files-Section, Shortcut auf SharePoint),
`histlanding`, `base` und je Fachbereich `mart_<fachbereich>`.

Jeder Fachbereich erhält ein eigenes `mart_<fachbereich>`-Schema. Zur Bereitstellung an die
Fachbereiche siehe [ADR 0002](0002-mart-layer.md).

Tabellennamen tragen kein Layer-Präfix; das Schema ordnet die Tabelle ein.

## Konsequenzen

- Geringere Anzahl an Fabric-Items; Verwaltung und Deployment bleiben einfach.
- Governance der Mart-Daten über OneLake Security auf einem zentralen Lakehouse.
- MLVs müssen auf dem zentralen Lakehouse liegen; die Pfadschreibung und Katalog-Sichtbarkeit
  sowie die Lakehouse-Bindung der MLV-Notebooks beim Deployment sind gesondert zu klären (offen).
- Die Trennung erfolgt über Namens- und Schema-Konventionen statt über Item-Grenzen.
