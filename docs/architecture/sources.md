# Sources

Phase 1 umfasst das Veranstaltungscontrolling (Plan-Soll-Ist) und die monatliche BWA.
Kurzfristig kommen die Quellen als Excel/CSV über SharePoint, langfristig direkt aus den
Quellsystemen.

| Quelle | Inhalt | Kadenz | Besonderheiten |
|---|---|---|---|
| DATEV | Ist-Kosten/-Erlöse, BWA | monatlich | Einzelposten mit Sachkonto, Kostenstelle, Kostenträger |
| theasoft / thea.order | Plan, Soll, Obligos | – | Soll überschreibt Plan im Quellsystem; keine Historie im Quellsystem; Dubletten; uneinheitliche Datumsformate |
| Eventim | Ticketverkäufe | wöchentlich | 2–3 CSV-Dateien |

## Zentraler Schlüssel

Der **Kostenträger** verbindet die Quellen: DATEV `COST2` = Eventim „Projekt-Infocode“.

## Fachliche Hinweise

- Eventim-Ticketpreise entsprechen **nicht** den DATEV-Erlösen (Gebühren, Provisionen) und werden
  nie direkt gleichgesetzt.
- Mappings (Sachkonto ↔ Kostenkategorie / BWA-Zeile / Kostenträger / Veranstaltung) liegen in
  fachlicher Hoheit der Kronberg Academy.
