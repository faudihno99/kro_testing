# ADR 0002 – Mart-Layer ohne Serving-Lakehouse

- **Status:** Accepted
- **Datum:** 2026-11-17

## Kontext

Der oberste Layer hieß bisher `serving_<fachbereich>` und sollte über einen Shortcut in ein
eigenes Lakehouse `lh_serving_<fachbereich>` (Workspace `ws_serving_<fachbereich>`) bereitgestellt
werden. Ob die Fachbereiche überhaupt direkten Zugriff auf die Daten brauchen, ist nicht geklärt.

## Entscheidung

Der Layer heißt **`mart_<fachbereich>`**, durchgängig in Schemas, Doku und `AGENTS.md`.

Es werden **vorerst keine** `lh_serving_*`-Lakehouses und `ws_serving_*`-Workspaces angelegt. Eine
Bereitstellung wird erst entschieden, nachdem der Fachbereich bestätigt hat, ob direkter Zugriff
auf die Daten erforderlich ist.

## Konsequenzen

- Weniger Fabric-Items und weniger Rechteverwaltung in der ersten Phase.
- Zugriff der Fachbereiche und Naming einer späteren Bereitstellung sind **offen** und
  werden nach der Rücksprache in einem eigenen ADR festgehalten.
- Ersetzt die Bereitstellungsaussage aus [ADR 0001](0001-storage-zone-layout.md).
