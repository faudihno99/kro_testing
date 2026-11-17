# Lifecycle & Retention

## Historie

`histlanding` ist die **einzige Historie** der Plattform. Die Historie ergibt sich aus dem
**Delta Table Time Travel**, nicht aus einer separaten Historisierungsschicht. Tabellen im Schema
`histlanding` werden daher nie gedroppt, neu angelegt, mit `overwrite`/`overwriteSchema`
geschrieben, truncatet oder mit verkürzter Retention gevacuumt.

## Wartung und Retention

**Offen** (`AGENTS.md` §9). Es ist bislang nicht festgelegt, welche Retention-Werte gelten und
nach welcher Regel die Tabellenwartung (VACUUM / Table Maintenance) für `histlanding` läuft.
Solange das ungeklärt ist, gilt keine automatische Wartung mit verkürzter Retention.

## Full-Rebuild

Ein vollständiger Rebuild von `histlanding` findet nur nach ausdrücklicher Rückfrage statt. Auf der
Capacity F2 werden unnötige Full-Rebuilds vermieden.
