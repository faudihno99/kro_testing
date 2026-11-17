# Data Quality

## Schema-Check (Pflicht)

Vor dem Schreiben nach `histlanding` prüft ein Schema-Check Spaltenstruktur und Datentypen gegen
das Soll-Schema aus der Config. Excel-Quellen führen alle Spalten als String. Eine Abweichung ist
eine Exception; das Schema wird nie still angepasst.

## Great Expectations

Darüber hinaus werden DQ-Regeln mit **Great Expectations** formuliert. Gründe:

- Die Regeln und ihre Ergebnisse werden **persistiert** und sind so für das Monitoring nutzbar.
- Jede Regel trägt eine **Severity**, aus der die Maßnahme folgt:

| Severity | Maßnahme |
|---|---|
| `info` | Ergebnis wird geloggt, der Lauf geht weiter |
| `error` | Exception, der Lauf bricht ab |

Wo Regeln und Ergebnisse abgelegt werden, ist **offen**.
