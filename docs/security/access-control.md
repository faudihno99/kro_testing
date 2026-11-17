# Access Control

Grundlage sind **Sicherheitsgruppen in Entra ID**. Sie werden frühzeitig angelegt, damit die
Plattform sie von Anfang an berücksichtigt. Leitlinie: **keep it simple**, Rechte schrittweise
erweitern, sobald fehlende Berechtigungen bekannt werden.

## Workspace-Rollen

- **Engineering-Gruppe:** Admin in den Workspaces. Verwaltet alle Berechtigungen.
- **Zentrale Gruppe der Fachbereiche:** Rolle **Viewer**

## Datenzugriff der Fachbereiche

Die Mart-Daten (`mart_<fachbereich>`) bleiben in `lh_dwh_dev` / `lh_dwh_prod` und werden nicht
kopiert. Eine Bereitstellung über ein eigenes Lakehouse/einen eigenen Workspace per Shortcut gibt
es **vorerst nicht**; sie wird erst nach Rücksprache mit dem Fachbereich entschieden, ob direkter
Zugriff auf die Daten erforderlich ist. Greift sie, gilt **OneLake Security** als Governance.

## Secrets

Secrets werden ausschließlich im **Azure Key Vault (AKV)** abgelegt. Keine hartkodierten Secrets,
Workspace- oder Lakehouse-IDs.

## Offen

Ein feingranulares RBAC-Konzept (z. B. Rollen je Fachbereich, Rechte über Shortcuts hinweg) ist
**offen** und wird erst bei konkretem Bedarf ergänzt.
