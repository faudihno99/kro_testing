# Access Control

Grundlage sind **Sicherheitsgruppen in Entra ID**. Sie werden frühzeitig angelegt, damit die
Plattform sie von Anfang an berücksichtigt. Leitlinie: **keep it simple**, Rechte schrittweise
erweitern, sobald fehlende Berechtigungen bekannt werden.

## Workspace-Rollen

- **Engineering-Gruppe:** Admin in den Workspaces. Verwaltet alle Berechtigungen.
- **Zentrale Gruppe der Fachbereiche:** Rolle **Viewer**.

## Datenzugriff über Shortcuts

Die Serving-Daten bleiben in `lh_dwh_dev` / `lh_dwh_prod` und werden nicht kopiert. Der
Fachbereich sieht sie über den Shortcut in `lh_serving_<fachbereich>` (Workspace
`ws_serving_<fachbereich>`); die Governance greift über **OneLake Security**.

## Secrets

Secrets werden ausschließlich im **Azure Key Vault (AKV)** abgelegt. Keine hartkodierten Secrets,
Workspace- oder Lakehouse-IDs.

## Offen

Ein feingranulares RBAC-Konzept (z. B. Rollen je Fachbereich, Rechte über Shortcuts hinweg) ist
**offen** und wird erst bei konkretem Bedarf ergänzt.
