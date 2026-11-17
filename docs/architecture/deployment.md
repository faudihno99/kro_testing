# Deployment

## Git und CI/CD

Der Code liegt in **GitHub**. Deployments laufen ausschließlich über **GitHub Actions** mit dem
Paket **fabric-cicd**, nie manuell. Niemals Fabric's native Deployment Pipeline.

Der Fokus liegt zunächst auf dem Deployment; DQ-Prüfungen
werden schrittweise ergänzt.

## CI-Workflow

Vor dem Deployment laufen in GitHub Actions:

- **Unit-Tests** (`unittest`): Die Fabric-Notebooks werden im CI-Workflow in Python-Dateien
  konvertiert und als Tests ausgeführt. Das Verfahren ist **offen** und wird mit Simon R. (taod)
  abgestimmt.
- **Linting** des Codes mit **ruff**, gegebenenfalls auch der Dokumentation. Die Konfiguration
  ist **offen**.
- **Variable Libraries** definieren die Umgebungsparameter zentral (siehe unten).

## Umgebungen

| Workspace | Zweck |
|---|---|
| `ws_feature_*` | Entwicklung in Feature-Branches |
| `ws_dwh_dev` | Test |
| `ws_dwh_prod` | Produktion |

Änderungen gelangen per Review und Merge über die Umgebungen nach Produktion.

## Umgebungsabhängige Werte

Umgebungsabhängige Werte, einschließlich der abfs-Pfade, kommen ausschließlich aus der
**Variable Library**; fabric-cicd setzt sie je Umgebung. Name, Variablen und Value Sets
(inkl. `ws_feature_*`) sind **offen**.

## Spark-Konfiguration

Spark-Settings werden ausschließlich über **Environments** gepflegt. So lässt sich die Compute
je Workload individuell steuern.

## Packages

Es gibt **kein `pip install` in der Session**. Alle Packages (z. B. DuckDB, delta-rs für die
Single-Node-Ingestion) werden in einem **Custom Environment** definiert. Die konkrete Paketliste
mit Versionen ist **offen**.

## Capacity

Fabric Capacity **F2**; dabei bleibt es vorerst. Eine kleinere SKU (F0) ist zur Kostenersparnis
interessant, ihr Release ist **offen** und wird geprüft, sobald sie verfügbar ist.

Die Excel-Ingestion nach `histlanding` läuft auf einer Single-Node-Engine (Python-Notebook, ohne
Spark-Cluster), siehe [ADR 0003](../adr/0003-single-node-excel-ingestion.md). Ob die Engine auch
für `base` genutzt wird, ist **offen**. MLVs setzen weiterhin Spark voraus.

## Secrets

Secrets liegen im **Azure Key Vault**, siehe [Access Control](../security/access-control.md).
