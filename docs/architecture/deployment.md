# Deployment

## Git und CI/CD

Der Code liegt in **GitHub**. Deployments laufen ausschließlich über **GitHub Actions** mit dem
Paket **fabric-cicd**, nie manuell. Niemals Fabric's native Deployment Pipeline.

Der Fokus liegt zunächst auf dem Deployment; DQ-Prüfungen
werden schrittweise ergänzt.

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

## Capacity

Fabric Capacity **F2**; dabei bleibt es vorerst. Eine kleinere SKU (F0) ist zur Kostenersparnis
interessant, ihr Release ist **offen** und wird geprüft, sobald sie verfügbar ist.

**Zu diskutieren:** Ob für histlanding und base reine Python-Notebooks (ohne Spark) auf der F2
günstiger sind als PySpark. MLVs setzen weiterhin Spark voraus.

## Secrets

Secrets liegen im **Azure Key Vault**, siehe [Access Control](../security/access-control.md).
