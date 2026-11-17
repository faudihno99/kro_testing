# Naming Conventions

## Workspaces

Der Workspace-Aufbau folgt der **Separation of Concerns**: er richtet sich daran, wer Inhalte
verwaltet und sehen darf.

| Workspace | Zweck |
|---|---|
| `ws_feature_*` | Dev-Workspaces für Arbeit in Feature-Branches |
| `ws_dwh_dev` | Test-Workspace |
| `ws_dwh_prod` | Produktions-Workspace |
| `ws_serving_<fachbereich>` | Bereitstellung für den Fachbereich, Zugriff über Shortcut |

Standardmäßig existieren zwei DWH-Workspaces (`ws_dwh_dev`, `ws_dwh_prod`) plus die
fachbereichsspezifischen Serving-Workspaces.

## Lakehouses

Es gibt **ein Lakehouse je Umgebung** (`lh_dwh_dev` / `lh_dwh_prod`), in dem die Layer als
Schemas liegen. Je Fachbereich kommt ein Serving-Lakehouse `lh_serving_<fachbereich>` hinzu, das
per Shortcut auf das zentrale Lakehouse zeigt.

## Schemas

Die Layer sind Schemas im zentralen Lakehouse: `histlanding`, `base` und je Fachbereich
`serving_<fachbereich>`. Neue Layer werden nicht als eigenes Lakehouse angelegt.

## Tabellen und Spalten

Tabellennamen sind die Entität in `snake_case` und enthalten **kein Layer-Präfix** – das Schema
ordnet die Tabelle ein. Spaltennamen ebenfalls in `snake_case`, englische Identifier. Business
Keys werden ausschließlich aus der Config bestimmt.

## Notebooks

Benennung nach Funktion:

| Präfix | Bedeutung |
|---|---|
| `nb_build_<layer>` | zentrales, entitätsunabhängiges Build-Notebook (`nb_build_histlanding`, `nb_build_base`) |
| `nb_utils_<thema>` | Utils, thematisch gruppiert |
| `nb_test_<thema>` | Unit-Tests zum jeweiligen Thema |
| `nb_config_<layer>_<quelle>` | Config-Notebook je Quelle (Python-Dict) |

Configs liegen unter `notebooks/<layer>/configs/`. Utils werden nach Thema geschnitten, etwa
Delta-Handling, Landing-SharePoint-Zugriff und Logging. Die Einbindung erfolgt per `%run` mit
festem Notebook-Namen.
