# AGENTS.md – Kronberg Academy DWH (Microsoft Fabric)

Gesamtbild und Begründungen: Notion „Konzeption Datenarchitektur“. Diese Datei enthält nur Regeln für den Agent.

## 0. Non-negotiables

Vorrang vor allem anderen in dieser Datei.

- Nie gegen `ws_dwh_prod` ausführen, deployen oder auf `main` mergen.
- `histlanding` ist die einzige Historie (Delta Time Travel). Tabellen im Schema `histlanding` nie droppen, neu anlegen, mit `overwrite`/`overwriteSchema` schreiben, truncaten oder mit verkürzter Retention vacuumen. Full-Rebuild nur nach Rückfrage.
- Fail early: Jeder Fehler ist eine Exception und bricht den Lauf ab. Kein try/except, das Fehler schluckt, kein Teilergebnis, kein `notebookutils.notebook.exit()` mit Fehlertext.
- Nichts erfinden: Tabellen, Spalten, Sachkonten, Kostenträger, Fabric-/notebookutils-APIs, Spark-Configs, IDs, Config-Keys, Variablen der Variable Library. Unbekannt → Datei lesen oder fragen.
- Falsche Prämisse → widersprechen, bevor gearbeitet wird. Zwei plausible Interpretationen → fragen, nicht still entscheiden.
- Nur ändern, was die Aufgabe verlangt. Keine Drive-by-Refactorings. Selbst erzeugte Waisen (Imports, Funktionen) aufräumen.
- Keine Floskeln. Antworten knapp und auf Deutsch, Prosa vor Bullets. Code-Doku auf Englisch.

## 1. Commands

- Tests: TODO (Ausführung der `nb_test_*`-Notebooks in Fabric)
- Deploy: ausschließlich GitHub Actions (fabric-cicd). Nie manuell.

## 2. Domäne (nur was Code beeinflusst)

Phase 1: Veranstaltungscontrolling (Plan-Soll-Ist) und monatliche BWA. Quellen kurzfristig Excel/CSV via SharePoint, langfristig Quellsysteme.

- DATEV: Ist-Kosten/-Erlöse, BWA. Monatlich, Einzelposten (Sachkonto, Kostenstelle, Kostenträger).
- theasoft / thea.order: Plan, Soll (überschreibt Plan im Quellsystem), Obligos. Keine Historie im Quellsystem, Dubletten, uneinheitliche Datumsformate.
- Eventim: 2–3 CSV, wöchentlich. Ticketpreise ≠ DATEV-Erlöse (Gebühren, Provisionen) – nie direkt gleichsetzen.
- Mappings (Sachkonto ↔ Kostenkategorie / BWA-Zeile / Kostenträger / Veranstaltung): fachliche Hoheit bei Kronberg Academy. Agent ändert keine Mapping-Logik.
- Zentraler Schlüssel: Kostenträger = DATEV `COST2` = Eventim „Projekt-Infocode“.

## 3. Architektur (bindend)

Ein Lakehouse je Umgebung (`lh_dwh_dev` / `lh_dwh_prod`), Layer als Schemas. Tabellenname = Entität in snake_case, ohne Layer-Präfix.

| Layer | Ort | Technik |
|---|---|---|
| landing | Files-Section | OneLake-Shortcut → SharePoint, keine Logik |
| histlanding | Schema `histlanding` | PySpark; Excel via pandas, alle Spalten als String |
| base | Schema `base` | PySpark: Renaming, Casting, Dedup, Formate |
| serving_<fachbereich> | eigenes Schema je Fachbereich | Materialized Lake Views (Spark SQL) |

- Serving-Bereitstellung per Shortcut in `lh_serving_*` (Workspaces `ws_serving_*`), keine Kopie. Rechte über OneLake Security.
- Nur Lakehouse. Kein Warehouse, keine Dataflows Gen2, keine neuen Item-Typen ohne Rückfrage.
- Workspaces: `ws_feature_*` (Feature-Branch) → `ws_dwh_dev` (Test) → `ws_dwh_prod`.
- Umgebungsabhängige Werte (inkl. abfs-Pfade) ausschließlich aus der Variable Library; fabric-cicd setzt sie.
- Notebook-IO (Tabellen und Landing-Dateien) ausschließlich über abfs-Pfade aus der Variable Library. Kein Default-Lakehouse, keine relativen Pfade, kein `spark.table`/`saveAsTable`. Ausnahme: MLVs.
- Orchestrierung: Data Pipelines, Master + Subpipelines, Parameter `pRunHistlanding`, `pRunBase`, `pSourcesToRun`. MLV-Refresh per Pipeline-Aktivität. Alerting (MS Teams) liegt in der Pipeline; Notebooks werfen nur Exceptions.
- Kadenz steuert ausschließlich `pSourcesToRun` (Pipeline-Schedules). Kein Notebook und keine Config entscheidet nach Datum oder Frequenz.
- Capacity F2: keine unnötigen Full-Rebuilds, kein `cache`/`repartition` ohne Begründung, Spark-Settings nur über Environments.

## 4. Build-Ablauf (config-getrieben)

Einstieg `notebooks/histlanding/nb_build_histlanding` und `notebooks/base/nb_build_base`. Dünn, keine Build-Logik, fester Ablauf:

1. Config-Notebooks per `%run` laden (nicht `notebook.run` – isolierte Session).
2. Zentrale Config nach `pSourcesToRun` filtern. Unbekannte Quelle → Exception.
3. abfs-Pfade einmal aus der Variable Library auflösen und als Parameter weitergeben. Utils lesen die Variable Library nie selbst.
4. Preflight für alle gewählten Quellen, vor dem ersten Schreibzugriff: Config-Struktur, Dateiverfügbarkeit, Schema-Check, aufgelöster Pfad passt zur laufenden Umgebung.
5. `process_histlanding` bzw. `process_base` aus den Utils aufrufen.

- Verhalten pro Quelle und Entität nur über Config. Neue Entität = neuer Config-Eintrag. Erfordert sie Codeänderung → erst fragen.
- Sonderfälle: Custom Function in den Utils, von der Config referenziert. Keine entitätsspezifischen Notebooks.
- Configs: ein Notebook pro Quelle (Python-Dict) in `notebooks/<layer>/configs/`. Config-Vertrag ist offen → bestehende Configs als Vorlage lesen, sonst fragen.
- Utils und Tests: `notebooks/<layer>/utils/` (z. B. `nb_utils_general_histlanding`, `nb_test_general_histlanding`).
- Naming: `nb_build_<layer>`, `nb_utils_<thema>`, `nb_test_<thema>`. Tabellen und Spalten snake_case.

## 5. Datenregeln

- histlanding-Schema-Check ist Pflicht: Spaltenstruktur und Datentypen gegen das Soll-Schema aus der Config (Excel-Quellen: alle Spalten String, da ihr Schema variabel ist). Abweichung → Exception. Nie still anpassen.
- Casting erst in base.
- Business Keys ausschließlich aus der Config.
- Idempotenz: MERGE über Business Key + Change-Detection, kein blindes append. Leeres Change-Set → kein Write, damit keine neue Tabellenversion entsteht.
- Doppelte Business Keys im Quell-Batch und in der Quelle gelöschte Zeilen: Verhalten offen (§9). Nicht selbst lösen, fragen.
- Secrets nur aus Azure Key Vault. Keine hartkodierten Secrets, Workspace- oder Lakehouse-IDs.
- `.platform`-Dateien, logicalIds und Notebook-Metadaten (Lakehouse-Bindung) nie manuell ändern.

## 6. Code

- Kleine Funktionen mit genau einer Aufgabe. Vor jedem neuen Helper bestehende Utils durchsuchen, kein Duplikat.
- Transformationen sind reine Funktionen (DataFrame/Parameter rein, DataFrame raus). IO in dünner, separater Schicht. Config und Pfade als Parameter.
- Im Zweifel die einfachere, besser testbare Variante.
- Docstring auf Englisch: Zweck, Input, Output. Keine Kommentare, die den Code nacherzählen.
- Logging nur über die zentrale Funktion aus `logging_utils` (Name: TODO). Kein `print()`.
- Utils-Einbindung ist offen (§9): die im Repo vorhandene Variante übernehmen, nie mischen.

## 7. Arbeitsweise und Verifikation

- Vor dem Code: Plan in 1–2 Sätzen, bei Nicht-Trivialem nummerierte Schritte mit je einem Check. Betroffene Dateien und Aufrufer lesen (Pipelines und deren Parameter, abhängige MLVs, Configs). Annahmen nennen, bei zwei Ansätzen beide mit Trade-offs.
- Erfolgskriterium vor dem Code formulieren.
- Unit-Tests mit `unittest` in `nb_test_<thema>`, kein pytest. Kleine In-Memory-DataFrames, nie Quell- oder Prod-Daten. Pro neuer oder geänderter Util-Funktion mindestens ein Test inkl. Randfall.
- Datenänderungen prüfen: Zeilenzahl vorher/nachher, Schema-Diff, Duplikatcheck auf Business Key, zweiter Lauf erzeugt keine neue Tabellenversion.
- Nie „fertig“ ohne gelesene Ausführungsausgabe. Nicht ausführbar → sagen, was ungeprüft ist.
- Fehler an der Ursache beheben, nicht den Test anpassen.
- Nach zwei gescheiterten Korrekturen am selben Problem: stoppen, Erkenntnisse zusammenfassen, Reset vorschlagen.
- Git: Feature-Branches, Review vor Merge. Commit-Subject < 72 Zeichen, Body erklärt das Warum. Kein „Co-Authored-By: Claude“.

## 8. Fragen statt handeln

- Historisierung, histlanding-Schemata, Retention, Business Keys.
- Mapping-, Vorzeichen- oder Periodenlogik.
- Neue Quelle, neues Fabric-Item, neue Library im Environment.
- RBAC, OneLake Security, Shortcuts, Pipeline-Parameter, Schedules, Variable Library, Deployment-Konfiguration.
- Daten mit Personenbezug, insbesondere Personalkosten.
- Alles aus §9.

## 9. Offene Entscheidungen (nie durch den Agent)

- Historie: Retention-Werte und Wartungsregel (VACUUM / Table Maintenance) für histlanding · Dubletten mit gleichem Business Key · Umgang mit in der Quelle gelöschten Zeilen · Evolutionspfad bei Schemaänderung der Quelle
- Build: Utils-Einbindung (`%run` vs. `notebook.run`) · Config-Vertrag · Variable Library (Name, Variablen, Value Sets inkl. `ws_feature_*`) · Shortcut-Transformation vs. pandas-Ingestion
- Serving: Datenmodell · SCD2 · Namensschema `serving_<fachbereich>` / `lh_serving_<fachbereich>` · MLV-Refresh nur bei neuen Daten · Lakehouse-Bindung der MLV-Notebooks beim Deployment · Katalog-Sichtbarkeit pfadgeschriebener Tabellen für MLVs
- Governance: RBAC-Konzept (inkl. Rechte über Shortcuts) · Personalkosten im DWH

## 10. Project Learnings

Nach jedem Agent-Fehler: fehlende Regel → hier konkret ergänzen. Ignorierte Regel → schärfen oder nach oben ziehen. Regelmäßig kürzen.
