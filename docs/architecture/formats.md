# Formats

Die Plattform basiert vollständig auf **Delta**. Delta ist das Format der Tabellen in
`histlanding`, `base` und `mart_*` und liefert über **Time Travel** die Historie direkt aus dem
Tabellenverlauf.

In der Landing-Zone liegen die Rohdaten im Quellformat, also **Excel (XLSX) und CSV**, wie sie
über den SharePoint-Shortcut eintreffen. Sie werden erst beim Übergang nach `histlanding` in
Delta überführt.

Bei Anbindung der direkten Quellsysteme (beispielsweise theasoft oder Eventim) werden die
Quelldaten in die Files-Section des Lakehouse geschrieben und von dort wie die übrigen
Landing-Dateien verarbeitet.

Standardformate (Parquet, JSON) werden config-getrieben nach `histlanding` überführt; Excel läuft
über ein Custom-Notebook auf einer Single-Node-Engine (siehe [Storage Layers](storage-layers.md)).
