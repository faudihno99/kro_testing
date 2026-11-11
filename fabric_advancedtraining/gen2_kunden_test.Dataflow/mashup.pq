[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "adventure_works adventureworks_kunden_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"adventure_works adventureworks_kunden" = let
  Source = PostgreSQL.Database("pg-enablement-server.postgres.database.azure.com", "postgres"),
  #"Navigation 1" = Source{[Schema = "adventure_works", Item = "adventureworks_kunden"]}[Data]
in
  #"Navigation 1";
shared #"adventure_works adventureworks_kunden_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "98c91999-02e5-4d24-8f5f-5b95d409608e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1df0f3cd-75d3-4520-805a-c4cb67fbe687"]}[Data],
  TableNavigation = Navigation_2{[Id = "adventure_works adventureworks_kunden", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
