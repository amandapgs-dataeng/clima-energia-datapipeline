resource "databricks_storage_credential" "main" {
  name = "cred-storage-clima-energia"

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.main.id
  }

  comment = "Credencial de acesso ao Data Lake via Managed Identity"

  depends_on = [azurerm_role_assignment.access_connector_storage]
}

resource "databricks_external_location" "bronze" {
  name            = "ext-loc-bronze"
  url             = "abfss://bronze@${azurerm_storage_account.main.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.main.id
  comment         = "External location para o container bronze"
}

resource "databricks_external_location" "silver" {
  name            = "ext-loc-silver"
  url             = "abfss://silver@${azurerm_storage_account.main.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.main.id
  comment         = "External location para o container silver"
}

resource "databricks_external_location" "gold" {
  name            = "ext-loc-gold"
  url             = "abfss://gold@${azurerm_storage_account.main.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.main.id
  comment         = "External location para o container gold"
}

resource "databricks_catalog" "main" {
  name         = "clima_energia"
  comment      = "Catalog do projeto Clima + Energia"
  storage_root = databricks_external_location.bronze.url

  depends_on = [databricks_external_location.bronze]
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.main.name
  name         = "bronze"
  storage_root = databricks_external_location.bronze.url
  comment      = "Camada bronze - dados brutos"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.main.name
  name         = "silver"
  storage_root = databricks_external_location.silver.url
  comment      = "Camada silver - dados tratados"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.main.name
  name         = "gold"
  storage_root = databricks_external_location.gold.url
  comment      = "Camada gold - dados agregados"
}