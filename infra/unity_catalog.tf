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
  for_each = var.environments

  name         = "clima_energia_${each.key}"
  comment      = "Catalog do projeto Clima + Energia (${each.key})"
  storage_root = databricks_external_location.bronze.url

  depends_on = [databricks_external_location.bronze]
}

# Bronze única, compartilhada por dev e prod: cópia fiel das fontes, sem regra de negócio.
# Só os jobs de ingestão escrevem aqui; silver e gold de cada ambiente apenas leem.
resource "databricks_catalog" "bronze" {
  name         = "clima_energia_bronze"
  comment      = "Camada bronze única (dados brutos das fontes), compartilhada entre ambientes"
  storage_root = databricks_external_location.bronze.url

  depends_on = [databricks_external_location.bronze]
}

locals {
  schemas_bronze = {
    ons        = "Dados brutos do ONS (arquivos parquet de dados abertos)"
    open_meteo = "Respostas brutas da API do Open-Meteo"
    controle   = "Controle da ingestão (log de auditoria)"
  }
}

resource "databricks_schema" "bronze_compartilhada" {
  for_each = local.schemas_bronze

  catalog_name = databricks_catalog.bronze.name
  name         = each.key
  storage_root = "${databricks_external_location.bronze.url}${each.key}/"
  comment      = each.value
}

resource "databricks_schema" "silver" {
  for_each = var.environments

  catalog_name = databricks_catalog.main[each.key].name
  name         = "silver"
  storage_root = "${databricks_external_location.silver.url}${each.key}/"
  comment      = "Camada silver - dados tratados"
}

resource "databricks_schema" "gold" {
  for_each = var.environments

  catalog_name = databricks_catalog.main[each.key].name
  name         = "gold"
  storage_root = "${databricks_external_location.gold.url}${each.key}/"
  comment      = "Camada gold - dados agregados"
}