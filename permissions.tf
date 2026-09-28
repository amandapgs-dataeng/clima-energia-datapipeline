# Identidade dos jobs de silver/gold de dev.
# Os demais jobs rodam como a dona do workspace, que é dona de todos os objetos, e um
# GRANT não restringe o dono. Uma identidade própria é o que garante, de fato, que dev
# só lê a bronze e só escreve no próprio catalog.
resource "databricks_service_principal" "dev" {
  display_name = "sp-clima-energia-dev"
}

resource "databricks_grants" "bronze" {
  catalog = databricks_catalog.bronze.name

  grant {
    principal  = databricks_service_principal.dev.application_id
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
  }
}

resource "databricks_grants" "catalog_dev" {
  catalog = databricks_catalog.main["dev"].name

  grant {
    principal  = databricks_service_principal.dev.application_id
    privileges = ["ALL_PRIVILEGES"]
  }
}
