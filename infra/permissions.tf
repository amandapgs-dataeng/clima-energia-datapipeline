# Identidade dos jobs de silver/gold de dev.
# Os demais jobs rodam como a dona do workspace, que é dona de todos os objetos, e um
# GRANT não restringe o dono. Uma identidade própria é o que garante, de fato, que dev
# só lê a bronze e só escreve no próprio catalog.
resource "databricks_service_principal" "dev" {
  display_name     = "sp-clima-energia-dev"
  workspace_access = true # necessário para ler o código na Git folder e rodar jobs
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

  # As tabelas de dev pertencem ao service principal (quem as cria); ser dona do catalog
  # não dá leitura sobre elas. A desenvolvedora lê dev para validar as mudanças.
  grant {
    principal  = data.databricks_current_user.me.user_name
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
  }
}
