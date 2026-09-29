# Gold: um Lakeflow Declarative Pipeline por ambiente, com materialized views calculadas a
# partir da silver do mesmo ambiente. Roda no job de tratamento, logo depois da silver.

locals {
  raiz_gold = "pipelines/gold"
}

resource "databricks_pipeline" "gold" {
  for_each = var.environments

  name        = "gold-clima-energia-${each.key}"
  catalog     = databricks_catalog.main[each.key].name
  schema      = databricks_schema.gold[each.key].name
  serverless  = true
  channel     = "CURRENT"
  development = each.key == "dev"
  root_path   = "${databricks_repo.codigo[each.key].workspace_path}/${local.raiz_gold}"

  library {
    glob {
      include = "${databricks_repo.codigo[each.key].workspace_path}/${local.raiz_gold}/transformations/**"
    }
  }

  configuration = {
    "clima_energia.catalog"      = databricks_catalog.main[each.key].name
    "spark.sql.session.timeZone" = "UTC"
  }

  dynamic "run_as" {
    for_each = each.key == "dev" ? [databricks_service_principal.dev.application_id] : []
    content {
      service_principal_name = run_as.value
    }
  }

  notification {
    email_recipients = [var.email_alertas]
    alerts           = ["on-update-failure", "on-update-fatal-failure", "on-flow-failure"]
  }

  depends_on = [databricks_grants.catalog_dev, databricks_permissions.codigo_dev]
}

# O job de dev roda como o service principal e precisa poder disparar o pipeline.
resource "databricks_permissions" "pipeline_gold_dev" {
  pipeline_id = databricks_pipeline.gold["dev"].id

  access_control {
    service_principal_name = databricks_service_principal.dev.application_id
    permission_level       = "CAN_RUN"
  }
}
