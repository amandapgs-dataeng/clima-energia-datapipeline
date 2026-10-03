# Dashboards (Databricks AI/BI), publicados a partir dos templates gerados por
# dashboards/construir.py. Leem o ambiente de prod.

data "databricks_sql_warehouse" "dashboards" {
  name = "Serverless Starter Warehouse"
}

resource "databricks_directory" "dashboards" {
  path = "${databricks_directory.codigo.path}/dashboards"
}

locals {
  # Os logs de eventos dos pipelines são tabelas com o ID do pipeline no nome.
  valores_dashboards = {
    catalog          = databricks_catalog.main["prod"].name
    catalog_bronze   = databricks_catalog.bronze.name
    event_log_silver = "event_log_${replace(databricks_pipeline.silver["prod"].id, "-", "_")}"
    event_log_gold   = "event_log_${replace(databricks_pipeline.gold["prod"].id, "-", "_")}"
  }

  dashboards = {
    negocio       = "Clima × Energia Renovável — Nordeste"
    monitoramento = "Monitoramento do pipeline clima-energia"
  }
}

resource "databricks_dashboard" "painel" {
  for_each = local.dashboards

  display_name         = each.value
  warehouse_id         = data.databricks_sql_warehouse.dashboards.id
  parent_path          = databricks_directory.dashboards.path
  serialized_dashboard = templatefile("${path.module}/../dashboards/${each.key}.lvdash.json.tftpl", local.valores_dashboards)
  embed_credentials    = true
}
