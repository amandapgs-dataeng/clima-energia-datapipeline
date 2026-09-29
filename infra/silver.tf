# Silver: um Lakeflow Declarative Pipeline por ambiente, lendo a bronze única e gravando em
# clima_energia_<ambiente>.silver. Dev roda como o service principal de dev (só lê a bronze).

locals {
  raiz_silver = "pipelines/silver"

  # Tabelas da bronze que alimentam a silver: a atualização de qualquer uma dispara a silver de prod.
  tabelas_bronze_silver = [
    for tabela in [
      "ons.carga_energia",
      "ons.geracao_usina",
      "ons.fator_capacidade",
      "ons.previsao_programado_eolsol",
      "open_meteo.previsao_bruta",
      "open_meteo.historico_observado",
    ] : "${databricks_catalog.bronze.name}.${tabela}"
  ]
}

# Código dos pipelines: uma Git folder por ambiente, na branch do ambiente.
resource "databricks_directory" "codigo" {
  path = "/Shared/clima-energia"
}

resource "databricks_repo" "codigo" {
  for_each = var.environments

  url          = local.repositorio_git
  git_provider = "gitHub"
  branch       = each.value.git_branch
  path         = "${databricks_directory.codigo.path}/${each.key}" # a API devolve sem o prefixo /Workspace
}

resource "databricks_permissions" "codigo_dev" {
  repo_id = databricks_repo.codigo["dev"].id

  access_control {
    service_principal_name = databricks_service_principal.dev.application_id
    permission_level       = "CAN_MANAGE"
  }
}

resource "databricks_pipeline" "silver" {
  for_each = var.environments

  name        = "silver-clima-energia-${each.key}"
  catalog     = databricks_catalog.main[each.key].name
  schema      = databricks_schema.silver[each.key].name
  serverless  = true
  channel     = "CURRENT"
  development = each.key == "dev"
  root_path   = "${databricks_repo.codigo[each.key].workspace_path}/${local.raiz_silver}"

  library {
    glob {
      include = "${databricks_repo.codigo[each.key].workspace_path}/${local.raiz_silver}/transformations/**"
    }
  }

  configuration = {
    "clima_energia.catalog_bronze" = databricks_catalog.bronze.name
    # O ONS grava horário local rotulado como UTC: com a sessão em UTC, datas e horas
    # são lidas exatamente como publicadas.
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

  depends_on = [databricks_grants.bronze, databricks_grants.catalog_dev, databricks_permissions.codigo_dev]
}

# O job de dev roda como o service principal e precisa poder disparar o pipeline.
resource "databricks_permissions" "pipeline_silver_dev" {
  pipeline_id = databricks_pipeline.silver["dev"].id

  access_control {
    service_principal_name = databricks_service_principal.dev.application_id
    permission_level       = "CAN_RUN"
  }
}

resource "databricks_job" "silver" {
  for_each = var.environments

  name                = "silver-clima-energia-${each.key}"
  max_concurrent_runs = 1

  git_source {
    url      = local.repositorio_git
    provider = "gitHub"
    branch   = each.value.git_branch
  }

  dynamic "run_as" {
    for_each = each.key == "dev" ? [databricks_service_principal.dev.application_id] : []
    content {
      service_principal_name = run_as.value
    }
  }

  # Prod roda sozinho quando a bronze é atualizada; dev roda sob demanda.
  dynamic "trigger" {
    for_each = each.value.schedule_paused ? [] : [1]
    content {
      # Pausado durante o reprocessamento de 29/09/2026: a bronze recebe o formato novo do clima
      # (vários pontos) antes da silver que o entende. Reativado junto com a silver nova.
      pause_status = "PAUSED"
      table_update {
        table_names                    = local.tabelas_bronze_silver
        condition                      = "ANY_UPDATED" # qualquer tabela da bronze atualizada dispara a silver
        wait_after_last_change_seconds = 300
      }
    }
  }

  # Tasks em ordem alfabética de task_key (a ordem em que a API as devolve).
  task {
    task_key = "silver"
    depends_on {
      task_key = "sincronizar_codigo"
    }
    pipeline_task {
      pipeline_id = databricks_pipeline.silver[each.key].id
    }
  }

  # Sem cluster declarado: roda em compute serverless (só atualiza a Git folder).
  task {
    task_key = "sincronizar_codigo"
    notebook_task {
      notebook_path = "notebooks/sincronizar_git_folder.py"
      source        = "GIT"
      base_parameters = {
        repo_id = databricks_repo.codigo[each.key].id
        branch  = each.value.git_branch
      }
    }
  }

  email_notifications {
    on_failure = [var.email_alertas]
  }
}
