resource "databricks_job" "pipeline_diario" {
  for_each = var.environments

  name = "pipeline-diario-clima-energia-${each.key}"

  git_source {
    url      = "https://github.com/amandapgs-dataeng/clima-energia-datapipeline"
    provider = "gitHub"
    branch   = each.value.git_branch
  }

  schedule {
    quartz_cron_expression = "0 0 21 * * ?"
    timezone_id            = "America/Fortaleza"
    pause_status           = each.value.schedule_paused ? "PAUSED" : "UNPAUSED"
  }

  task {
    task_key            = "forecast_clima"
    existing_cluster_id = databricks_cluster.main.id
    notebook_task {
      notebook_path = "notebooks/01_forecast_clima.py"
      source        = "GIT"
      base_parameters = {
        catalog = databricks_catalog.main[each.key].name
      }
    }
    max_retries               = 2
    min_retry_interval_millis = 300000
  }

  task {
    task_key = "previsao_programado"
    depends_on {
      task_key = "forecast_clima"
    }
    existing_cluster_id = databricks_cluster.main.id
    notebook_task {
      notebook_path = "notebooks/04_previsao_programado.py"
      source        = "GIT"
      base_parameters = {
        catalog = databricks_catalog.main[each.key].name
      }
    }
    max_retries               = 2
    min_retry_interval_millis = 300000
  }

  email_notifications {
    on_failure = ["amandapgs@hotmail.com"]
  }

  max_concurrent_runs = 1
}

resource "databricks_job" "pipeline_semanal" {
  for_each = var.environments

  name = "pipeline-semanal-clima-energia-${each.key}"

  git_source {
    url      = "https://github.com/amandapgs-dataeng/clima-energia-datapipeline"
    provider = "gitHub"
    branch   = each.value.git_branch
  }

  schedule {
    quartz_cron_expression = "0 0 21 ? * SUN"
    timezone_id            = "America/Fortaleza"
    pause_status           = each.value.schedule_paused ? "PAUSED" : "UNPAUSED"
  }

  task {
    task_key            = "carga_energia"
    existing_cluster_id = databricks_cluster.main.id
    notebook_task {
      notebook_path = "notebooks/05_carga_energia.py"
      source        = "GIT"
      base_parameters = {
        catalog = databricks_catalog.main[each.key].name
      }
    }
    max_retries               = 2
    min_retry_interval_millis = 300000
  }

  task {
    task_key = "historical_clima"
    depends_on {
      task_key = "carga_energia"
    }
    existing_cluster_id = databricks_cluster.main.id
    notebook_task {
      notebook_path = "notebooks/06_historical_clima.py"
      source        = "GIT"
      base_parameters = {
        catalog = databricks_catalog.main[each.key].name
      }
    }
    max_retries               = 2
    min_retry_interval_millis = 300000
  }

  email_notifications {
    on_failure = ["amandapgs@hotmail.com"]
  }

  max_concurrent_runs = 1
}

resource "databricks_job" "pipeline_mensal" {
  for_each = var.environments

  name = "pipeline-mensal-clima-energia-${each.key}"

  git_source {
    url      = "https://github.com/amandapgs-dataeng/clima-energia-datapipeline"
    provider = "gitHub"
    branch   = each.value.git_branch
  }

  schedule {
    quartz_cron_expression = "0 0 21 2 * ?"
    timezone_id            = "America/Fortaleza"
    pause_status           = each.value.schedule_paused ? "PAUSED" : "UNPAUSED"
  }

  # Tasks em ordem alfabética de task_key: a API do Databricks devolve assim,
  # e outra ordem gera diff perpétuo no plan.
  task {
    task_key = "fator_capacidade"
    depends_on {
      task_key = "geracao_usina"
    }
    existing_cluster_id = databricks_cluster.main.id
    notebook_task {
      notebook_path = "notebooks/03_fator_capacidade.py"
      source        = "GIT"
      base_parameters = {
        catalog = databricks_catalog.main[each.key].name
      }
    }
    max_retries               = 2
    min_retry_interval_millis = 300000
  }

  task {
    task_key            = "geracao_usina"
    existing_cluster_id = databricks_cluster.main.id
    notebook_task {
      notebook_path = "notebooks/02_geracao_usina.py"
      source        = "GIT"
      base_parameters = {
        catalog = databricks_catalog.main[each.key].name
      }
    }
    max_retries               = 2
    min_retry_interval_millis = 300000
  }

  email_notifications {
    on_failure = ["amandapgs@hotmail.com"]
  }

  max_concurrent_runs = 1
}

moved {
  from = databricks_job.pipeline_diario
  to   = databricks_job.pipeline_diario["dev"]
}

moved {
  from = databricks_job.pipeline_semanal
  to   = databricks_job.pipeline_semanal["dev"]
}

moved {
  from = databricks_job.pipeline_mensal
  to   = databricks_job.pipeline_mensal["dev"]
}
