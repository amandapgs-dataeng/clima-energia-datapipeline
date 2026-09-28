# Jobs de ingestão: rodam uma vez só (não por ambiente), com o código da main,
# e gravam na bronze única (clima_energia_bronze). Silver e gold de cada ambiente
# terão seus próprios jobs.
locals {
  ingestao_git_branch = "main"
}

resource "databricks_job" "pipeline_diario" {
  name = "ingestao-diaria-clima-energia"

  git_source {
    url      = "https://github.com/amandapgs-dataeng/clima-energia-datapipeline"
    provider = "gitHub"
    branch   = local.ingestao_git_branch
  }

  schedule {
    quartz_cron_expression = "0 0 21 * * ?"
    timezone_id            = "America/Fortaleza"
  }

  task {
    task_key            = "forecast_clima"
    existing_cluster_id = databricks_cluster.main.id
    notebook_task {
      notebook_path = "notebooks/01_forecast_clima.py"
      source        = "GIT"
      base_parameters = {
        catalog = databricks_catalog.bronze.name
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
        catalog = databricks_catalog.bronze.name
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
  name = "ingestao-semanal-clima-energia"

  git_source {
    url      = "https://github.com/amandapgs-dataeng/clima-energia-datapipeline"
    provider = "gitHub"
    branch   = local.ingestao_git_branch
  }

  schedule {
    quartz_cron_expression = "0 0 21 ? * SUN"
    timezone_id            = "America/Fortaleza"
  }

  task {
    task_key            = "carga_energia"
    existing_cluster_id = databricks_cluster.main.id
    notebook_task {
      notebook_path = "notebooks/05_carga_energia.py"
      source        = "GIT"
      base_parameters = {
        catalog = databricks_catalog.bronze.name
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
        catalog = databricks_catalog.bronze.name
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
  name = "ingestao-mensal-clima-energia"

  git_source {
    url      = "https://github.com/amandapgs-dataeng/clima-energia-datapipeline"
    provider = "gitHub"
    branch   = local.ingestao_git_branch
  }

  schedule {
    quartz_cron_expression = "0 0 21 2 * ?"
    timezone_id            = "America/Fortaleza"
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
        catalog = databricks_catalog.bronze.name
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
        catalog = databricks_catalog.bronze.name
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
  from = databricks_job.pipeline_diario["prod"]
  to   = databricks_job.pipeline_diario
}

moved {
  from = databricks_job.pipeline_semanal["prod"]
  to   = databricks_job.pipeline_semanal
}

moved {
  from = databricks_job.pipeline_mensal["prod"]
  to   = databricks_job.pipeline_mensal
}
