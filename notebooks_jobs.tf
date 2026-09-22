resource "databricks_job" "pipeline_diario" {
  name = "pipeline-diario-clima-energia"

  git_source {
    url      = "https://github.com/amandapgs-dataeng/clima-energia-datapipeline"
    provider = "gitHub"
    branch   = "develop"
  }

  schedule {
    quartz_cron_expression = "0 0 21 * * ?"
    timezone_id            = "America/Fortaleza"
  }

  task {
    task_key             = "forecast_clima"
    existing_cluster_id  = databricks_cluster.main.id
    notebook_task {
      notebook_path = "notebooks/01_forecast_clima.py"
      source        = "GIT"
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
  name = "pipeline-semanal-clima-energia"

  git_source {
    url      = "https://github.com/amandapgs-dataeng/clima-energia-datapipeline"
    provider = "gitHub"
    branch   = "develop"
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
  name = "pipeline-mensal-clima-energia"

  git_source {
    url      = "https://github.com/amandapgs-dataeng/clima-energia-datapipeline"
    provider = "gitHub"
    branch   = "develop"
  }

  schedule {
    quartz_cron_expression = "0 0 21 2 * ?"
    timezone_id            = "America/Fortaleza"
  }

  task {
    task_key             = "geracao_usina"
    existing_cluster_id  = databricks_cluster.main.id
    notebook_task {
      notebook_path = "notebooks/02_geracao_usina.py"
      source        = "GIT"
    }
    max_retries               = 2
    min_retry_interval_millis = 300000
  }

  task {
    task_key = "fator_capacidade"
    depends_on {
      task_key = "geracao_usina"
    }
    existing_cluster_id = databricks_cluster.main.id
    notebook_task {
      notebook_path = "notebooks/03_fator_capacidade.py"
      source        = "GIT"
    }
    max_retries               = 2
    min_retry_interval_millis = 300000
  }

  email_notifications {
    on_failure = ["amandapgs@hotmail.com"]
  }

  max_concurrent_runs = 1
}