# Jobs de ingestão: rodam uma vez só (não por ambiente), com o código da main, e gravam
# na bronze única (clima_energia_bronze). Silver e gold terão jobs próprios por ambiente.
locals {
  repositorio_git     = "https://github.com/amandapgs-dataeng/clima-energia-datapipeline"
  ingestao_git_branch = "main"

  # Tasks de cada job: notebook e dependência. O Terraform percorre mapas em ordem
  # alfabética de chave, a mesma ordem em que a API do Databricks devolve as tasks
  # (qualquer outra ordem gera diff perpétuo no plan).
  jobs_ingestao = {
    diaria = {
      cron = "0 0 21 * * ?"
      tasks = {
        forecast_clima      = { notebook = "01_forecast_clima", depende_de = null }
        previsao_programado = { notebook = "04_previsao_programado", depende_de = "forecast_clima" }
      }
    }
    semanal = {
      cron = "0 0 21 ? * SUN"
      tasks = {
        carga_energia    = { notebook = "05_carga_energia", depende_de = null }
        historical_clima = { notebook = "06_historical_clima", depende_de = "carga_energia" }
      }
    }
    mensal = {
      cron = "0 0 21 2 * ?"
      tasks = {
        fator_capacidade = { notebook = "03_fator_capacidade", depende_de = "geracao_usina" }
        geracao_usina    = { notebook = "02_geracao_usina", depende_de = null }
      }
    }
  }
}

resource "databricks_job" "ingestao" {
  for_each = local.jobs_ingestao

  name                = "ingestao-${each.key}-clima-energia"
  max_concurrent_runs = 1

  git_source {
    url      = local.repositorio_git
    provider = "gitHub"
    branch   = local.ingestao_git_branch
  }

  schedule {
    quartz_cron_expression = each.value.cron
    timezone_id            = "America/Fortaleza"
  }

  # Cluster de job: criado para a execução e desligado ao fim. Custa menos que o
  # cluster interativo e isola cada execução.
  job_cluster {
    job_cluster_key = local.job_cluster_key

    new_cluster {
      spark_version      = data.databricks_spark_version.latest_lts.id
      node_type_id       = local.cluster_node_type
      num_workers        = 0
      data_security_mode = "SINGLE_USER"
      single_user_name   = data.databricks_current_user.me.user_name
      spark_conf         = local.cluster_spark_conf_single_node
      custom_tags        = local.cluster_tags_single_node
    }
  }

  dynamic "task" {
    for_each = each.value.tasks

    content {
      task_key                  = task.key
      job_cluster_key           = local.job_cluster_key
      max_retries               = 2
      min_retry_interval_millis = 300000

      dynamic "depends_on" {
        for_each = task.value.depende_de == null ? [] : [task.value.depende_de]
        content {
          task_key = depends_on.value
        }
      }

      notebook_task {
        notebook_path = "notebooks/${task.value.notebook}.py"
        source        = "GIT"
        base_parameters = {
          catalog = databricks_catalog.bronze.name
        }
      }
    }
  }

  email_notifications {
    on_failure = [var.email_alertas]
  }
}

moved {
  from = databricks_job.pipeline_diario
  to   = databricks_job.ingestao["diaria"]
}

moved {
  from = databricks_job.pipeline_semanal
  to   = databricks_job.ingestao["semanal"]
}

moved {
  from = databricks_job.pipeline_mensal
  to   = databricks_job.ingestao["mensal"]
}
