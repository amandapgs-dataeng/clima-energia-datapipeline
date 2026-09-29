# Reprocessamento (backfill): reaproveita os notebooks da ingestão para carregar períodos
# passados na bronze. Sem agendamento: roda sob demanda, com os períodos como parâmetros.
#
#   meses_ons         lista JSON de datas de referência; cada uma carrega o mês anterior
#                     (geração e fator de capacidade), ex.: ["2024-11-01", "2024-12-01"]
#   dias_programacao  lista JSON de dias da programação do ONS, ex.: ["2024-10-01", ...]
#   janela_carga_dias janela da carga de energia, em dias (a carga vem em arquivos anuais)
#   data_inicio/fim   período do clima observado e das previsões D+1 históricas
locals {
  tarefas_reprocessamento = {
    # tarefa = { notebook, parâmetros, lista do for_each (null = execução única), paralelismo }
    carga_energia = {
      notebook   = "05_carga_energia"
      parametros = { janela_dias = "{{job.parameters.janela_carga_dias}}" }
      entradas   = null
    }
    clima_observado = {
      notebook   = "06_historical_clima"
      parametros = { data_inicio = "{{job.parameters.data_inicio}}", data_fim = "{{job.parameters.data_fim}}" }
      entradas   = null
    }
    clima_previsao_historica = {
      notebook   = "07_previsao_clima_historica"
      parametros = { data_inicio = "{{job.parameters.data_inicio}}", data_fim = "{{job.parameters.data_fim}}" }
      entradas   = null
    }
    fator_capacidade = {
      notebook    = "03_fator_capacidade"
      parametros  = { data_referencia = "{{input}}" }
      entradas    = "{{job.parameters.meses_ons}}"
      paralelismo = 2
    }
    geracao_usina = {
      notebook    = "02_geracao_usina"
      parametros  = { data_referencia = "{{input}}" }
      entradas    = "{{job.parameters.meses_ons}}"
      paralelismo = 2
    }
    programacao = {
      notebook    = "04_previsao_programado"
      parametros  = { data_referencia = "{{input}}" }
      entradas    = "{{job.parameters.dias_programacao}}"
      paralelismo = 4
    }
  }

  # Um cluster de um nó só: as tarefas rodam uma depois da outra, nesta ordem.
  ordem_reprocessamento = [
    "carga_energia", "clima_observado", "clima_previsao_historica", "geracao_usina", "fator_capacidade", "programacao",
  ]
}

resource "databricks_job" "reprocessamento" {
  name                = "reprocessamento-clima-energia"
  max_concurrent_runs = 1

  git_source {
    url      = local.repositorio_git
    provider = "gitHub"
    branch   = local.ingestao_git_branch
  }

  parameter {
    name    = "meses_ons"
    default = "[]"
  }
  parameter {
    name    = "dias_programacao"
    default = "[]"
  }
  parameter {
    name    = "janela_carga_dias"
    default = "60"
  }
  parameter {
    name    = "data_inicio"
    default = ""
  }
  parameter {
    name    = "data_fim"
    default = ""
  }

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

  # Tasks em ordem alfabética de task_key (a ordem em que a API as devolve).
  dynamic "task" {
    for_each = local.tarefas_reprocessamento

    content {
      task_key = task.key

      dynamic "depends_on" {
        for_each = index(local.ordem_reprocessamento, task.key) == 0 ? [] : [local.ordem_reprocessamento[index(local.ordem_reprocessamento, task.key) - 1]]
        content {
          task_key = depends_on.value
        }
      }

      # Execução única
      job_cluster_key = task.value.entradas == null ? local.job_cluster_key : null

      dynamic "notebook_task" {
        for_each = task.value.entradas == null ? [1] : []
        content {
          notebook_path   = "notebooks/${task.value.notebook}.py"
          source          = "GIT"
          base_parameters = merge({ catalog = databricks_catalog.bronze.name }, task.value.parametros)
        }
      }

      # Uma execução por item da lista, em paralelo no mesmo cluster
      dynamic "for_each_task" {
        for_each = task.value.entradas == null ? [] : [1]
        content {
          inputs      = task.value.entradas
          concurrency = task.value.paralelismo

          task {
            task_key        = "${task.key}_item"
            job_cluster_key = local.job_cluster_key
            max_retries     = 2

            notebook_task {
              notebook_path   = "notebooks/${task.value.notebook}.py"
              source          = "GIT"
              base_parameters = merge({ catalog = databricks_catalog.bronze.name }, task.value.parametros)
            }
          }
        }
      }
    }
  }

  email_notifications {
    on_failure = [var.email_alertas]
  }
}
