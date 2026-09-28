# Databricks notebook source
# Atualiza a Git folder de um ambiente para o último commit da branch.
# Os pipelines leem o código de uma Git folder do workspace, que não se atualiza sozinha:
# este notebook roda como primeira task do job, antes do pipeline.

# COMMAND ----------
from databricks.sdk import WorkspaceClient

from utils.logging_helpers import obter_logger

dbutils.widgets.text("repo_id", "")
dbutils.widgets.text("branch", "")

repo_id = int(dbutils.widgets.get("repo_id"))
branch = dbutils.widgets.get("branch")
logger = obter_logger("sincronizar_git_folder")

# COMMAND ----------
workspace = WorkspaceClient()
antes = workspace.repos.get(repo_id).head_commit_id
workspace.repos.update(repo_id=repo_id, branch=branch)
depois = workspace.repos.get(repo_id).head_commit_id

logger.info("Git folder %s | branch %s | %s -> %s", repo_id, branch, antes[:7], depois[:7])
