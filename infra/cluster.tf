data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

data "databricks_current_user" "me" {}

locals {
  cluster_node_type = "Standard_DC4as_v5"
  job_cluster_key   = "single_node"

  cluster_spark_conf_single_node = {
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.master"                     = "local[*]"
  }

  cluster_tags_single_node = {
    "ResourceClass" = "SingleNode"
  }
}

# Cluster interativo, só para desenvolvimento e exploração (os jobs usam clusters de job).
resource "databricks_cluster" "main" {
  cluster_name            = "cluster-clima-energia"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = local.cluster_node_type
  autotermination_minutes = 20
  num_workers             = 0
  data_security_mode      = "SINGLE_USER"
  single_user_name        = data.databricks_current_user.me.user_name
  spark_conf              = local.cluster_spark_conf_single_node
  custom_tags             = local.cluster_tags_single_node
}
