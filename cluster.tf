data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

data "databricks_current_user" "me" {}

resource "databricks_cluster" "main" {
  cluster_name            = "cluster-clima-energia"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = "Standard_DC4as_v5"
  autotermination_minutes = 20
  num_workers             = 0
  data_security_mode      = "SINGLE_USER"
  single_user_name        = data.databricks_current_user.me.user_name

  spark_conf = {
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.master"                     = "local[*]"
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
}