resource "azurerm_databricks_workspace" "main" {
  name                        = "dbw-clima-energia-br"
  resource_group_name         = azurerm_resource_group.main.name
  location                    = azurerm_resource_group.main.location
  sku                         = "premium"
  managed_resource_group_name = "rg-managed-dbw-clima-energia-br"

  custom_parameters {
    no_public_ip = true
  }

  tags = {
    project    = "clima-energia"
    managed_by = "terraform"
  }
}