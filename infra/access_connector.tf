resource "azurerm_databricks_access_connector" "main" {
  name                = "ac-clima-energia-br"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  identity {
    type = "SystemAssigned"
  }

  tags = {
    project    = "clima-energia"
    managed_by = "terraform"
  }
}

resource "azurerm_role_assignment" "access_connector_storage" {
  scope                = azurerm_storage_account.main.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.main.identity[0].principal_id
}