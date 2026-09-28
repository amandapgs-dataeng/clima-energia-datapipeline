resource "azurerm_resource_group" "main" {
  name     = "rg-clima-energia-br"
  location = "East US"

  tags = {
    project    = "clima-energia"
    managed_by = "terraform"
  }
}