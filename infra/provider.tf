terraform {
  required_version = ">= 1.7"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.132"
    }
  }

  # State remoto no Azure (criado por bootstrap/criar_backend_state.sh), com lock
  # nativo via lease do blob, versionamento e soft delete.
  backend "azurerm" {
    resource_group_name  = "rg-clima-energia-tfstate"
    storage_account_name = "sttfstateclimaenergia"
    container_name       = "tfstate"
    key                  = "clima-energia.tfstate"
  }
}

provider "azurerm" {
  features {}
}