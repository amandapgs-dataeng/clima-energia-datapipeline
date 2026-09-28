#!/usr/bin/env bash
# Cria (uma única vez) o storage onde o Terraform guarda o state remoto.
#
# Fica fora do Terraform principal de propósito: o state não pode morar num recurso
# gerenciado pelo próprio state (um destroy apagaria o próprio histórico).
# Resource group separado, versionamento e soft delete protegem o state contra
# exclusão ou sobrescrita acidental.
set -euo pipefail

RESOURCE_GROUP="rg-clima-energia-tfstate"
STORAGE_ACCOUNT="sttfstateclimaenergia"
CONTAINER="tfstate"
LOCATION="eastus"

az group create --name "$RESOURCE_GROUP" --location "$LOCATION" \
  --tags project=clima-energia managed_by=bootstrap --output none

az storage account create \
  --name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --min-tls-version TLS1_2 \
  --allow-blob-public-access false \
  --tags project=clima-energia managed_by=bootstrap \
  --output none

az storage account blob-service-properties update \
  --account-name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --enable-versioning true \
  --enable-delete-retention true --delete-retention-days 30 \
  --enable-container-delete-retention true --container-delete-retention-days 30 \
  --output none

az storage container create \
  --name "$CONTAINER" \
  --account-name "$STORAGE_ACCOUNT" \
  --auth-mode key \
  --output none

echo "Backend pronto: $STORAGE_ACCOUNT/$CONTAINER (resource group $RESOURCE_GROUP)"
