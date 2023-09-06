provider "azurerm" {
  features {}
}

# Crea un resource group
resource "azurerm_resource_group" "mongo-cdc-poc-rg" {
  name     = "mongo-cdc-poc-rg"
  location = "West Europe"
}

resource "azurerm_eventhub_namespace" "mongo-cdc-poc-eh-namespace" {
  name                = "mongo-cdc-poc-eh-namespace"
  location            = azurerm_resource_group.mongo-cdc-poc-rg.location
  resource_group_name = azurerm_resource_group.mongo-cdc-poc-rg.name
  sku                 = "Standard"
  capacity            = 1
}

resource "azurerm_eventhub" "mongo-cdc-poc-eventhub" {
  name                = "mongo-cdc-poc-eventhub"
  namespace_name      = azurerm_eventhub_namespace.mongo-cdc-poc-eh-namespace.name
  resource_group_name = azurerm_resource_group.mongo-cdc-poc-rg.name
  partition_count     = 2
  message_retention   = 1
}

resource "azurerm_eventhub_authorization_rule" "eventhub-ar-mongo-cdc-poc" {
  name                = "eventhub-mongo-pg-cdc-poc"
  namespace_name      = azurerm_eventhub_namespace.mongo-cdc-poc-eh-namespace.name
  eventhub_name       = azurerm_eventhub.mongo-cdc-poc-eventhub.name
  resource_group_name = azurerm_resource_group.mongo-cdc-poc-rg.name

  listen = true
  send   = true
  manage = true
}

resource "azurerm_cosmosdb_account" "mongo-cdc-poc-cosmosdb" {
  name                = "mongo-cdc-poc-cosmosdb"
  resource_group_name = azurerm_resource_group.mongo-cdc-poc-rg.name
  location            = azurerm_resource_group.mongo-cdc-poc-rg.location
  offer_type          = "Standard"
  kind                = "MongoDB"

  consistency_policy {
    consistency_level = "Session"
  }

  geo_location {
    location          = azurerm_resource_group.mongo-cdc-poc-rg.location
    failover_priority = 0
  }
}

resource "azurerm_cosmosdb_mongo_database" "mongo-cdc-poc-mongodb" {
  name                = "mongo-cdc-poc-mongodb"
  resource_group_name = azurerm_resource_group.mongo-cdc-poc-rg.name
  account_name        = azurerm_cosmosdb_account.mongo-cdc-poc-cosmosdb.name
}