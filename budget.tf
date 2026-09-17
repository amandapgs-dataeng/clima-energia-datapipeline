data "azurerm_subscription" "current" {}

resource "azurerm_consumption_budget_subscription" "main" {
  name            = "budget-clima-energia-br"
  subscription_id = data.azurerm_subscription.current.id
  amount          = 50
  time_grain      = "Monthly"

  time_period {
    start_date = "2026-09-01T00:00:00Z"
    end_date   = "2028-09-01T00:00:00Z"
  }

  notification {
    enabled        = true
    threshold      = 50
    operator       = "GreaterThanOrEqualTo"
    threshold_type = "Actual"
    contact_emails = ["amandapgs@hotmail.com"]
  }

  notification {
    enabled        = true
    threshold      = 80
    operator       = "GreaterThanOrEqualTo"
    threshold_type = "Actual"
    contact_emails = ["amandapgs@hotmail.com"]
  }

  notification {
    enabled        = true
    threshold      = 100
    operator       = "GreaterThanOrEqualTo"
    threshold_type = "Actual"
    contact_emails = ["amandapgs@hotmail.com"]
  }
}