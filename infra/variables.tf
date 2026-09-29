variable "environments" {
  description = "Ambientes de deploy e suas configurações"
  type = map(object({
    git_branch      = string
    schedule_paused = bool
  }))
  default = {
    dev = {
      git_branch      = "develop"
      schedule_paused = true
    }
    prod = {
      git_branch      = "main"
      schedule_paused = false
    }
  }

  validation {
    condition     = alltrue([for env in keys(var.environments) : contains(["dev", "prod"], env)])
    error_message = "Os ambientes devem ser 'dev' ou 'prod'."
  }
}

variable "email_alertas" {
  description = "E-mail que recebe alertas de orçamento e de falha dos jobs"
  type        = string
}

variable "orcamento_mensal" {
  description = "Orçamento mensal da assinatura, na moeda de cobrança (BRL); alertas em 50%, 80% e 100%"
  type        = number
  default     = 300
}
