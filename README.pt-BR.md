# Clima × Energia Renovável — Nordeste

[![CI](https://github.com/amandapgs-dataeng/clima-energia-datapipeline/actions/workflows/ci.yml/badge.svg?branch=develop)](https://github.com/amandapgs-dataeng/clima-energia-datapipeline/actions/workflows/ci.yml)
![Azure Databricks](https://img.shields.io/badge/Azure%20Databricks-Unity%20Catalog-FF3621?logo=databricks&logoColor=white)
![Terraform](https://img.shields.io/badge/IaC-Terraform-7B42BC?logo=terraform&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)

🇺🇸 [Read in English](README.md)

Projeto de engenharia de dados de ponta a ponta que ingere **dados de clima** (Open-Meteo) e
**dados do sistema elétrico** do Operador Nacional do Sistema (ONS) para estudar como o clima
influencia a **geração eólica e solar no Nordeste**, a região que produz a maior parte da
energia eólica do país.

Tudo é código: infraestrutura, permissões, jobs e a esteira de entrega são versionados,
testados e promovidos automaticamente.

## Arquitetura

```mermaid
flowchart LR
    subgraph Fontes
        OM[API Open-Meteo<br/>previsão + clima observado]
        ONS[Dados abertos do ONS<br/>geração, fator de capacidade,<br/>carga, programação]
    end

    subgraph Databricks["Azure Databricks · Unity Catalog"]
        B[(Bronze<br/>clima_energia_bronze<br/>única, bruta, compartilhada)]
        subgraph DEV[clima_energia_dev]
            SD[(Silver)] --> GD[(Gold)]
        end
        subgraph PROD[clima_energia_prod]
            SP[(Silver)] --> GP[(Gold)]
        end
        B -- somente leitura --> SD
        B --> SP
    end

    OM --> B
    ONS --> B
    GP --> D[Dashboard]
```

| Camada | Pergunta que responde | Escopo |
|---|---|---|
| **Bronze** | O que a fonte mandou? | Uma cópia para todos os ambientes. Cópia fiel da fonte, sem regra de negócio. |
| **Silver** | O dado é confiável, limpo e padronizado? | Por ambiente. Deduplicação, tipagem, vocabulário comum entre fontes, checagens de qualidade. |
| **Gold** | O que isso significa para o negócio? | Por ambiente. Agregações que respondem às perguntas de negócio. |

## Destaques de engenharia

- **Infraestrutura como código, de ponta a ponta.** Resource group, ADLS Gen2, workspace
  Databricks, Unity Catalog (credencial, external locations, catalogs, schemas, permissões),
  jobs e alerta de orçamento, tudo em Terraform. O state fica num backend remoto no Azure, com
  lock, versionamento e soft delete.
- **Ambientes com isolamento real.** Dev e prod têm catalogs separados para silver e gold. Dev
  roda com um service principal próprio, que só tem `SELECT` na bronze. Um bug em dev não
  consegue tocar o dado bruto.
- **Bronze única e fiel à fonte.** O dado é ingerido uma vez, completo e sem regras de negócio,
  e compartilhado pelos ambientes. Os filtros ficam na silver: corrigir um filtro exige só
  reprocessar, sem nova ingestão.
- **CI/CD com portão de qualidade.** Toda mudança passa por um PR para a `develop`, onde o CI
  roda os testes unitários, uma varredura de segredos (gitleaks) e `terraform fmt`/`validate`.
  Com o CI verde, um workflow promove a `develop` para a `main` protegida automaticamente. Os
  jobs de produção leem o código da `main`.
- **Ingestão defensiva.** Retry HTTP com backoff exponencial e timeout, logging estruturado e um
  log de auditoria que registra falhas além de sucessos. O job continua falhando de forma
  visível e mandando alerta.
- **Controle de custo.** Os jobs rodam em clusters de job efêmeros, de um nó só, e um alerta de
  orçamento acompanha o gasto (menos de US$ 50 por mês).

## O que o profiling do dado revelou

Antes de escrever qualquer transformação, a bronze foi perfilada
([dicionário da bronze](docs/dicionario_bronze.md)). Alguns achados que definem a silver:

- **O horário do ONS é local, mas vem rotulado como UTC.** A geração solar começa às 6h e
  termina às 17h, o que só é plausível em horário local. Tratar como UTC deslocaria em 3 horas
  todo cruzamento com o clima.
- **O mesmo conceito tem nomes diferentes entre datasets:** `EOLIELÉTRICA` × `Eólica`,
  `TIPO I` × `Tipo I`.
- **Algumas "duplicatas" não são duplicatas.** Usinas híbridas (eólica + solar) aparecem duas
  vezes por hora. Usinas podem compartilhar o código regulatório (`ceg`), e usinas agregadas
  usam `-` como código. As chaves naturais precisaram ser descobertas, não supostas.
- **Reingestão é histórico, não ruído.** A carga é relida numa janela móvel de 60 dias toda
  semana, e o ONS pode revisar valores passados. A silver guarda o histórico de versões.

## Estrutura do repositório

```
.github/workflows/   CI (testes, gitleaks, terraform) e promoção automática para a main
infra/               Terraform: Azure + Databricks + Unity Catalog + jobs
  bootstrap/         Script, rodado uma vez, que cria o storage do state remoto
notebooks/           Notebooks Databricks (ingestão)
pipelines/silver/    Lakeflow Declarative Pipeline da camada silver
  utils/             Funções compartilhadas e testadas (datas, HTTP, logging, gravação na bronze)
tests/               Testes com pytest (helpers sem Spark; transformações da silver em Spark local)
docs/                Dicionários de dados (bronze, silver) e decisões de desenho
migracoes/           Migrações de dados pontuais (SQL)
```

## Fluxo de entrega

```mermaid
flowchart LR
    F[feature/*] -- PR + CI --> D[develop]
    D -- CI verde --> P{{workflow de promoção}}
    P -- PR + merge automáticos --> M[main]
    M -- git source --> J[jobs Databricks]
```

## Como rodar

Pré-requisitos: Azure CLI (com login feito), Terraform ≥ 1.7 e Python 3.12.

```bash
# 1. Só uma vez: cria o storage do state do Terraform
./infra/bootstrap/criar_backend_state.sh

# 2. Infraestrutura
cd infra
cp terraform.tfvars.example terraform.tfvars   # preencha o e-mail de alertas
terraform init
terraform apply

# 3. Testes
pip install -r requirements-dev.txt
pytest
```

## Roadmap

- [x] Infraestrutura como código, ambientes dev/prod e acesso com privilégio mínimo
- [x] CI/CD com portão de qualidade e promoção automática
- [x] Ingestão na bronze (6 datasets, 2 fontes), profiling e dicionário de dados
- [x] Silver: 6 tabelas com deduplicação, histórico de versões, tipagem, vocabulário comum e
  regras de qualidade ([dicionário da silver](docs/dicionario_silver.md))
- [ ] Gold: métricas de negócio da geração eólica e solar no Nordeste
- [ ] Dashboard

## Convenções

- Branches: `feature/*`, `fix/*`, `docs/*`, `chore/*` → PR para a `develop`.
- Commits seguem o [Conventional Commits](https://www.conventionalcommits.org/pt-br/)
  (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`, `ci:`).
- O código e a documentação estão em português; o README é bilíngue.
