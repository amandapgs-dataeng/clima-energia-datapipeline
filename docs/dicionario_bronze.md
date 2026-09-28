# Dicionário de dados — bronze

Catalog `clima_energia_bronze`. A bronze é uma cópia fiel das fontes: nenhum filtro ou regra de
negócio é aplicado na ingestão. Tratamento, deduplicação e padronização acontecem na silver.

Perfil levantado em 28/09/2026 sobre os dados então disponíveis (mês de referência 08/2026 para
os datasets mensais do ONS).

## Visão geral

| Tabela | O que representa | Granularidade (1 linha =) | Chave natural | Ingestão |
|---|---|---|---|---|
| `ons.geracao_usina` | Geração verificada de cada usina | usina × hora | `din_instante` + `ceg` + `nom_usina` | mensal (mês fechado) |
| `ons.fator_capacidade` | Uso da capacidade instalada de usinas eólicas e solares | conjunto/usina × tipo × hora | `din_instante` + `nom_usina_conjunto` + `nom_tipousina` | mensal (mês fechado) |
| `ons.carga_energia` | Consumo de energia por subsistema | subsistema × dia | `din_instante` + `id_subsistema` | semanal (janela de 60 dias) |
| `ons.previsao_programado_eolsol` | Previsão e programação do ONS para usinas eólicas e solares | usina × dia × patamar de 30 min | `dat_programacao` + `num_patamar` + `cod_usinapdp` | diária (D-1) |
| `open_meteo.previsao_bruta` | Previsão horária do tempo em Fortaleza | resposta da API (24 horas) | `data_referencia` + `ingestion_timestamp` | diária |
| `open_meteo.historico_observado` | Tempo observado em Fortaleza | resposta da API (168 horas) | `start_date` + `ingestion_timestamp` | semanal |
| `controle._audit_log` | Execuções da ingestão (sucesso e falha) | execução de notebook | `pipeline_name` + `execution_timestamp` | a cada execução |

Todas as tabelas de dados têm duas colunas de controle, preenchidas pela ingestão:
`ingestion_timestamp` (momento da gravação, UTC) e `source` (identificador da fonte).

## Achados que valem para mais de uma tabela

**Horário do ONS é local, mas está rotulado como UTC.** `din_instante` aparece como
`2026-08-01T06:00:00Z`, mas representa o horário de Brasília: a geração solar começa às 6h e
termina às 17h, o que só faz sentido em horário local. A silver deve tratar esses valores como
horário local (sem fuso), senão o cruzamento com o clima fica deslocado em 3 horas.

**Mesmo conceito, nomes diferentes entre datasets.**

| Conceito | `geracao_usina` | `fator_capacidade` | `carga_energia` |
|---|---|---|---|
| Tipo eólica | `EOLIELÉTRICA` | `Eólica` | — |
| Tipo solar | `FOTOVOLTAICA` | `Solar` | — |
| Modalidade | `TIPO I`, `TIPO II-B` | `Tipo I`, `Tipo II-B` | — |
| Subsistema | `NORDESTE` | `NORDESTE` | `Nordeste`, `Sudeste/Centro-Oeste` |

A silver precisa de um vocabulário comum; `id_subsistema` (`N`, `NE`, `S`, `SE`) é consistente
entre os datasets e é a melhor chave para subsistema.

**`ceg = "-"` é um marcador, não um código.** Usinas agregadas ("Conjunto de Usinas",
"Pequenas Usinas") não têm CEG próprio e recebem `-`. Por isso `ceg` sozinho não identifica uma usina.

**Códigos de usina diferentes entre fontes.** `previsao_programado_eolsol` usa `cod_usinapdp`,
que não corresponde a `ceg` nem a `id_ons`. Cruzar a programação com a geração exige uma tabela
de correspondência.

**Valores levemente negativos são físicos.** Usinas solares registram cerca de −1,4 MW de
madrugada: é o consumo interno da usina sem geração. Não é erro de dado.

## Tipos de duplicata encontrados

| Tipo | Exemplo | Tratamento na silver |
|---|---|---|
| **Técnica** (mesmo dado gravado duas vezes) | `carga_energia`: 2 lotes idênticos por causa de um retry | descartar, manter uma versão |
| **Reingestão de janela** (a coleta relê períodos já lidos) | `carga_energia` relê 60 dias por semana; cada dia aparece ~8 vezes | guardar o histórico de versões (ver decisões) |
| **Aparente** (parece duplicata, mas são registros diferentes) | usinas híbridas, homônimas e CEG compartilhado | não é duplicata: a chave natural precisa incluir a coluna que as diferencia |

## Tabelas

### `ons.geracao_usina`

Geração verificada, hora a hora, de cada usina do Sistema Interligado Nacional (todas as fontes:
hídrica, térmica, eólica, solar, nuclear). Arquivo mensal do ONS; a ingestão baixa o último mês fechado.

| Coluna | Tipo | Descrição |
|---|---|---|
| `din_instante` | timestamp | Hora da medição (horário local, ver achados) |
| `id_subsistema` / `nom_subsistema` | string | Subsistema (`N`, `NE`, `S`, `SE`) |
| `id_estado` / `nom_estado` | string | UF da usina |
| `cod_modalidadeoperacao` | string | Classificação do ONS pela forma de programação e despacho (`TIPO I`, `TIPO II-A/B/C`, `TIPO III`, "Conjunto de Usinas", "Pequenas Usinas") |
| `nom_tipousina` | string | Fonte: `HIDROELÉTRICA`, `TÉRMICA`, `EOLIELÉTRICA`, `FOTOVOLTAICA`, `NUCLEAR` |
| `nom_tipocombustivel` | string | Combustível (térmicas) |
| `nom_usina` | string | Nome da usina ou do conjunto |
| `id_ons` | string | Código da usina no ONS (nulo em "Pequenas Usinas") |
| `ceg` | string | Código ANEEL do empreendimento; `-` em usinas agregadas |
| `val_geracao` | double | Geração na hora |

- **Volume**: ~721 usinas × 744 horas = 536.712 linhas por mês.
- **Chave**: `din_instante + ceg + nom_usina`. Nenhuma combinação menor é única:
  - `ceg` se repete em usinas agregadas (`-`) e é compartilhado por Belo Monte e Pimental;
  - `nom_usina` se repete em usinas homônimas ("UTE SUZANO" no MA e no MS).
- **Nulos em `val_geracao`** (~12%): concentrados em térmicas e hídricas das modalidades
  II-C e III, que não têm medição horária centralizada. Estrutural; não afeta eólica e solar.
- **Negativos**: 11 valores (mínimo −1,42), usina solar de madrugada.

### `ons.fator_capacidade`

Geração programada, verificada, capacidade instalada e fator de capacidade, por hora, de
conjuntos e usinas **eólicos e solares**. Arquivo mensal do ONS.

| Coluna | Tipo | Descrição |
|---|---|---|
| `din_instante` | timestamp | Hora (horário local) |
| `id_subsistema`, `nom_subsistema`, `id_estado`, `nom_estado` | string | Localização |
| `cod_pontoconexao`, `nom_pontoconexao`, `nom_localizacao` | string | Ponto de conexão à rede |
| `val_latitude*`, `val_longitude*` | double | Coordenadas da subestação coletora e do ponto de conexão |
| `nom_modalidadeoperacao` | string | Modalidade (`Tipo I`, `Tipo II-B`, "Conjunto de Usinas") |
| `nom_tipousina` | string | `Eólica` ou `Solar` |
| `nom_usina_conjunto` | string | Nome do conjunto ou usina |
| `id_ons`, `ceg` | string | Códigos (`ceg = -` em conjuntos) |
| `val_geracaoprogramada` | **string** | Geração programada (número em texto, ex.: `149.500`) |
| `val_geracaoverificada` | **string** | Geração verificada (número em texto) |
| `val_capacidadeinstalada` | **string** | Capacidade instalada (número em texto) |
| `val_fatorcapacidade` | double | `verificada / capacidade` (conferido) |

- **Volume**: 231 conjuntos/usinas × 744 horas ≈ 174.600 linhas por mês.
- **Chave**: `din_instante + nom_usina_conjunto + nom_tipousina`. Cinco conjuntos
  (Babilônia Centro, Babilônia Sul, Serra da Babilônia, Serra do Mato, São Basílio) são
  **híbridos** e têm uma linha eólica e outra solar por hora. `id_ons` também é único por hora.
- Os três valores em texto convertem para número sem perdas.
- Fator entre −0,005 e 1,0; os 11 negativos são consumo noturno.

### `ons.carga_energia`

Carga de energia (consumo) diária por subsistema, em MW médios. Arquivo anual do ONS; a ingestão
relê uma janela de 60 dias a cada semana.

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_subsistema` / `nom_subsistema` | string | Subsistema |
| `din_instante` | timestamp | Dia (sempre 00:00) |
| `val_cargaenergiamwmed` | double | Carga média do dia (MWmed) |

- **Granularidade diária**, não horária. Os dados chegam com ~3 dias de atraso.
- **Duplicatas**: por desenho, cada dia é relido em até ~8 semanas seguidas. Se o ONS revisar
  um valor, as versões diferem; ver decisões.

### `ons.previsao_programado_eolsol`

Para cada usina eólica ou solar, a geração **prevista** e a **programada** pelo ONS em cada meia
hora do dia. Arquivo diário; a ingestão baixa o dia anterior (D-1).

| Coluna | Tipo | Descrição |
|---|---|---|
| `dat_programacao` | **string** | Dia, no formato `yyyyMMdd` (ex.: `20260923`) |
| `num_patamar` | int | Meia hora do dia, de 1 a 48 |
| `cod_usinapdp` | string | Código da usina na programação (tamanho fixo, com espaços à direita) |
| `nom_usinapdp` | string | Nome da usina (tamanho fixo, com espaços à direita) |
| `val_previsao` | **string** | Geração prevista (número em texto) |
| `val_programado` | **string** | Geração programada (número em texto) |

- **Volume**: 628 usinas × 48 patamares = 30.144 linhas por dia.
- **Chave** única, sem duplicatas: cada dia chega uma vez.
- Valores de 0 a ~1.100.

### `open_meteo.previsao_bruta`

Resposta completa da API de previsão do Open-Meteo para Fortaleza.

| Coluna | Tipo | Descrição |
|---|---|---|
| `data_referencia` | string | Dia da **emissão** da previsão (dia em que o job rodou) |
| `data_prevista` | string | Dia que a previsão descreve (a partir de 28/09/2026; ver decisões) |
| `horizonte_dias` | long | Distância entre emissão e dia previsto (a partir de 28/09/2026) |
| `latitude_requested`, `longitude_requested` | double | Ponto pedido (−3,72; −38,54) |
| `raw_response` | string | JSON completo da API |

- O JSON tem `hourly.time` (24 horas, horário de `America/Fortaleza`) e uma série por variável
  (21 variáveis: temperatura, umidade, precipitação, nuvens, vento, radiação etc.).
- Unidades: temperatura em °C, vento em km/h, radiação em W/m² (em `hourly_units`).
- A API devolve o ponto de grade mais próximo (−3,76; −38,53), não exatamente o pedido.
- Nenhum nulo nas séries horárias.

### `open_meteo.historico_observado`

Resposta completa da API de arquivo histórico (tempo observado) do Open-Meteo, para a janela de
16 a 10 dias antes da data de referência (a API publica com atraso).

| Coluna | Tipo | Descrição |
|---|---|---|
| `start_date`, `end_date` | string | Janela pedida (7 dias) |
| `latitude_requested`, `longitude_requested` | double | Ponto pedido |
| `raw_response` | string | JSON completo (168 horas) |

- No agendamento semanal, as janelas se encaixam sem sobreposição. Execuções fora do agendamento
  (como a manual de 28/09/2026) criam janelas sobrepostas.

### `controle._audit_log`

Uma linha por execução de notebook de ingestão.

| Coluna | Tipo | Descrição |
|---|---|---|
| `pipeline_name` | string | Notebook (`01_forecast_clima` etc.) |
| `data_referencia` | string | Data de referência da execução |
| `execution_timestamp` | timestamp | Início da execução (UTC) |
| `status` | string | `success` ou `failed` |
| `linhas_gravadas` | long | Linhas gravadas na bronze |
| `duracao_segundos` | double | Duração (a partir de 28/09/2026) |
| `erro` | string | Mensagem de erro, se falhou (a partir de 28/09/2026) |

## Decisões

| Data | Decisão | Motivo |
|---|---|---|
| 28/09/2026 | A previsão do tempo passa a buscar o **dia seguinte** (`horizonte_dias = 1`) | O job roda às 21h; buscar o dia corrente não é uma previsão. As linhas anteriores (24 a 28/09) têm horizonte 0: previam o próprio dia da emissão. |
| 28/09/2026 | A silver **guarda o histórico de versões** das releituras do ONS (ex.: revisões da carga) | Revisões são informação de negócio: mostram quanto e quando o dado mudou. A silver terá a versão atual e o histórico. |
| 28/09/2026 | A silver guarda **todos os subsistemas**; o recorte do Nordeste acontece na gold | Silver sem regra de negócio serve a qualquer pergunta futura. |
