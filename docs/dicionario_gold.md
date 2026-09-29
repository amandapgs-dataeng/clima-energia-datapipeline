# Dicionário de dados — gold

Catalogs `clima_energia_dev` e `clima_energia_prod`, schema `gold`. A gold responde às perguntas
de negócio do projeto: quanto do potencial eólico e solar do Nordeste vira energia, e por quê.

Construída com Lakeflow Declarative Pipelines (`pipelines/gold/`): uma materialized view por
tabela, recalculada a partir da silver do mesmo ambiente sempre que o job de tratamento roda
(`sincronizar_codigo → silver → gold`). As regras de cálculo estão em `pipelines/gold/metricas/`
e são testadas em `tests/gold/`, inclusive contra o schema real da silver. As tabelas de colunas
abaixo são geradas a partir das definições do código.

## Visão geral

| Tabela | Pergunta | Origem |
|---|---|---|
| `aproveitamento_mensal` | Quanto da capacidade eólica e solar instalada no Nordeste vira energia, ao longo do ano? | `silver.fator_capacidade` |
| `restricao_geracao_diaria` | Quanto da geração eólica e solar prevista o ONS deixou de programar? | `silver.previsao_programado` |
| `restricao_geracao_por_usina` | Quais usinas tiveram mais geração prevista e não programada? | `silver.previsao_programado` |
| `precisao_programacao_mensal` | Quanto a geração real das usinas eólicas e solares do Nordeste se afasta do que o ONS programou? | `silver.fator_capacidade` |
| `clima_x_geracao_horaria` | O clima no local das usinas explica a geração eólica e solar? | `silver.fator_capacidade`, `silver.clima_observado` |
| `curva_de_potencia` | Como o fator de capacidade responde ao vento (eólica) e à radiação (solar)? | `gold.clima_x_geracao_horaria` |
| `correlacao_clima_geracao` | Quanto cada variável climática se relaciona com a geração, estado a estado? | `gold.clima_x_geracao_horaria` |
| `precisao_previsao_tempo` | Quanto a previsão do tempo feita na véspera acerta o vento, a radiação e a temperatura? | `silver.clima_previsao`, `silver.clima_observado` |
| `carga_x_temperatura` | O consumo de energia do Nordeste sobe nos dias mais quentes? | `silver.carga_energia`, `silver.clima_observado` |

## Convenções

- **Recorte**: subsistema Nordeste (`id_subsistema = NE`) e fontes eólica e solar, exceto nas
  tabelas de restrição, que são do Brasil (ver abaixo).
- **Energia** em MWh: soma de valores horários em MW médios (1 hora x 1 MWmed = 1 MWh); na
  programação do ONS, que é por meia hora, cada patamar vale 0,5 h.
- **Fator de capacidade agregado** = energia gerada / energia da capacidade instalada, ou seja,
  ponderado pela capacidade (não a média simples dos fatores das usinas).
- **Vento** em m/s nas análises de geração (padrão das curvas de potência) e em km/h na precisão
  da previsão (unidade da fonte).

## Limitações conhecidas

- **Restrição de geração no nível Brasil**: a programação do ONS não informa o estado da usina.
  Na eólica, 91% da capacidade está no Nordeste; na solar, cerca de metade.
- **Restrição = previsto - programado**, quando positivo. É um indicador da geração que o ONS
  deixou de programar, não a medição oficial de cortes (constrained-off).
- **Programação disponível desde 04/2026** (reprocessamento de 6 meses, por custo).
- **Feriados** não são identificados em `carga_x_temperatura`.

## Tabelas

### `aproveitamento_mensal`

**Pergunta:** Quanto da capacidade eólica e solar instalada no Nordeste vira energia, ao longo do ano?

Fator de capacidade mensal das usinas eólicas e solares do Nordeste, por estado. Origem: `silver.fator_capacidade`.

| Coluna | Tipo | Descrição |
|---|---|---|
| `mes` | DATE | Primeiro dia do mês |
| `uf` | STRING | Sigla do estado |
| `tipo_usina` | STRING | Eólica ou Solar |
| `usinas` | BIGINT | Usinas e conjuntos com medição no mês |
| `geracao_verificada_mwh` | DOUBLE | Energia gerada no mês, em MWh |
| `geracao_programada_mwh` | DOUBLE | Energia programada pelo ONS no mês, em MWh |
| `capacidade_mwh` | DOUBLE | Energia que a capacidade instalada geraria operando o mês inteiro a plena carga, em MWh |
| `fator_capacidade` | DOUBLE | Geração verificada dividida pela capacidade (0 a 1) |
| `fator_capacidade_programado` | DOUBLE | Geração programada dividida pela capacidade (0 a 1) |

### `restricao_geracao_diaria`

**Pergunta:** Quanto da geração eólica e solar prevista o ONS deixou de programar?

Geração prevista x programada pelo ONS por dia (Brasil; 91% da capacidade eólica fica no Nordeste). Origem: `silver.previsao_programado`.

| Coluna | Tipo | Descrição |
|---|---|---|
| `data` | DATE | Dia da programação |
| `usinas` | BIGINT | Usinas eólicas e solares programadas no dia |
| `previsto_mwh` | DOUBLE | Energia que o ONS previu que as usinas poderiam gerar, em MWh |
| `programado_mwh` | DOUBLE | Energia que o ONS programou para as usinas gerarem, em MWh |
| `restricao_mwh` | DOUBLE | Energia prevista e não programada (soma das meias horas com programado abaixo do previsto), em MWh |
| `restricao_pct` | DOUBLE | Restrição em relação ao previsto (0 a 1) |
| `meias_horas_com_restricao_pct` | DOUBLE | Fração das meias horas por usina com programado abaixo do previsto (0 a 1) |

### `restricao_geracao_por_usina`

**Pergunta:** Quais usinas tiveram mais geração prevista e não programada?

Geração prevista x programada pelo ONS por usina e mês. Origem: `silver.previsao_programado`.

| Coluna | Tipo | Descrição |
|---|---|---|
| `mes` | DATE | Primeiro dia do mês |
| `codigo_usina` | STRING | Código da usina na programação do ONS |
| `nome_usina` | STRING | Nome da usina na programação do ONS |
| `previsto_mwh` | DOUBLE | Energia prevista no mês, em MWh |
| `restricao_mwh` | DOUBLE | Energia prevista e não programada no mês, em MWh |
| `restricao_pct` | DOUBLE | Restrição em relação ao previsto (0 a 1) |

### `precisao_programacao_mensal`

**Pergunta:** Quanto a geração real das usinas eólicas e solares do Nordeste se afasta do que o ONS programou?

Erro e viés da programação do ONS em relação à geração verificada, por mês, estado e fonte. Origem: `silver.fator_capacidade`.

| Coluna | Tipo | Descrição |
|---|---|---|
| `mes` | DATE | Primeiro dia do mês |
| `uf` | STRING | Sigla do estado |
| `tipo_usina` | STRING | Eólica ou Solar |
| `geracao_programada_mwh` | DOUBLE | Energia programada pelo ONS, em MWh |
| `geracao_verificada_mwh` | DOUBLE | Energia efetivamente gerada, em MWh |
| `erro_absoluto_mwh` | DOUBLE | Soma, hora a hora e usina a usina, da diferença absoluta entre programado e verificado, em MWh |
| `erro_relativo` | DOUBLE | Erro absoluto dividido pela geração verificada (WAPE) |
| `vies_relativo` | DOUBLE | (verificado - programado) / programado; negativo = gerou menos que o programado |

### `clima_x_geracao_horaria`

**Pergunta:** O clima no local das usinas explica a geração eólica e solar?

Fator de capacidade horário por estado e fonte no Nordeste, com o clima observado no ponto das usinas. Origem: `silver.fator_capacidade`, `silver.clima_observado`.

| Coluna | Tipo | Descrição |
|---|---|---|
| `data_hora` | TIMESTAMP_NTZ | Hora local (sem fuso) |
| `uf` | STRING | Sigla do estado |
| `tipo_usina` | STRING | Eólica ou Solar |
| `ponto_id` | STRING | Ponto de clima: centro das usinas da fonte no estado |
| `geracao_verificada_mwmed` | DOUBLE | Geração das usinas da fonte no estado, em MW médios |
| `capacidade_mw` | DOUBLE | Capacidade instalada das usinas da fonte no estado, em MW |
| `fator_capacidade` | DOUBLE | Geração dividida pela capacidade (0 a 1) |
| `vento_100m_ms` | DOUBLE | Velocidade do vento a 100 m no ponto, em m/s |
| `radiacao_solar_wm2` | DOUBLE | Radiação solar no ponto, em W/m² |
| `cobertura_nuvens_pct` | DOUBLE | Cobertura de nuvens no ponto, em % |
| `temperatura_c` | DOUBLE | Temperatura do ar no ponto, em °C |
| `precipitacao_mm` | DOUBLE | Precipitação na hora no ponto, em mm |

### `curva_de_potencia`

**Pergunta:** Como o fator de capacidade responde ao vento (eólica) e à radiação (solar)?

Fator de capacidade por faixa de vento a 100 m ou de radiação solar, no Nordeste. Origem: `gold.clima_x_geracao_horaria`.

| Coluna | Tipo | Descrição |
|---|---|---|
| `tipo_usina` | STRING | Eólica ou Solar |
| `variavel` | STRING | vento_100m_ms (eólica) ou radiacao_solar_wm2 (solar) |
| `faixa_inicio` | DOUBLE | Início da faixa da variável climática |
| `faixa_fim` | DOUBLE | Fim da faixa da variável climática |
| `horas` | BIGINT | Horas-estado observadas na faixa |
| `fator_capacidade` | DOUBLE | Fator de capacidade na faixa (geração total / capacidade total) |
| `fator_capacidade_p25` | DOUBLE | Percentil 25 do fator de capacidade horário na faixa |
| `fator_capacidade_p75` | DOUBLE | Percentil 75 do fator de capacidade horário na faixa |

### `correlacao_clima_geracao`

**Pergunta:** Quanto cada variável climática se relaciona com a geração, estado a estado?

Correlação entre variáveis climáticas e o fator de capacidade horário, por estado e fonte. Origem: `gold.clima_x_geracao_horaria`.

| Coluna | Tipo | Descrição |
|---|---|---|
| `uf` | STRING | Sigla do estado |
| `tipo_usina` | STRING | Eólica ou Solar |
| `variavel` | STRING | Variável climática correlacionada com o fator de capacidade |
| `correlacao` | DOUBLE | Correlação de Pearson entre a variável e o fator de capacidade horário (-1 a 1) |
| `horas` | BIGINT | Horas usadas no cálculo (na solar, só horas com sol) |

### `precisao_previsao_tempo`

**Pergunta:** Quanto a previsão do tempo feita na véspera acerta o vento, a radiação e a temperatura?

Erro da previsão D+1 do Open-Meteo em relação ao observado, por mês e ponto. Origem: `silver.clima_previsao`, `silver.clima_observado`.

| Coluna | Tipo | Descrição |
|---|---|---|
| `mes` | DATE | Primeiro dia do mês |
| `ponto_id` | STRING | Ponto de clima |
| `uf` | STRING | Sigla do estado do ponto |
| `tipo_ponto` | STRING | eolica, solar ou capital |
| `horas` | BIGINT | Horas com previsão D+1 e observação |
| `vento_100m_observado_kmh` | DOUBLE | Vento a 100 m médio observado, em km/h |
| `erro_medio_absoluto_vento_100m_kmh` | DOUBLE | Erro médio absoluto da previsão D+1 do vento a 100 m, em km/h |
| `vies_vento_100m_kmh` | DOUBLE | Previsto menos observado, em média; positivo = previsão superestima o vento |
| `radiacao_observada_wm2` | DOUBLE | Radiação solar média observada nas horas com sol, em W/m² |
| `erro_medio_absoluto_radiacao_wm2` | DOUBLE | Erro médio absoluto da previsão D+1 da radiação nas horas com sol, em W/m² |
| `vies_radiacao_wm2` | DOUBLE | Previsto menos observado nas horas com sol, em média |
| `erro_medio_absoluto_temperatura_c` | DOUBLE | Erro médio absoluto da previsão D+1 da temperatura, em °C |

### `carga_x_temperatura`

**Pergunta:** O consumo de energia do Nordeste sobe nos dias mais quentes?

Carga diária do Nordeste com a temperatura das capitais e o dia da semana. Origem: `silver.carga_energia`, `silver.clima_observado`.

| Coluna | Tipo | Descrição |
|---|---|---|
| `data` | DATE | Dia |
| `carga_mwmed` | DOUBLE | Carga média do dia no subsistema Nordeste, em MW médios |
| `temperatura_media_c` | DOUBLE | Temperatura média do dia nas capitais (Fortaleza, Recife, Salvador), em °C |
| `temperatura_maxima_c` | DOUBLE | Maior temperatura horária do dia entre as capitais, em °C |
| `dia_semana` | INT | Dia da semana ISO: 1 = segunda-feira, 7 = domingo |
| `fim_de_semana` | BOOLEAN | Sábado ou domingo (feriados não são identificados) |
