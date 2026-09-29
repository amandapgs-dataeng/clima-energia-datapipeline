# Dicionário de dados — silver

Catalogs `clima_energia_dev` e `clima_energia_prod`, schema `silver`. A silver é o dado
**tratado**: deduplicado, tipado, padronizado e validado, mas ainda sem regras de negócio
(nenhum recorte de região ou de fonte: isso é papel da gold).

Construída com Lakeflow Declarative Pipelines (`pipelines/silver/`), um pipeline por ambiente,
lendo a bronze única (`clima_energia_bronze`). Cada fonte descreve a sua tabela numa definição
(`TabelaSilver`: origem na bronze, tratamento, colunas, chave e regras de qualidade), reunidas em
`pipelines/silver/utilities/catalogo.py`. O pipeline, os testes de contrato e este dicionário
partem dessas definições; as tabelas de colunas abaixo são geradas a partir delas.

## Visão geral

| Tabela | 1 linha = | Chave | Histórico de versões | Origem na bronze |
|---|---|---|---|---|
| `carga_energia` | subsistema × dia | `id_subsistema`, `data` | sim | `ons.carga_energia` |
| `geracao_usina` | usina × hora | `data_hora`, `chave_usina` | sim | `ons.geracao_usina` |
| `fator_capacidade` | usina/conjunto eólico ou solar × hora | `data_hora`, `id_ons` | sim | `ons.fator_capacidade` |
| `previsao_programado` | usina eólica ou solar × meia hora | `data`, `patamar`, `codigo_usina` | sim | `ons.previsao_programado_eolsol` |
| `clima_previsao` | ponto × dia de emissão × hora | `ponto_id`, `data_emissao`, `data_hora` | não (cada emissão é um fato novo) | `open_meteo.previsao_bruta` e `open_meteo.previsao_historica_bruta` |
| `clima_observado` | ponto × hora | `ponto_id`, `data_hora` | não (janelas sobrepostas só se deduplicam) | `open_meteo.historico_observado` |

## Padrão das tabelas

Para cada tabela `x`:

| Objeto | Tipo | Para quê |
|---|---|---|
| `x` | streaming table (AUTO CDC, SCD tipo 1) | **Versão atual** de cada chave. É a tabela de consulta do dia a dia. |
| `x_quarentena` | streaming table | Registros **descartados** pelas regras de qualidade, com a lista de regras que violaram. |
| `x_versoes` | materialized view | **Histórico legível**: cada valor diferente publicado pela fonte é uma versão. |
| `x_historico_cdc` | streaming table (AUTO CDC, SCD tipo 2) | Histórico **técnico**, de uso interno do pipeline (colunas `__START_AT`/`__END_AT`). |

- **Deduplicação**: para cada chave, vale o valor com o `ingerido_em` mais recente. Retries e
  releituras da bronze não geram linhas repetidas.
- **Versões só quando o valor muda**: uma releitura com o mesmo valor apenas atualiza o
  `confirmado_em`; uma revisão da fonte cria uma nova versão e fecha a anterior.
- **Processamento incremental**: cada execução lê só o que chegou de novo na bronze. Um
  *full refresh* (reconstrução completa) só é necessário quando a estrutura muda, e é seguro
  porque a bronze guarda todas as ingestões.

Colunas de toda tabela `x_quarentena`: as mesmas de `x`, mais

| Coluna | Tipo | Descrição |
|---|---|---|
| `regras_violadas` | ARRAY<STRING> | Regras de descarte que o registro violou |

Colunas de toda tabela `x_versoes` (além das colunas de dados de `x`):

| Coluna | Tipo | Descrição |
|---|---|---|
| `numero_versao` | INT | Ordem da versão para a chave (1 = primeira publicação) |
| `vigente_desde` | TIMESTAMP | Quando esta versão passou a valer (ingestão em que o valor apareceu) |
| `vigente_ate` | TIMESTAMP | Quando esta versão foi substituída; nulo se ainda é a atual |
| `versao_atual` | BOOLEAN | Verdadeiro se esta é a versão vigente |
| `confirmado_em` | TIMESTAMP | Última ingestão que trouxe este mesmo valor (UTC) |


## Convenções

**Datas e horas**

| O que representa | Tipo | Exemplo |
|---|---|---|
| Relógio local publicado pela fonte (hora do ONS, hora do Open-Meteo) | `TIMESTAMP_NTZ` (sem fuso) | `data_hora`, `inicio_patamar` |
| Instante em que o sistema fez algo | `TIMESTAMP` em UTC | `ingerido_em`, `vigente_desde`, `confirmado_em` |
| Dia | `DATE` | `data`, `data_emissao` |

O ONS grava horário de Brasília rotulado como UTC ([dicionário da bronze](dicionario_bronze.md)).
A silver lê esse valor exatamente como publicado, sem deslocar 3 horas. Os pipelines rodam com
a sessão Spark em UTC para que nenhuma conversão implícita aconteça.

**Nomes de colunas**: sem os prefixos da fonte (`nom_`, `val_`, `din_`), em português, com a
**unidade no nome** quando há uma (`_mwmed`, `_mw`, `_c`, `_pct`, `_mm`, `_hpa`, `_kmh`,
`_wm2`, `_m`, `_graus`).

**Textos**: sem espaços nas pontas; texto vazio vira nulo; marcadores de ausência (`-` no lugar
de um código) viram nulo.

**Vocabulário comum** entre as fontes:

| Conceito | Na bronze | Na silver |
|---|---|---|
| Tipo de usina | `EOLIELÉTRICA`, `Eólica`, `FOTOVOLTAICA`, `Solar`, `HIDROELÉTRICA`, `TÉRMICA`, `NUCLEAR` | `Eólica`, `Solar`, `Hidrelétrica`, `Térmica`, `Nuclear` |
| Modalidade de operação | `TIPO I`, `Tipo I`, `TIPO II-B`, `Tipo II-B` | `Tipo I`, `Tipo II-B` (conjuntos e pequenas usinas mantêm o nome) |
| Subsistema | `NORDESTE`, `Nordeste`, `SUDESTE`, `Sudeste/Centro-Oeste` | `Nordeste`, `Sudeste/Centro-Oeste`, a partir do código (`NE`, `SE`) |

## Qualidade

Cada tabela tem duas classes de regra (expectations do pipeline), com métricas por execução no
painel do pipeline:

- **Descarte**: registros que quebram a estrutura (sem chave, subsistema desconhecido, sem o
  valor principal) não entram na silver. Vão para a tabela `x_quarentena`, com as regras que
  violaram, e continuam na bronze. Uma regra cujo resultado é nulo conta como violada.
- **Alerta**: valores suspeitos, mas publicados pela fonte, entram e ficam sinalizados. A silver
  não corrige o dado por conta própria.

| Tabela | Descarte | Alerta |
|---|---|---|
| `carga_energia` | sem chave; subsistema desconhecido; sem carga | carga ≤ 0 |
| `geracao_usina` | sem hora ou nome; subsistema desconhecido | tipo de usina desconhecido; geração < −10 MW |
| `fator_capacidade` | sem hora ou `id_ons`; subsistema desconhecido | tipo diferente de Eólica/Solar; valor em texto não numérico; fator fora de −0,05 a 1 |
| `previsao_programado` | sem data ou usina; patamar fora de 1 a 48 | valor em texto não numérico; valor negativo |
| `clima_previsao` | sem ponto, hora ou dia de emissão | temperatura fora de 5 a 48 °C; umidade fora de 0 a 100%; radiação ou vento a 100 m negativos |
| `clima_observado` | sem ponto ou hora | as mesmas da previsão |

## Tabelas

### `carga_energia`

Carga de energia (consumo) diária por subsistema.

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_subsistema` | STRING | Código do subsistema: N, NE, S, SE |
| `subsistema` | STRING | Nome padronizado do subsistema |
| `data` | DATE | Dia da medição |
| `carga_mwmed` | DOUBLE | Carga média do dia, em MW médios |
| `ingerido_em` | TIMESTAMP | Quando o valor foi ingerido pela última vez (UTC) |

- `din_instante` (00:00 do dia) vira `data`.

### `geracao_usina`

Geração verificada, hora a hora, de cada usina do país, de todas as fontes.

| Coluna | Tipo | Descrição |
|---|---|---|
| `data_hora` | TIMESTAMP_NTZ | Hora da medição, horário de Brasília (sem fuso) |
| `chave_usina` | STRING | Identificador da usina na silver: CEG + nome (o CEG sozinho não é único) |
| `nome_usina` | STRING | Nome da usina ou do conjunto de usinas |
| `ceg` | STRING | Código ANEEL do empreendimento; nulo em usinas agregadas |
| `id_ons` | STRING | Código da usina no ONS; nulo em pequenas usinas |
| `tipo_usina` | STRING | Fonte: Hidrelétrica, Térmica, Eólica, Solar ou Nuclear |
| `combustivel` | STRING | Combustível (usinas térmicas) |
| `modalidade_operacao` | STRING | Modalidade de operação no ONS (Tipo I, Tipo II-A/B/C, Tipo III, conjuntos, pequenas usinas) |
| `id_subsistema` | STRING | Código do subsistema: N, NE, S, SE |
| `subsistema` | STRING | Nome padronizado do subsistema |
| `uf` | STRING | Sigla do estado |
| `geracao_mwmed` | DOUBLE | Geração na hora, em MW médios; nulo quando o ONS não mede a usina hora a hora |
| `ingerido_em` | TIMESTAMP | Quando o valor foi ingerido pela última vez (UTC) |

- **`chave_usina`** existe porque nenhuma coluna da fonte identifica a usina sozinha: usinas
  agregadas usam `-` como CEG, Belo Monte e Pimental compartilham o mesmo CEG, e há usinas
  homônimas em estados diferentes.
- Nulos em `geracao_mwmed` são estruturais (usinas que o ONS não mede hora a hora) e são mantidos.

### `fator_capacidade`

Geração programada, verificada, capacidade instalada e fator de capacidade, hora a hora, de
usinas e conjuntos eólicos e solares.

| Coluna | Tipo | Descrição |
|---|---|---|
| `data_hora` | TIMESTAMP_NTZ | Hora da medição, horário de Brasília (sem fuso) |
| `id_ons` | STRING | Código da usina ou conjunto no ONS (distingue as partes de um conjunto híbrido) |
| `nome_usina` | STRING | Nome da usina ou do conjunto de usinas |
| `ceg` | STRING | Código ANEEL do empreendimento; nulo em conjuntos |
| `tipo_usina` | STRING | Fonte: Eólica ou Solar |
| `modalidade_operacao` | STRING | Modalidade de operação no ONS |
| `id_subsistema` | STRING | Código do subsistema: N, NE, S, SE |
| `subsistema` | STRING | Nome padronizado do subsistema |
| `uf` | STRING | Sigla do estado |
| `codigo_ponto_conexao` | STRING | Código do ponto de conexão à rede |
| `nome_ponto_conexao` | STRING | Nome do ponto de conexão à rede |
| `localizacao` | STRING | Localização informada pelo ONS |
| `latitude_coletora` | DOUBLE | Latitude da subestação coletora |
| `longitude_coletora` | DOUBLE | Longitude da subestação coletora |
| `latitude_ponto_conexao` | DOUBLE | Latitude do ponto de conexão |
| `longitude_ponto_conexao` | DOUBLE | Longitude do ponto de conexão |
| `geracao_programada_mwmed` | DOUBLE | Geração programada na hora, em MW médios |
| `geracao_verificada_mwmed` | DOUBLE | Geração verificada na hora, em MW médios |
| `capacidade_instalada_mw` | DOUBLE | Capacidade instalada, em MW |
| `fator_capacidade` | DOUBLE | Geração verificada dividida pela capacidade instalada (0 a 1) |
| `ingerido_em` | TIMESTAMP | Quando o valor foi ingerido pela última vez (UTC) |

- Os três valores de geração e capacidade chegam como texto na bronze e são convertidos; um
  texto que não é número vira nulo (e gera alerta) em vez de interromper o pipeline.
- Conjuntos **híbridos** têm uma linha eólica e uma solar por hora, distinguidas por `id_ons`.

### `previsao_programado`

Geração prevista e programada pelo ONS para cada usina eólica ou solar, a cada meia hora.

| Coluna | Tipo | Descrição |
|---|---|---|
| `data` | DATE | Dia da programação |
| `patamar` | INT | Meia hora do dia, de 1 (00:00) a 48 (23:30) |
| `inicio_patamar` | TIMESTAMP_NTZ | Início da meia hora, horário de Brasília (sem fuso) |
| `codigo_usina` | STRING | Código da usina na programação do ONS (não corresponde a CEG nem a id_ons) |
| `nome_usina` | STRING | Nome da usina na programação do ONS |
| `previsao_mwmed` | DOUBLE | Geração prevista pelo ONS para a meia hora, em MW médios |
| `programado_mwmed` | DOUBLE | Geração programada pelo ONS para a meia hora, em MW médios |
| `ingerido_em` | TIMESTAMP | Quando o valor foi ingerido pela última vez (UTC) |

- `dat_programacao` (`"20260923"`) vira `data`; o patamar ganha `inicio_patamar`.
- `codigo_usina` e `nome_usina` perdem os espaços de campo de tamanho fixo.
- `codigo_usina` não corresponde a `ceg` nem a `id_ons`: cruzar com `geracao_usina` exige uma
  tabela de correspondência.

### `clima_previsao`

Previsão horária do tempo em cada ponto de coleta: o centro das usinas eólicas e solares de cada
estado do Nordeste e as capitais (ver o [dicionário da bronze](dicionario_bronze.md)). Cada
resposta da API vira uma linha por ponto e hora, com uma coluna por variável.

| Coluna | Tipo | Descrição |
|---|---|---|
| `ponto_id` | STRING | Ponto de coleta: centro das usinas de uma fonte num estado, ou uma capital |
| `uf` | STRING | Sigla do estado do ponto |
| `tipo_ponto` | STRING | eolica, solar ou capital |
| `data_emissao` | DATE | Dia em que a previsão foi emitida |
| `data_prevista` | DATE | Dia que a previsão descreve |
| `horizonte_dias` | INT | Dias entre a emissão e o dia previsto (0 = o próprio dia) |
| `origem` | STRING | coleta_diaria ou previsoes_anteriores (reprocessamento via API de previsões anteriores) |
| `data_hora` | TIMESTAMP_NTZ | Hora local do Nordeste, UTC-3 (sem fuso) |
| `temperatura_c` | DOUBLE | Temperatura do ar a 2 m, em °C |
| `sensacao_termica_c` | DOUBLE | Sensação térmica, em °C |
| `umidade_relativa_pct` | DOUBLE | Umidade relativa a 2 m, em % |
| `ponto_orvalho_c` | DOUBLE | Ponto de orvalho a 2 m, em °C |
| `precipitacao_mm` | DOUBLE | Precipitação total na hora, em mm |
| `chuva_mm` | DOUBLE | Chuva na hora, em mm |
| `pancadas_mm` | DOUBLE | Pancadas de chuva na hora, em mm |
| `codigo_tempo` | INT | Código de condição do tempo (padrão WMO) |
| `pressao_nivel_mar_hpa` | DOUBLE | Pressão ao nível do mar, em hPa |
| `pressao_superficie_hpa` | DOUBLE | Pressão na superfície, em hPa |
| `cobertura_nuvens_pct` | DOUBLE | Cobertura total de nuvens, em % |
| `nuvens_baixas_pct` | DOUBLE | Cobertura de nuvens baixas, em % |
| `nuvens_medias_pct` | DOUBLE | Cobertura de nuvens médias, em % |
| `nuvens_altas_pct` | DOUBLE | Cobertura de nuvens altas, em % |
| `vento_velocidade_10m_kmh` | DOUBLE | Velocidade do vento a 10 m, em km/h |
| `vento_direcao_10m_graus` | DOUBLE | Direção do vento a 10 m, em graus |
| `vento_rajada_10m_kmh` | DOUBLE | Rajada de vento a 10 m, em km/h |
| `vento_velocidade_100m_kmh` | DOUBLE | Velocidade do vento a 100 m (altura aproximada do rotor), em km/h |
| `vento_direcao_100m_graus` | DOUBLE | Direção do vento a 100 m, em graus |
| `radiacao_solar_wm2` | DOUBLE | Radiação solar de onda curta, em W/m² |
| `indice_uv` | DOUBLE | Índice UV |
| `visibilidade_m` | DOUBLE | Visibilidade, em metros |
| `periodo_diurno` | BOOLEAN | Verdadeiro entre o nascer e o pôr do sol |
| `latitude_grade` | DOUBLE | Latitude do ponto de grade usado pela API (o mais próximo do pedido) |
| `longitude_grade` | DOUBLE | Longitude do ponto de grade usado pela API |
| `ingerido_em` | TIMESTAMP | Quando o valor foi ingerido pela última vez (UTC) |

- Duas origens: a **coleta diária** (emitida às 21h para o dia seguinte) e as **previsões
  anteriores** (previsões D+1 de dias passados, carregadas no reprocessamento). Nas previsões
  anteriores, `data_emissao` é a véspera de cada hora prevista.
- Até 28/09/2026 a coleta diária buscava o próprio dia (`horizonte_dias = 0`) e só em Fortaleza;
  essas linhas aparecem com `ponto_id = fortaleza`.
- Nas previsões anteriores, `chuva_mm`, `nuvens_baixas/medias/altas_pct`, `indice_uv` e
  `visibilidade_m` são nulos (a API não as fornece).

### `clima_observado`

Tempo observado hora a hora em cada ponto de coleta. Mesmas colunas de `clima_previsao`, sem as
de emissão:

| Coluna | Tipo | Descrição |
|---|---|---|
| `ponto_id` | STRING | Ponto de coleta: centro das usinas de uma fonte num estado, ou uma capital |
| `uf` | STRING | Sigla do estado do ponto |
| `tipo_ponto` | STRING | eolica, solar ou capital |
| `data_hora` | TIMESTAMP_NTZ | Hora local do Nordeste, UTC-3 (sem fuso) |
| `temperatura_c` | DOUBLE | Temperatura do ar a 2 m, em °C |
| `sensacao_termica_c` | DOUBLE | Sensação térmica, em °C |
| `umidade_relativa_pct` | DOUBLE | Umidade relativa a 2 m, em % |
| `ponto_orvalho_c` | DOUBLE | Ponto de orvalho a 2 m, em °C |
| `precipitacao_mm` | DOUBLE | Precipitação total na hora, em mm |
| `chuva_mm` | DOUBLE | Chuva na hora, em mm |
| `pancadas_mm` | DOUBLE | Pancadas de chuva na hora, em mm |
| `codigo_tempo` | INT | Código de condição do tempo (padrão WMO) |
| `pressao_nivel_mar_hpa` | DOUBLE | Pressão ao nível do mar, em hPa |
| `pressao_superficie_hpa` | DOUBLE | Pressão na superfície, em hPa |
| `cobertura_nuvens_pct` | DOUBLE | Cobertura total de nuvens, em % |
| `nuvens_baixas_pct` | DOUBLE | Cobertura de nuvens baixas, em % |
| `nuvens_medias_pct` | DOUBLE | Cobertura de nuvens médias, em % |
| `nuvens_altas_pct` | DOUBLE | Cobertura de nuvens altas, em % |
| `vento_velocidade_10m_kmh` | DOUBLE | Velocidade do vento a 10 m, em km/h |
| `vento_direcao_10m_graus` | DOUBLE | Direção do vento a 10 m, em graus |
| `vento_rajada_10m_kmh` | DOUBLE | Rajada de vento a 10 m, em km/h |
| `vento_velocidade_100m_kmh` | DOUBLE | Velocidade do vento a 100 m (altura aproximada do rotor), em km/h |
| `vento_direcao_100m_graus` | DOUBLE | Direção do vento a 100 m, em graus |
| `radiacao_solar_wm2` | DOUBLE | Radiação solar de onda curta, em W/m² |
| `indice_uv` | DOUBLE | Índice UV |
| `visibilidade_m` | DOUBLE | Visibilidade, em metros |
| `periodo_diurno` | BOOLEAN | Verdadeiro entre o nascer e o pôr do sol |
| `latitude_grade` | DOUBLE | Latitude do ponto de grade usado pela API (o mais próximo do pedido) |
| `longitude_grade` | DOUBLE | Longitude do ponto de grade usado pela API |
| `ingerido_em` | TIMESTAMP | Quando o valor foi ingerido pela última vez (UTC) |

- As janelas de coleta podem se sobrepor; cada ponto e hora aparece uma vez, com o valor da
  ingestão mais recente.
