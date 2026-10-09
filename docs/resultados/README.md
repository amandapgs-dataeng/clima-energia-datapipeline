# Resultados (gold)

Exportação das tabelas da camada gold de produção em 09/10/2026, antes da desativação da
infraestrutura no Azure. Período: 10/2024 a 09/2026 (programação do ONS: 04/2026 a 10/2026).

| Arquivo | Conteúdo |
|---|---|
| `aproveitamento_mensal.csv` | Fator de capacidade mensal das usinas eólicas e solares do Nordeste, por estado |
| `curva_de_potencia.csv` | Fator de capacidade por faixa de vento a 100 m (eólica) e de radiação (solar) |
| `correlacao_clima_geracao.csv` | Correlação entre variáveis climáticas e a geração, por estado e fonte |
| `restricao_geracao_diaria.csv` | Geração prevista x programada pelo ONS, por dia |
| `restricao_geracao_por_usina.csv` | Geração prevista x programada pelo ONS, por usina e mês |
| `precisao_programacao_mensal.csv` | Erro e viés da programação do ONS, por mês, estado e fonte |
| `precisao_previsao_tempo.csv` | Erro da previsão do tempo D+1, por mês e ponto |
| `carga_x_temperatura.csv` | Carga diária do Nordeste com a temperatura das capitais |

As colunas estão descritas no [dicionário da gold](../dicionario_gold.md). A tabela horária
`clima_x_geracao_horaria` (cerca de 200 mil linhas) não foi exportada; as tabelas de curva de
potência e de correlação resumem o que ela mostra.
