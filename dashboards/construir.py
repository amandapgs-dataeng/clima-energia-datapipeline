"""Gera os dashboards (Databricks AI/BI) a partir de uma descrição compacta em Python.

Saída: dashboards/*.lvdash.json.tftpl, templates que o Terraform preenche (catalog e tabelas
de log de eventos dos pipelines) e publica (infra/dashboards.tf).

Uso: python dashboards/construir.py
"""
import json
from pathlib import Path

PASTA = Path(__file__).parent
LARGURA = 6  # a grade dos dashboards tem 6 colunas

GOLD = "${catalog}.gold"
SILVER = "${catalog}.silver"
AUDITORIA = "${catalog_bronze}.controle._audit_log"


def brasilia(coluna):
    """Instante UTC exibido no horário de Brasília, sem fuso na tela."""
    return f"convert_timezone('UTC', 'America/Sao_Paulo', {coluna})"


# --- Peças --------------------------------------------------------------------------------------

def texto(nome, linhas, x, y, largura, altura):
    return {"widget": {"name": nome, "multilineTextboxSpec": {"lines": linhas}},
            "position": {"x": x, "y": y, "width": largura, "height": altura}}


def _consulta(dataset, campos):
    return [{"name": "main_query", "query": {
        "datasetName": dataset,
        "fields": [{"name": c, "expression": f"`{c}`"} for c in campos],
        "disaggregated": True,
    }}]


def contador(nome, dataset, campo, titulo, x, y, largura=2, altura=3):
    return {"widget": {"name": nome, "queries": _consulta(dataset, [campo]), "spec": {
        "version": 2, "widgetType": "counter",
        "encodings": {"value": {"fieldName": campo, "displayName": titulo}},
        "frame": {"showTitle": True, "title": titulo},
    }}, "position": {"x": x, "y": y, "width": largura, "height": altura}}


def grafico(nome, tipo, dataset, titulo, x_campo, y_campo, posicao, cor=None, eixo_x="temporal",
            rotulo_x=None, rotulo_y=None, rotulo_cor=None, descricao=None):
    campos = [x_campo, y_campo] + ([cor] if cor else [])
    encodings = {
        "x": {"fieldName": x_campo, "scale": {"type": eixo_x}, "displayName": rotulo_x or x_campo},
        "y": {"fieldName": y_campo, "scale": {"type": "quantitative"}, "displayName": rotulo_y or y_campo},
    }
    if cor:
        encodings["color"] = {"fieldName": cor, "scale": {"type": "categorical"}, "displayName": rotulo_cor or cor}
    frame = {"showTitle": True, "title": titulo}
    if descricao:
        frame.update({"showDescription": True, "description": descricao})
    x, y, largura, altura = posicao
    return {"widget": {"name": nome, "queries": _consulta(dataset, campos), "spec": {
        "version": 3, "widgetType": tipo, "encodings": encodings, "frame": frame,
    }}, "position": {"x": x, "y": y, "width": largura, "height": altura}}


def tabela(nome, dataset, titulo, colunas, posicao):
    """colunas: [(campo, rótulo)]"""
    x, y, largura, altura = posicao
    return {"widget": {"name": nome, "queries": _consulta(dataset, [c for c, _ in colunas]), "spec": {
        "version": 2, "widgetType": "table",
        "encodings": {"columns": [{"fieldName": c, "displayName": r} for c, r in colunas]},
        "frame": {"showTitle": True, "title": titulo},
    }}, "position": {"x": x, "y": y, "width": largura, "height": altura}}


def pagina(nome, titulo, widgets):
    return {"name": nome, "displayName": titulo, "pageType": "PAGE_TYPE_CANVAS", "layout": widgets}


def dashboard(datasets, paginas):
    return {
        "datasets": [{"name": n, "displayName": d, "queryLines": [linha + "\n" for linha in sql.strip().splitlines()]}
                     for n, d, sql in datasets],
        "pages": paginas,
    }


# --- Dashboard de negócio -----------------------------------------------------------------------

DATASETS_NEGOCIO = [
    ("destaques", "Destaques", f"""
SELECT
  (SELECT round(sum(geracao_verificada_mwh) / sum(capacidade_mwh) * 100, 1) FROM {GOLD}.aproveitamento_mensal
    WHERE tipo_usina = 'Eólica' AND month(mes) BETWEEN 7 AND 10) AS fator_eolico_safra_pct,
  (SELECT round(sum(geracao_verificada_mwh) / sum(capacidade_mwh) * 100, 1) FROM {GOLD}.aproveitamento_mensal
    WHERE tipo_usina = 'Eólica' AND month(mes) BETWEEN 1 AND 4) AS fator_eolico_entressafra_pct,
  (SELECT round(sum(restricao_mwh) / 1e6, 1) FROM {GOLD}.restricao_geracao_diaria) AS restricao_twh,
  (SELECT round(avg(correlacao), 2) FROM {GOLD}.correlacao_clima_geracao WHERE variavel = 'vento_100m_ms') AS correlacao_vento,
  (SELECT round(avg(erro_medio_absoluto_vento_100m_kmh), 1) FROM {GOLD}.precisao_previsao_tempo) AS erro_previsao_vento_kmh
"""),
    ("aproveitamento_ne", "Aproveitamento do Nordeste por mês", f"""
SELECT mes, tipo_usina, round(sum(geracao_verificada_mwh) / sum(capacidade_mwh) * 100, 1) AS fator_pct
FROM {GOLD}.aproveitamento_mensal GROUP BY mes, tipo_usina
"""),
    ("aproveitamento_uf", "Aproveitamento por estado", f"""
SELECT uf, tipo_usina, round(sum(geracao_verificada_mwh) / sum(capacidade_mwh) * 100, 1) AS fator_pct
FROM {GOLD}.aproveitamento_mensal GROUP BY uf, tipo_usina
"""),
    ("curva_eolica", "Curva de potência eólica", f"""
SELECT faixa_inicio AS vento_ms, round(fator_capacidade * 100, 1) AS fator_pct, horas
FROM {GOLD}.curva_de_potencia WHERE tipo_usina = 'Eólica' AND horas >= 100
"""),
    ("curva_solar", "Curva de potência solar", f"""
SELECT faixa_inicio AS radiacao_wm2, round(fator_capacidade * 100, 1) AS fator_pct, horas
FROM {GOLD}.curva_de_potencia WHERE tipo_usina = 'Solar' AND horas >= 100
"""),
    ("correlacao", "Correlação clima x geração", f"""
SELECT uf, concat(tipo_usina, ': ', CASE variavel WHEN 'vento_100m_ms' THEN 'vento a 100 m'
  WHEN 'radiacao_solar_wm2' THEN 'radiação' WHEN 'cobertura_nuvens_pct' THEN 'nuvens' ELSE 'temperatura' END) AS relacao,
  round(correlacao, 2) AS correlacao
FROM {GOLD}.correlacao_clima_geracao WHERE variavel IN ('vento_100m_ms', 'radiacao_solar_wm2')
"""),
    ("restricao_mensal", "Restrição por mês", f"""
SELECT date_trunc('month', data) AS mes, round(sum(restricao_mwh) / 1000, 0) AS restricao_gwh,
  round(sum(restricao_mwh) / sum(previsto_mwh) * 100, 1) AS restricao_pct
FROM {GOLD}.restricao_geracao_diaria
WHERE data < date_trunc('month', current_date())  -- só meses completos
GROUP BY 1
"""),
    ("restricao_usinas", "Usinas mais restringidas", f"""
SELECT nome_usina, round(sum(restricao_mwh) / 1000, 1) AS restricao_gwh
FROM {GOLD}.restricao_geracao_por_usina GROUP BY nome_usina ORDER BY restricao_gwh DESC LIMIT 10
"""),
    ("precisao_programacao", "Precisão da programação do ONS", f"""
SELECT mes, tipo_usina, round(sum(erro_absoluto_mwh) / sum(geracao_verificada_mwh) * 100, 1) AS erro_pct
FROM {GOLD}.precisao_programacao_mensal GROUP BY mes, tipo_usina
"""),
    ("precisao_tempo", "Precisão da previsão do tempo", f"""
SELECT ponto_id, round(avg(erro_medio_absoluto_vento_100m_kmh), 1) AS erro_vento_kmh,
  round(avg(vento_100m_observado_kmh), 1) AS vento_medio_kmh
FROM {GOLD}.precisao_previsao_tempo GROUP BY ponto_id
"""),
    ("carga_temperatura", "Carga x temperatura", f"""
SELECT temperatura_media_c, round(carga_mwmed / 1000, 2) AS carga_gwmed,
  CASE WHEN fim_de_semana THEN 'Fim de semana' ELSE 'Dia útil' END AS tipo_dia
FROM {GOLD}.carga_x_temperatura
"""),
]

PAGINAS_NEGOCIO = [
    pagina("visao_geral", "Visão geral", [
        texto("titulo", [
            "# Vento e sol no Nordeste: quanto do potencial renovável vira energia, e por quê",
            "Dados do ONS (geração, capacidade, programação, carga) e do Open-Meteo (clima no local das usinas), "
            "out/2024 a set/2026. Pipeline em Azure Databricks: bronze → silver → gold.",
        ], 0, 0, LARGURA, 2),
        contador("c_safra", "destaques", "fator_eolico_safra_pct", "Fator eólico na safra dos ventos (jul–out), %", 0, 2),
        contador("c_entressafra", "destaques", "fator_eolico_entressafra_pct", "Fator eólico na entressafra (jan–abr), %", 2, 2),
        contador("c_restricao", "destaques", "restricao_twh", "Geração prevista e não programada (abr–set/2026), TWh", 4, 2),
        contador("c_correlacao", "destaques", "correlacao_vento", "Correlação vento a 100 m × geração eólica", 0, 5, 3),
        contador("c_erro_tempo", "destaques", "erro_previsao_vento_kmh", "Erro da previsão do vento (D+1), km/h", 3, 5, 3),
    ]),
    pagina("aproveitamento", "1. Aproveitamento", [
        texto("t_aprov", ["## Quanto da capacidade instalada vira energia?",
                          "Fator de capacidade = energia gerada / energia que a capacidade instalada geraria a plena carga."],
              0, 0, LARGURA, 1),
        grafico("g_aprov_mes", "line", "aproveitamento_ne", "Fator de capacidade no Nordeste, por mês (%)",
                "mes", "fator_pct", (0, 1, LARGURA, 6), cor="tipo_usina", rotulo_x="Mês", rotulo_y="Fator (%)", rotulo_cor="Fonte",
                descricao="A safra dos ventos (jul–out) quase dobra o aproveitamento eólico."),
        grafico("g_aprov_uf", "bar", "aproveitamento_uf", "Fator de capacidade médio por estado (%)",
                "uf", "fator_pct", (0, 7, LARGURA, 6), cor="tipo_usina", eixo_x="categorical",
                rotulo_x="Estado", rotulo_y="Fator (%)", rotulo_cor="Fonte"),
    ]),
    pagina("clima", "2. Clima × geração", [
        texto("t_clima", ["## O clima no local das usinas explica a geração?",
                          "Clima observado no centro das usinas de cada estado e fonte, ponderado pela capacidade instalada."],
              0, 0, LARGURA, 1),
        grafico("g_curva_eolica", "line", "curva_eolica", "Curva de potência eólica: fator (%) por vento a 100 m (m/s)",
                "vento_ms", "fator_pct", (0, 1, 3, 6), eixo_x="quantitative", rotulo_x="Vento a 100 m (m/s)", rotulo_y="Fator (%)"),
        grafico("g_curva_solar", "line", "curva_solar", "Curva solar: fator (%) por radiação (W/m²)",
                "radiacao_wm2", "fator_pct", (3, 1, 3, 6), eixo_x="quantitative", rotulo_x="Radiação (W/m²)", rotulo_y="Fator (%)"),
        grafico("g_correlacao", "bar", "correlacao", "Correlação entre o clima e a geração, por estado",
                "uf", "correlacao", (0, 7, LARGURA, 6), cor="relacao", eixo_x="categorical",
                rotulo_x="Estado", rotulo_y="Correlação (−1 a 1)", rotulo_cor="Relação"),
    ]),
    pagina("restricao", "3. Restrição", [
        texto("t_restr", ["## Quanto da geração prevista o ONS deixou de programar?",
                          "Restrição = previsto − programado, quando positivo. Brasil: 91% da capacidade eólica está no Nordeste; "
                          "na solar, cerca de metade. Indicador calculado, não a medição oficial de cortes."],
              0, 0, LARGURA, 2),
        grafico("g_restr_gwh", "bar", "restricao_mensal", "Geração prevista e não programada, por mês (GWh)",
                "mes", "restricao_gwh", (0, 2, 3, 6), rotulo_x="Mês", rotulo_y="GWh"),
        grafico("g_restr_pct", "line", "restricao_mensal", "Restrição em relação ao previsto (%)",
                "mes", "restricao_pct", (3, 2, 3, 6), rotulo_x="Mês", rotulo_y="%"),
        grafico("g_restr_usinas", "bar", "restricao_usinas", "As 10 usinas com mais geração não programada (GWh)",
                "nome_usina", "restricao_gwh", (0, 8, LARGURA, 6), eixo_x="categorical", rotulo_x="Usina", rotulo_y="GWh"),
    ]),
    pagina("precisao", "4. Precisão das previsões", [
        texto("t_prec", ["## O planejamento acerta?",
                         "Programação do ONS: erro hora a hora, usina a usina, em relação à geração verificada. "
                         "Previsão do tempo: erro da previsão feita na véspera (D+1) em relação ao observado."],
              0, 0, LARGURA, 1),
        grafico("g_prec_prog", "line", "precisao_programacao", "Erro da programação do ONS (% da geração verificada)",
                "mes", "erro_pct", (0, 1, LARGURA, 6), cor="tipo_usina", rotulo_x="Mês", rotulo_y="Erro (%)", rotulo_cor="Fonte"),
        grafico("g_prec_tempo", "bar", "precisao_tempo", "Erro médio da previsão D+1 do vento a 100 m, por ponto (km/h)",
                "ponto_id", "erro_vento_kmh", (0, 7, LARGURA, 6), eixo_x="categorical", rotulo_x="Ponto", rotulo_y="km/h"),
    ]),
    pagina("carga", "Bônus: carga × temperatura", [
        texto("t_carga", ["## O consumo do Nordeste sobe nos dias mais quentes?",
                          "Temperatura média das capitais (Fortaleza, Recife, Salvador). Associação, não causalidade: "
                          "estação do ano e calendário também influenciam a carga."],
              0, 0, LARGURA, 1),
        grafico("g_carga", "scatter", "carga_temperatura", "Carga do Nordeste (GW médios) × temperatura média (°C)",
                "temperatura_media_c", "carga_gwmed", (0, 1, LARGURA, 8), cor="tipo_dia", eixo_x="quantitative",
                rotulo_x="Temperatura média (°C)", rotulo_y="Carga (GW médios)", rotulo_cor="Dia"),
    ]),
]


# --- Painel de monitoramento --------------------------------------------------------------------

DATASETS_MONITORAMENTO = [
    ("resumo", "Resumo dos últimos 30 dias", f"""
SELECT
  (SELECT count(*) FROM {AUDITORIA} WHERE execution_timestamp >= current_timestamp() - INTERVAL 30 DAYS) AS execucoes_ingestao,
  (SELECT count(*) FROM {AUDITORIA} WHERE status = 'failed' AND execution_timestamp >= current_timestamp() - INTERVAL 30 DAYS) AS falhas_ingestao,
  (SELECT sum(n) FROM (
     SELECT count(*) AS n FROM {SILVER}.carga_energia_quarentena UNION ALL
     SELECT count(*) FROM {SILVER}.geracao_usina_quarentena UNION ALL
     SELECT count(*) FROM {SILVER}.fator_capacidade_quarentena UNION ALL
     SELECT count(*) FROM {SILVER}.previsao_programado_quarentena UNION ALL
     SELECT count(*) FROM {SILVER}.clima_previsao_quarentena UNION ALL
     SELECT count(*) FROM {SILVER}.clima_observado_quarentena)) AS registros_em_quarentena
"""),
    ("ingestao_diaria", "Linhas ingeridas por dia", f"""
SELECT to_date({brasilia('execution_timestamp')}) AS dia, pipeline_name AS notebook, sum(linhas_gravadas) AS linhas
FROM {AUDITORIA} WHERE status = 'success' AND execution_timestamp >= current_timestamp() - INTERVAL 30 DAYS
GROUP BY 1, 2
"""),
    ("ultimas_execucoes", "Últimas execuções da ingestão", f"""
SELECT {brasilia('execution_timestamp')} AS inicio_brasilia, pipeline_name AS notebook, status, linhas_gravadas,
  duracao_segundos, erro
FROM {AUDITORIA} ORDER BY execution_timestamp DESC LIMIT 50
"""),
    ("atualizacoes_pipelines", "Atualizações dos pipelines", f"""
WITH eventos AS (
  SELECT 'silver' AS camada, origin.update_id, timestamp, details:update_progress.state::string AS estado
  FROM {SILVER}.${{event_log_silver}} WHERE event_type = 'update_progress'
  UNION ALL
  SELECT 'gold', origin.update_id, timestamp, details:update_progress.state::string
  FROM ${{catalog}}.gold.${{event_log_gold}} WHERE event_type = 'update_progress')
SELECT camada, {brasilia('min(timestamp)')} AS inicio_brasilia,
  round((unix_millis(max(timestamp)) - unix_millis(min(timestamp))) / 60000, 1) AS duracao_min,
  max_by(estado, timestamp) AS estado
FROM eventos WHERE timestamp >= current_timestamp() - INTERVAL 30 DAYS GROUP BY camada, update_id
"""),
    ("qualidade", "Regras de qualidade", f"""
SELECT replace(e.dataset, '_validada', '') AS tabela, e.name AS regra,
  sum(e.passed_records) AS aprovados, sum(e.failed_records) AS reprovados
FROM {SILVER}.${{event_log_silver}}
LATERAL VIEW explode(from_json(details:flow_progress.data_quality.expectations::string,
  'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
WHERE event_type = 'flow_progress' AND e.dataset LIKE '%_validada'
GROUP BY 1, 2
"""),
]

PAGINAS_MONITORAMENTO = [
    pagina("operacao", "Operação", [
        texto("titulo_mon", ["# Monitoramento do pipeline",
                             "Últimos 30 dias, horário de Brasília. Ingestão: log de auditoria da bronze. "
                             "Silver e gold: log de eventos dos pipelines."], 0, 0, LARGURA, 1),
        contador("c_exec", "resumo", "execucoes_ingestao", "Execuções da ingestão", 0, 1),
        contador("c_falhas", "resumo", "falhas_ingestao", "Falhas da ingestão", 2, 1),
        contador("c_quarentena", "resumo", "registros_em_quarentena", "Registros em quarentena (silver)", 4, 1),
        grafico("g_ingestao", "bar", "ingestao_diaria", "Linhas gravadas na bronze por dia e notebook",
                "dia", "linhas", (0, 4, LARGURA, 6), cor="notebook", rotulo_x="Dia", rotulo_y="Linhas", rotulo_cor="Notebook"),
        grafico("g_pipelines", "scatter", "atualizacoes_pipelines", "Duração das atualizações da silver e da gold (min)",
                "inicio_brasilia", "duracao_min", (0, 10, LARGURA, 6), cor="camada",
                rotulo_x="Início", rotulo_y="Minutos", rotulo_cor="Camada"),
        tabela("t_execucoes", "ultimas_execucoes", "Últimas execuções da ingestão", [
            ("inicio_brasilia", "Início (Brasília)"), ("notebook", "Notebook"), ("status", "Status"),
            ("linhas_gravadas", "Linhas"), ("duracao_segundos", "Duração (s)"), ("erro", "Erro"),
        ], (0, 16, LARGURA, 8)),
    ]),
    pagina("qualidade_pagina", "Qualidade", [
        texto("t_qual", ["## Regras de qualidade da silver",
                         "Registros avaliados por regra. Reprovados em regras de descarte vão para as tabelas x_quarentena."],
              0, 0, LARGURA, 1),
        grafico("g_reprovados", "bar", "qualidade", "Registros reprovados por regra",
                "regra", "reprovados", (0, 1, LARGURA, 6), cor="tabela", eixo_x="categorical",
                rotulo_x="Regra", rotulo_y="Reprovados", rotulo_cor="Tabela"),
        tabela("t_qualidade", "qualidade", "Avaliação por tabela e regra", [
            ("tabela", "Tabela"), ("regra", "Regra"), ("aprovados", "Aprovados"), ("reprovados", "Reprovados"),
        ], (0, 7, LARGURA, 8)),
    ]),
]


def gravar(nome, conteudo):
    caminho = PASTA / f"{nome}.lvdash.json.tftpl"
    caminho.write_text(json.dumps(conteudo, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return caminho


if __name__ == "__main__":
    print(gravar("negocio", dashboard(DATASETS_NEGOCIO, PAGINAS_NEGOCIO)))
    print(gravar("monitoramento", dashboard(DATASETS_MONITORAMENTO, PAGINAS_MONITORAMENTO)))
