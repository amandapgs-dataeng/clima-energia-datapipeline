"""Tratamento específico de cada fonte (o que o contrato genérico não cobre)."""
import json
from datetime import date, datetime, timedelta, timezone

from utilities.carga_energia import REGRAS_DESCARTE as DESCARTE_CARGA, padronizar_carga_energia
from utilities.clima import VARIAVEIS, padronizar_clima_observado, padronizar_clima_previsao
from utilities.fator_capacidade import padronizar_fator_capacidade
from utilities.geracao_usina import padronizar_geracao_usina
from utilities.previsao_programado import padronizar_previsao_programado

INGESTAO = datetime(2026, 9, 28, 14, 43, tzinfo=timezone.utc)


def ons(*args):
    """Horário como o ONS grava na bronze: o relógio de Brasília rotulado como UTC."""
    return datetime(*args, tzinfo=timezone.utc)


def filtrar(df, regras):
    for expressao in regras.values():
        df = df.filter(expressao)
    return df


class TestCargaEnergia:
    def test_meia_noite_rotulada_utc_vira_o_proprio_dia(self, bronze):
        linha = padronizar_carga_energia(bronze("ons.carga_energia", [("NE", "Nordeste", ons(2026, 9, 1), 1.0, INGESTAO)])).first()
        assert (linha.data, linha.subsistema) == (date(2026, 9, 1), "Nordeste")

    def test_descarta_registros_que_quebram_a_estrutura(self, bronze):
        df = padronizar_carga_energia(bronze("ons.carga_energia", [
            ("NE", "Nordeste", ons(2026, 9, 1), 13928.0, INGESTAO),
            (None, None, ons(2026, 9, 1), 100.0, INGESTAO),
            ("XX", "?", ons(2026, 9, 1), 100.0, INGESTAO),
            ("S", "Sul", ons(2026, 9, 1), None, INGESTAO),
        ]))
        assert [(r.id_subsistema, r.carga_mwmed) for r in filtrar(df, DESCARTE_CARGA).collect()] == [("NE", 13928.0)]


class TestGeracaoUsina:
    def linha(self, ceg, nome, estado="MA"):
        return (ons(2026, 8, 1, 13, 0), "NE", "NORDESTE", estado, "MARANHAO", "TIPO I", "EOLIELÉTRICA", None,
                nome, None, ceg, 100.5, INGESTAO)

    def test_conjuntos_com_ceg_marcador_tem_chaves_distintas(self, bronze):
        linhas = padronizar_geracao_usina(bronze("ons.geracao_usina", [
            self.linha("-", "CONJUNTO EÓLICO A"), self.linha("-", "CONJUNTO EÓLICO B"),
        ])).collect()
        assert {l.chave_usina for l in linhas} == {"- | CONJUNTO EÓLICO A", "- | CONJUNTO EÓLICO B"}
        assert {l.ceg for l in linhas} == {None}

    def test_usinas_que_compartilham_ceg_tem_chaves_distintas(self, bronze):
        linhas = padronizar_geracao_usina(bronze("ons.geracao_usina", [
            self.linha("UHE.PH.PA.030354-2.01", "BELO MONTE"), self.linha("UHE.PH.PA.030354-2.01", "UHE PIMENTAL"),
        ])).collect()
        assert len({l.chave_usina for l in linhas}) == 2

    def test_vocabulario_e_hora_local(self, bronze):
        l = padronizar_geracao_usina(bronze("ons.geracao_usina", [self.linha("EOL.CV.MA.1", "USINA X")])).first()
        assert (l.tipo_usina, l.modalidade_operacao, l.subsistema, l.uf) == ("Eólica", "Tipo I", "Nordeste", "MA")
        assert l.data_hora == datetime(2026, 8, 1, 13, 0)


class TestFatorCapacidade:
    def linha(self, id_ons, tipo, verificada="132.025", capacidade="426.000"):
        return ("NE", "NORDESTE", "BA", "Bahia", "BAOUR-500-A", "Ourolândia", "BA", -10.9, -41.0, -10.8, -41.1,
                "Conjunto de Usinas", tipo, "Conj. Babilônia Centro", id_ons, "-", ons(2026, 8, 15, 12, 0),
                "149.500", verificada, capacidade, 0.31, INGESTAO)

    def test_valores_em_texto_viram_numero(self, bronze):
        l = padronizar_fator_capacidade(bronze("ons.fator_capacidade", [self.linha("BAUBC", "Eólica")])).first()
        assert (l.geracao_programada_mwmed, l.geracao_verificada_mwmed, l.capacidade_instalada_mw) == (149.5, 132.025, 426.0)

    def test_texto_nao_numerico_vira_nulo_em_vez_de_quebrar(self, bronze):
        l = padronizar_fator_capacidade(bronze("ons.fator_capacidade", [self.linha("BAUBC", "Eólica", verificada="n/d")])).first()
        assert l.geracao_verificada_mwmed is None

    def test_conjunto_hibrido_tem_uma_linha_por_tipo(self, bronze):
        linhas = padronizar_fator_capacidade(bronze("ons.fator_capacidade", [
            self.linha("BAUBCE", "Eólica"), self.linha("BAUBCS", "Solar"),
        ])).collect()
        assert {(l.id_ons, l.tipo_usina) for l in linhas} == {("BAUBCE", "Eólica"), ("BAUBCS", "Solar")}
        assert {l.ceg for l in linhas} == {None}


class TestPrevisaoProgramado:
    def test_data_patamar_e_espacos(self, bronze):
        linhas = padronizar_previsao_programado(bronze("ons.previsao_programado_eolsol", [
            ("20260923", 1, "I4ALBA      ", "Albatroz                      ", "0.00", "12.50", INGESTAO),
            ("20260923", 48, "I4ALBA      ", "Albatroz                      ", "10.00", "9.00", INGESTAO),
        ])).orderBy("patamar").collect()
        assert linhas[0].data == date(2026, 9, 23)
        assert (linhas[0].inicio_patamar, linhas[1].inicio_patamar) == (datetime(2026, 9, 23, 0, 0), datetime(2026, 9, 23, 23, 30))
        assert (linhas[0].codigo_usina, linhas[0].nome_usina) == ("I4ALBA", "Albatroz")
        assert (linhas[0].previsao_mwmed, linhas[0].programado_mwmed) == (0.0, 12.5)


def resposta(dia, horas=24, sufixo=""):
    inicio = datetime.fromisoformat(dia)
    tempos = [(inicio + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M") for h in range(horas)]
    hourly = {"time": tempos}
    for i, (api, _, _, _) in enumerate(VARIAVEIS):
        hourly[f"{api}{sufixo}"] = [float(i)] * horas
    hourly[f"is_day{sufixo}"] = [1.0 if 6 <= h % 24 < 18 else 0.0 for h in range(horas)]
    return json.dumps({"latitude": -11.6, "longitude": -41.7, "timezone": "America/Fortaleza", "hourly": hourly})


PONTO_BA = ("ba-eolica", "BA", "eolica", -11.644, -41.736)


def coluna(nome):
    return [n for _, n, _, _ in VARIAVEIS].index(nome)


class TestClima:
    def previsao(self, bronze, diaria=(), anteriores=()):
        return padronizar_clima_previsao(
            bronze("open_meteo.previsao_bruta", list(diaria)),
            bronze("open_meteo.previsao_historica_bruta", list(anteriores)),
        )

    def test_coleta_diaria_vira_uma_linha_por_hora_e_ponto(self, bronze):
        linhas = self.previsao(bronze, diaria=[
            (*PONTO_BA, "2026-09-28", "2026-09-29", 1, resposta("2026-09-29"), INGESTAO),
        ]).orderBy("data_hora").collect()
        assert len(linhas) == 24
        l = linhas[0]
        assert (l.ponto_id, l.uf, l.tipo_ponto, l.origem) == ("ba-eolica", "BA", "eolica", "coleta_diaria")
        assert (l.data_emissao, l.data_prevista, l.horizonte_dias) == (date(2026, 9, 28), date(2026, 9, 29), 1)
        assert l.data_hora == datetime(2026, 9, 29, 0, 0)
        assert l.vento_velocidade_100m_kmh == float(coluna("vento_velocidade_100m_kmh"))
        assert (l.latitude_grade, l.longitude_grade) == (-11.6, -41.7)

    def test_previsoes_anteriores_emitidas_na_vespera_de_cada_hora(self, bronze):
        linhas = self.previsao(bronze, anteriores=[
            (*PONTO_BA, "2024-10-15", "2024-10-16", 1, resposta("2024-10-15", horas=48, sufixo="_previous_day1"), INGESTAO),
        ]).orderBy("data_hora").collect()
        assert len(linhas) == 48
        assert (linhas[0].data_prevista, linhas[0].data_emissao) == (date(2024, 10, 15), date(2024, 10, 14))
        assert (linhas[-1].data_prevista, linhas[-1].data_emissao) == (date(2024, 10, 16), date(2024, 10, 15))
        assert {(l.origem, l.horizonte_dias) for l in linhas} == {("previsoes_anteriores", 1)}
        assert linhas[0].temperatura_c == float(coluna("temperatura_c"))

    def test_periodo_diurno_vira_booleano(self, bronze):
        linhas = self.previsao(bronze, diaria=[
            (*PONTO_BA, "2026-09-28", "2026-09-29", 1, resposta("2026-09-29"), INGESTAO),
        ]).orderBy("data_hora").collect()
        assert (linhas[3].periodo_diurno, linhas[12].periodo_diurno) == (False, True)

    def test_coleta_antiga_era_de_fortaleza_e_do_proprio_dia(self, bronze):
        l = self.previsao(bronze, diaria=[
            (None, None, None, -3.72, -38.54, "2026-09-24", None, None, resposta("2026-09-24"), INGESTAO),
        ]).first()
        assert (l.ponto_id, l.uf, l.tipo_ponto) == ("fortaleza", "CE", "capital")
        assert (l.data_prevista, l.horizonte_dias) == (date(2026, 9, 24), 0)

    def test_bronze_antiga_sem_as_colunas_novas(self, spark, bronze):
        # Antes de 28/09/2026 a bronze nem tinha as colunas de ponto e de data prevista.
        antiga = spark.createDataFrame(
            [("2026-09-24", resposta("2026-09-24"), INGESTAO)], "data_referencia string, raw_response string, ingestion_timestamp timestamp"
        )
        l = padronizar_clima_previsao(antiga, bronze("open_meteo.previsao_historica_bruta", [])).first()
        assert (l.ponto_id, l.data_prevista, l.horizonte_dias) == ("fortaleza", date(2026, 9, 24), 0)

    def test_observado_vira_uma_linha_por_hora_e_ponto(self, bronze):
        df = padronizar_clima_observado(bronze("open_meteo.historico_observado", [
            (*PONTO_BA, "2026-09-11", "2026-09-17", resposta("2026-09-11", horas=168), INGESTAO),
            ("recife", "PE", "capital", -8.05, -34.9, "2026-09-11", "2026-09-17", resposta("2026-09-11", horas=168), INGESTAO),
        ]))
        assert df.count() == 2 * 168
        assert df.select("ponto_id").distinct().count() == 2
        assert df.agg({"data_hora": "max"}).first()[0] == datetime(2026, 9, 17, 23, 0)
