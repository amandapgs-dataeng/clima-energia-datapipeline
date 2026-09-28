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
        linha = padronizar_carga_energia(bronze("carga_energia", [("NE", "Nordeste", ons(2026, 9, 1), 1.0, INGESTAO)])).first()
        assert (linha.data, linha.subsistema) == (date(2026, 9, 1), "Nordeste")

    def test_descarta_registros_que_quebram_a_estrutura(self, bronze):
        df = padronizar_carga_energia(bronze("carga_energia", [
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
        linhas = padronizar_geracao_usina(bronze("geracao_usina", [
            self.linha("-", "CONJUNTO EÓLICO A"), self.linha("-", "CONJUNTO EÓLICO B"),
        ])).collect()
        assert {l.chave_usina for l in linhas} == {"- | CONJUNTO EÓLICO A", "- | CONJUNTO EÓLICO B"}
        assert {l.ceg for l in linhas} == {None}

    def test_usinas_que_compartilham_ceg_tem_chaves_distintas(self, bronze):
        linhas = padronizar_geracao_usina(bronze("geracao_usina", [
            self.linha("UHE.PH.PA.030354-2.01", "BELO MONTE"), self.linha("UHE.PH.PA.030354-2.01", "UHE PIMENTAL"),
        ])).collect()
        assert len({l.chave_usina for l in linhas}) == 2

    def test_vocabulario_e_hora_local(self, bronze):
        l = padronizar_geracao_usina(bronze("geracao_usina", [self.linha("EOL.CV.MA.1", "USINA X")])).first()
        assert (l.tipo_usina, l.modalidade_operacao, l.subsistema, l.uf) == ("Eólica", "Tipo I", "Nordeste", "MA")
        assert l.data_hora == datetime(2026, 8, 1, 13, 0)


class TestFatorCapacidade:
    def linha(self, id_ons, tipo, verificada="132.025", capacidade="426.000"):
        return ("NE", "NORDESTE", "BA", "Bahia", "BAOUR-500-A", "Ourolândia", "BA", -10.9, -41.0, -10.8, -41.1,
                "Conjunto de Usinas", tipo, "Conj. Babilônia Centro", id_ons, "-", ons(2026, 8, 15, 12, 0),
                "149.500", verificada, capacidade, 0.31, INGESTAO)

    def test_valores_em_texto_viram_numero(self, bronze):
        l = padronizar_fator_capacidade(bronze("fator_capacidade", [self.linha("BAUBC", "Eólica")])).first()
        assert (l.geracao_programada_mwmed, l.geracao_verificada_mwmed, l.capacidade_instalada_mw) == (149.5, 132.025, 426.0)

    def test_texto_nao_numerico_vira_nulo_em_vez_de_quebrar(self, bronze):
        l = padronizar_fator_capacidade(bronze("fator_capacidade", [self.linha("BAUBC", "Eólica", verificada="n/d")])).first()
        assert l.geracao_verificada_mwmed is None

    def test_conjunto_hibrido_tem_uma_linha_por_tipo(self, bronze):
        linhas = padronizar_fator_capacidade(bronze("fator_capacidade", [
            self.linha("BAUBCE", "Eólica"), self.linha("BAUBCS", "Solar"),
        ])).collect()
        assert {(l.id_ons, l.tipo_usina) for l in linhas} == {("BAUBCE", "Eólica"), ("BAUBCS", "Solar")}
        assert {l.ceg for l in linhas} == {None}


class TestPrevisaoProgramado:
    def test_data_patamar_e_espacos(self, bronze):
        linhas = padronizar_previsao_programado(bronze("previsao_programado", [
            ("20260923", 1, "I4ALBA      ", "Albatroz                      ", "0.00", "12.50", INGESTAO),
            ("20260923", 48, "I4ALBA      ", "Albatroz                      ", "10.00", "9.00", INGESTAO),
        ])).orderBy("patamar").collect()
        assert linhas[0].data == date(2026, 9, 23)
        assert (linhas[0].inicio_patamar, linhas[1].inicio_patamar) == (datetime(2026, 9, 23, 0, 0), datetime(2026, 9, 23, 23, 30))
        assert (linhas[0].codigo_usina, linhas[0].nome_usina) == ("I4ALBA", "Albatroz")
        assert (linhas[0].previsao_mwmed, linhas[0].programado_mwmed) == (0.0, 12.5)


def resposta(dia, horas=24):
    inicio = datetime.fromisoformat(dia)
    tempos = [(inicio + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M") for h in range(horas)]
    hourly = {"time": tempos}
    for i, (api, _, _, _) in enumerate(VARIAVEIS):
        hourly[api] = [float(i)] * horas
    hourly["is_day"] = [1.0 if 6 <= h % 24 < 18 else 0.0 for h in range(horas)]
    return json.dumps({"latitude": -3.76, "longitude": -38.53, "timezone": "America/Fortaleza", "hourly": hourly})


class TestClima:
    def test_previsao_vira_uma_linha_por_hora(self, bronze):
        df = padronizar_clima_previsao(bronze("clima_previsao", [
            ("2026-09-28", "2026-09-29", 1, -3.72, -38.54, resposta("2026-09-29"), INGESTAO),
        ]))
        linhas = df.orderBy("data_hora").collect()
        assert len(linhas) == 24
        primeira = linhas[0]
        assert (primeira.data_emissao, primeira.data_prevista, primeira.horizonte_dias) == (date(2026, 9, 28), date(2026, 9, 29), 1)
        assert primeira.data_hora == datetime(2026, 9, 29, 0, 0)
        assert (primeira.temperatura_c, primeira.umidade_relativa_pct) == (0.0, 2.0)  # posição de cada variável
        assert (primeira.latitude_grade, primeira.longitude_grade) == (-3.76, -38.53)

    def test_periodo_diurno_vira_booleano(self, bronze):
        linhas = padronizar_clima_previsao(bronze("clima_previsao", [
            ("2026-09-28", "2026-09-29", 1, -3.72, -38.54, resposta("2026-09-29"), INGESTAO),
        ])).orderBy("data_hora").collect()
        assert (linhas[3].periodo_diurno, linhas[12].periodo_diurno) == (False, True)

    def test_previsao_antiga_sem_data_prevista_previa_o_proprio_dia(self, bronze):
        linha = padronizar_clima_previsao(bronze("clima_previsao", [
            ("2026-09-24", None, None, -3.72, -38.54, resposta("2026-09-24"), INGESTAO),
        ])).first()
        assert (linha.data_prevista, linha.horizonte_dias) == (date(2026, 9, 24), 0)

    def test_previsao_de_bronze_sem_as_colunas_novas(self, spark):
        # Antes da mudança de 28/09/2026 a bronze nem tinha data_prevista/horizonte_dias.
        antiga = spark.createDataFrame(
            [("2026-09-24", resposta("2026-09-24"), INGESTAO)], "data_referencia string, raw_response string, ingestion_timestamp timestamp"
        )
        linha = padronizar_clima_previsao(antiga).first()
        assert (linha.data_prevista, linha.horizonte_dias) == (date(2026, 9, 24), 0)

    def test_observado_vira_uma_linha_por_hora(self, bronze):
        df = padronizar_clima_observado(bronze("clima_observado", [
            ("2026-09-11", "2026-09-17", -3.72, -38.54, resposta("2026-09-11", horas=168), INGESTAO),
        ]))
        assert df.count() == 168
        assert df.agg({"data_hora": "max"}).first()[0] == datetime(2026, 9, 17, 23, 0)
