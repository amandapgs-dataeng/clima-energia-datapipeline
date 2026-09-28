-- Migração: bronze por ambiente (clima_energia_prod.bronze) -> bronze única (clima_energia_bronze)
--
-- Rodar UMA vez, logo depois do `terraform apply` que cria clima_energia_bronze
-- e ANTES da primeira execução dos jobs de ingestão no catalog novo
-- (senão o CREATE TABLE IF NOT EXISTS encontra a tabela já criada e não copia nada).

-- 1. Copiar o que já estava bruto (sem filtro de negócio) --------------------------

-- Previsão do Open-Meteo: resposta JSON completa. Não dá para buscar de novo
-- (a API devolve a previsão atual, não a que foi emitida naquele dia).
CREATE TABLE IF NOT EXISTS clima_energia_bronze.open_meteo.previsao_bruta
  DEEP CLONE clima_energia_prod.bronze.previsao_bruta;

-- Histórico observado do Open-Meteo: resposta JSON completa.
CREATE TABLE IF NOT EXISTS clima_energia_bronze.open_meteo.historico_observado
  DEEP CLONE clima_energia_prod.bronze.historico_observado;

-- Programação x previsão do ONS: o notebook 04 nunca filtrou nada.
CREATE TABLE IF NOT EXISTS clima_energia_bronze.ons.previsao_programado_eolsol
  DEEP CLONE clima_energia_prod.bronze.previsao_programado_eolsol;

-- Log de auditoria: histórico das execuções.
CREATE TABLE IF NOT EXISTS clima_energia_bronze.controle._audit_log
  DEEP CLONE clima_energia_prod.bronze._audit_log;

-- carga_energia NÃO é copiada: estava filtrada só com o Nordeste, e misturá-la com o
-- dado completo deixaria a tabela inconsistente. A próxima execução do job semanal
-- reprocessa a janela de 60 dias e repõe tudo, já completo.

-- 2. Conferência -------------------------------------------------------------------

SELECT 'previsao_bruta' AS tabela,
       (SELECT COUNT(*) FROM clima_energia_prod.bronze.previsao_bruta) AS origem,
       (SELECT COUNT(*) FROM clima_energia_bronze.open_meteo.previsao_bruta) AS destino
UNION ALL
SELECT 'historico_observado',
       (SELECT COUNT(*) FROM clima_energia_prod.bronze.historico_observado),
       (SELECT COUNT(*) FROM clima_energia_bronze.open_meteo.historico_observado)
UNION ALL
SELECT 'previsao_programado_eolsol',
       (SELECT COUNT(*) FROM clima_energia_prod.bronze.previsao_programado_eolsol),
       (SELECT COUNT(*) FROM clima_energia_bronze.ons.previsao_programado_eolsol)
UNION ALL
SELECT '_audit_log',
       (SELECT COUNT(*) FROM clima_energia_prod.bronze._audit_log),
       (SELECT COUNT(*) FROM clima_energia_bronze.controle._audit_log);

-- 3. Limpeza (só depois de conferir o passo 2 e os jobs rodarem bem no catalog novo) --

-- DROP SCHEMA clima_energia_prod.bronze CASCADE;
-- DROP SCHEMA clima_energia_dev.bronze CASCADE;
