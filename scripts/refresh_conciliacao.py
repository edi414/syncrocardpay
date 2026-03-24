"""
Refresh da conciliacao.
Popula: deposito_diario, conciliacao_master.
Chamado pelo main.py apos processamento de arquivos.
"""

import logging
import psycopg2
from datetime import datetime

logger = logging.getLogger("refresh_conciliacao")


def build_deposito_diario(conn):
    """Agrega transacoes Liquidacao Normal por data e popula deposito_diario."""
    query = """
    WITH latest_per_parcel AS (
        SELECT DISTINCT ON (nsu_host_transacao, numero_parcela)
            nsu_host_transacao,
            numero_parcela,
            numero_total_parcelas,
            data_lancamento::date AS data_lancamento,
            valor_liquido_parcela,
            valor_liquido_venda,
            tipo_lancamento
        FROM unica_transactions.transacoes
        ORDER BY nsu_host_transacao, numero_parcela, created_at DESC
    ),
    aggregated AS (
        SELECT
            data_lancamento AS data_liquidacao,
            SUM(
                CASE
                    WHEN numero_total_parcelas NOT IN ('0', '1', '') THEN valor_liquido_parcela
                    ELSE valor_liquido_venda
                END
            ) AS total_liquido_esperado,
            COUNT(*) AS qtd_parcelas
        FROM latest_per_parcel
        WHERE tipo_lancamento = 'Liquidação Normal'
        GROUP BY data_lancamento
    )
    INSERT INTO unica_transactions.deposito_diario
        (data_liquidacao, total_liquido_esperado, qtd_parcelas)
    SELECT data_liquidacao, total_liquido_esperado, qtd_parcelas
    FROM aggregated
    ON CONFLICT (data_liquidacao) DO UPDATE SET
        total_liquido_esperado = EXCLUDED.total_liquido_esperado,
        qtd_parcelas           = EXCLUDED.qtd_parcelas,
        updated_at             = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(query)
        count = cur.rowcount
    logger.info(f"deposito_diario: {count} datas atualizadas")
    return count


def match_deposito_extrato(conn):
    """Faz match entre deposito_diario e extrato_juridica (Triangulo)."""
    query_match = """
    UPDATE unica_transactions.deposito_diario dd
    SET
        extrato_id    = sub.extrato_id,
        valor_extrato = sub.valor_extrato,
        diferenca     = dd.total_liquido_esperado - sub.valor_extrato,
        match_status  = CASE
            WHEN ABS(dd.total_liquido_esperado - sub.valor_extrato) < 0.02 THEN 'ok'
            ELSE 'divergente'
        END,
        matched_at = NOW(),
        updated_at = NOW()
    FROM (
        SELECT
            e.datalancamento::date AS data,
            MIN(e.id)              AS extrato_id,
            SUM(e.valorlancamento) AS valor_extrato
        FROM public.extrato_juridica e
        WHERE e.textodescricaohistorico LIKE '%%Triangulo%%'
          AND e.textodescricaohistorico NOT LIKE '%%Antecipação%%'
        GROUP BY e.datalancamento::date
    ) sub
    WHERE dd.data_liquidacao = sub.data
    """
    with conn.cursor() as cur:
        cur.execute(query_match)
        matched = cur.rowcount

    query_no_match = """
    UPDATE unica_transactions.deposito_diario
    SET match_status = 'sem_extrato', updated_at = NOW()
    WHERE extrato_id IS NULL AND match_status = 'pendente'
    """
    with conn.cursor() as cur:
        cur.execute(query_no_match)
        no_match = cur.rowcount

    logger.info(f"deposito_diario match: {matched} matched, {no_match} sem extrato")
    return matched


def refresh_conciliacao_master(conn):
    """Popula conciliacao_master a partir de transacoes + antecipacao_override."""

    query_portal = """
    INSERT INTO unica_transactions.conciliacao_master
        (nsu, parcela, data_venda, valor_bruto_venda, valor_liquido,
         numero_total_parcelas, portal_last_status, portal_last_status_at,
         data_liquidacao_portal, last_status)
    SELECT
        t.nsu_host_transacao,
        t.numero_parcela,
        t.data_transacao::date,
        t.valor_bruto_venda,
        CASE
            WHEN t.numero_total_parcelas NOT IN ('0', '1', '') THEN t.valor_liquido_parcela
            ELSE t.valor_liquido_venda
        END,
        t.numero_total_parcelas,
        t.tipo_lancamento,
        t.created_at,
        CASE
            WHEN t.tipo_lancamento = 'Liquidação Normal' THEN t.data_lancamento::date
            ELSE NULL
        END,
        t.tipo_lancamento
    FROM (
        SELECT DISTINCT ON (nsu_host_transacao, numero_parcela) *
        FROM unica_transactions.transacoes
        ORDER BY nsu_host_transacao, numero_parcela, created_at DESC
    ) t
    ON CONFLICT (nsu, parcela) DO UPDATE SET
        data_venda              = EXCLUDED.data_venda,
        valor_bruto_venda       = EXCLUDED.valor_bruto_venda,
        valor_liquido           = EXCLUDED.valor_liquido,
        numero_total_parcelas   = EXCLUDED.numero_total_parcelas,
        portal_last_status      = EXCLUDED.portal_last_status,
        portal_last_status_at   = EXCLUDED.portal_last_status_at,
        data_liquidacao_portal  = EXCLUDED.data_liquidacao_portal,
        last_status             = EXCLUDED.portal_last_status,
        antecipado              = FALSE,
        data_antecipacao        = NULL,
        valor_antecipado        = NULL,
        updated_at              = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(query_portal)
        portal_count = cur.rowcount
    logger.info(f"conciliacao_master portal: {portal_count} registros")

    query_previsao = """
    UPDATE unica_transactions.conciliacao_master cm
    SET data_prevista_pagamento = prev.data_prevista,
        updated_at = NOW()
    FROM (
        SELECT DISTINCT ON (nsu_host_transacao, numero_parcela)
            nsu_host_transacao,
            numero_parcela,
            data_lancamento::date AS data_prevista
        FROM unica_transactions.transacoes
        WHERE tipo_lancamento = 'Previsão'
        ORDER BY nsu_host_transacao, numero_parcela, created_at DESC
    ) prev
    WHERE cm.nsu = prev.nsu_host_transacao
      AND cm.parcela = prev.numero_parcela
    """
    with conn.cursor() as cur:
        cur.execute(query_previsao)
        previsao_count = cur.rowcount
    logger.info(f"conciliacao_master data_prevista: {previsao_count} atualizados")

    query_override = """
    UPDATE unica_transactions.conciliacao_master cm
    SET
        antecipado       = TRUE,
        data_antecipacao = ao.data_antecipacao,
        valor_antecipado = ao.valor_antecipado,
        last_status      = 'Antecipado',
        updated_at       = NOW()
    FROM (
        SELECT DISTINCT ON (nsu, parcela)
            nsu, parcela, data_antecipacao, valor_antecipado
        FROM unica_transactions.antecipacao_override
        ORDER BY nsu, parcela, data_antecipacao DESC
    ) ao
    WHERE cm.nsu = ao.nsu
      AND cm.parcela = ao.parcela
    """
    with conn.cursor() as cur:
        cur.execute(query_override)
        override_count = cur.rowcount
    logger.info(f"conciliacao_master overrides antecipacao: {override_count}")

    query_link = """
    UPDATE unica_transactions.conciliacao_master cm
    SET
        deposito_diario_id = dd.id,
        updated_at         = NOW()
    FROM unica_transactions.deposito_diario dd
    WHERE cm.data_liquidacao_portal = dd.data_liquidacao
      AND cm.portal_last_status = 'Liquidação Normal'
    """
    with conn.cursor() as cur:
        cur.execute(query_link)

    query_remessa = """
    UPDATE unica_transactions.conciliacao_master
    SET
        remessa_bb = CASE
            WHEN antecipado = TRUE THEN data_antecipacao
            WHEN portal_last_status = 'Liquidação Normal' THEN data_liquidacao_portal
            ELSE NULL
        END,
        updated_at = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(query_remessa)

    return portal_count, override_count


def full_refresh(connection_params):
    """Pipeline completo de refresh. Recebe dict com host, user, password, database, port."""
    start = datetime.now()
    logger.info("Iniciando refresh conciliacao...")

    conn = psycopg2.connect(
        host=connection_params['host'],
        port=connection_params['port'],
        user=connection_params['user'],
        password=connection_params['password'],
        database=connection_params['database']
    )

    try:
        conn.autocommit = False

        depositos = build_deposito_diario(conn)
        matched = match_deposito_extrato(conn)
        portal_count, override_count = refresh_conciliacao_master(conn)

        conn.commit()

        elapsed = (datetime.now() - start).total_seconds()
        result = {
            "status": "ok",
            "depositos": depositos,
            "matched": matched,
            "portal": portal_count,
            "overrides": override_count,
            "elapsed_seconds": elapsed
        }
        logger.info(f"Refresh completo em {elapsed:.1f}s: {result}")
        return result

    except Exception as e:
        conn.rollback()
        logger.error(f"Erro no refresh: {e}")
        raise
    finally:
        conn.close()
