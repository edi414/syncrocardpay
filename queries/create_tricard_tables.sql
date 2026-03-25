-- Tabelas para integração TRICARD (VENDA, FINANCEIRO, SALDO)
-- Todas no schema unica_transactions, usando controle_arquivos para controle de arquivos

-- 1. Tricard Vendas (registros 008 e 012)
CREATE TABLE IF NOT EXISTS unica_transactions.tricard_vendas (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    tipo_registro varchar(3) NOT NULL,
    numero_pv varchar(9) NOT NULL,
    numero_rv varchar(9) NOT NULL,
    data_venda date NOT NULL,
    numero_cv_nsu varchar(12),
    numero_cartao varchar(16),
    valor_bruto decimal(15,2) NOT NULL,
    valor_gorjeta decimal(15,2) DEFAULT 0,
    valor_desconto decimal(15,2) DEFAULT 0,
    valor_liquido decimal(15,2) NOT NULL,
    nr_autorizacao varchar(12),
    hora_transacao varchar(6),
    tipo_captura varchar(1),
    nr_terminal varchar(8),
    sigla_pais varchar(3),
    numero_parcelas int DEFAULT 1,
    numero_referencia varchar(13),
    file_id uuid NOT NULL REFERENCES unica_transactions.controle_arquivos(id),
    created_at timestamp DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tricard_vendas_file ON unica_transactions.tricard_vendas(file_id);
CREATE INDEX IF NOT EXISTS idx_tricard_vendas_nsu ON unica_transactions.tricard_vendas(numero_cv_nsu);
CREATE INDEX IF NOT EXISTS idx_tricard_vendas_data ON unica_transactions.tricard_vendas(data_venda);

-- 2. Tricard Financeiro (registros 034, 035, 036)
CREATE TABLE IF NOT EXISTS unica_transactions.tricard_financeiro (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    tipo_registro varchar(3) NOT NULL,
    numero_pv varchar(9) NOT NULL,
    numero_documento varchar(15),
    data_lancamento date NOT NULL,
    valor_lancamento decimal(15,2) NOT NULL,
    indicador_cd varchar(1),
    banco varchar(3),
    agencia varchar(6),
    conta_corrente varchar(11),
    numero_rv varchar(9),
    data_transacao_original date,
    tipo_transacao varchar(2),
    valor_bruto_rv decimal(15,2),
    valor_taxa_desconto decimal(15,2),
    parcela_total varchar(5),
    status_credito varchar(2),
    pv_original varchar(9),
    motivo_ajuste varchar(30),
    numero_cartao varchar(16),
    valor_credito_original decimal(15,2),
    data_vencimento_original date,
    file_id uuid NOT NULL REFERENCES unica_transactions.controle_arquivos(id),
    created_at timestamp DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tricard_fin_file ON unica_transactions.tricard_financeiro(file_id);
CREATE INDEX IF NOT EXISTS idx_tricard_fin_data ON unica_transactions.tricard_financeiro(data_lancamento);

-- 3. Tricard Saldos (registro 062)
CREATE TABLE IF NOT EXISTS unica_transactions.tricard_saldos (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    numero_oc varchar(15) NOT NULL,
    tipo_transacao varchar(1),
    banco varchar(3),
    agencia varchar(9),
    conta_corrente varchar(11),
    data_vencimento date NOT NULL,
    numero_ec varchar(9),
    valor_bruto decimal(15,2) NOT NULL,
    valor_desconto decimal(15,2) DEFAULT 0,
    valor_gorjeta decimal(15,2) DEFAULT 0,
    valor_liquido decimal(15,2) NOT NULL,
    numero_pv varchar(9),
    numero_parcela int,
    file_id uuid NOT NULL REFERENCES unica_transactions.controle_arquivos(id),
    created_at timestamp DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tricard_saldos_file ON unica_transactions.tricard_saldos(file_id);
CREATE INDEX IF NOT EXISTS idx_tricard_saldos_vencimento ON unica_transactions.tricard_saldos(data_vencimento);
