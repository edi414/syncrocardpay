import pandas as pd
from datetime import datetime
from utils.logger import setup_logger

logger = setup_logger("reading_tricard")


def parse_tricard_amount(value):
    """Parse 9(13)V99 format: last 2 digits are decimals"""
    try:
        return int(value) / 100
    except (ValueError, TypeError):
        return 0.0


def parse_tricard_date(value):
    """Parse DDMMAAAA date format to date object"""
    try:
        value = value.strip()
        if not value or value == '00000000':
            return None
        return datetime.strptime(value, '%d%m%Y').date()
    except (ValueError, TypeError):
        return None


class ExtratoTricard:
    def __init__(self, file_path, file_type):
        """
        file_type: 'VENDA', 'FINANCEIRO', 'SALDO'
        """
        self.file_path = file_path
        self.file_type = file_type
        self.data = None

    def load_file(self):
        with open(self.file_path, 'r', encoding='latin-1') as f:
            self.data = f.readlines()

    def parse_header(self):
        """Parse header record (002/030/060)"""
        if not self.data:
            return None
        header = self.data[0].rstrip('\n').rstrip('\r')
        tipo = header[0:3]

        if self.file_type == 'VENDA' and tipo == '002':
            return {
                'tipo_registro': tipo,
                'data_emissao': header[3:11],
                'literal': header[11:19].strip(),
                'nome_comercial': header[49:71].strip(),
                'seq_movimento': header[71:77],
                'pv_grupo': header[77:86],
                'tipo_processamento': header[86:101].strip(),
                'versao': header[101:121].strip(),
            }
        elif self.file_type == 'FINANCEIRO' and tipo == '030':
            return {
                'tipo_registro': tipo,
                'data_emissao': header[3:11],
                'literal': header[11:19].strip(),
                'nome_comercial': header[53:75].strip(),
                'seq_movimento': header[75:81],
                'pv_grupo': header[81:90],
                'tipo_processamento': header[90:105].strip(),
                'versao': header[105:125].strip(),
            }
        elif self.file_type == 'SALDO' and tipo == '060':
            return {
                'tipo_registro': tipo,
                'data_emissao': header[3:11],
                'literal': header[11:19].strip(),
                'nome_comercial': header[59:81].strip(),
                'seq_movimento': header[81:87],
                'pv_grupo': header[87:96],
                'tipo_processamento': header[96:111].strip(),
            }
        else:
            logger.error(f"Header inválido para tipo {self.file_type}: registro '{tipo}'")
            return None

    def parse_venda_records(self):
        """Parse reg 008 (rotativo) and 012 (parcelado) from VENDA file"""
        records = []
        for line in self.data:
            line = line.rstrip('\n').rstrip('\r')
            tipo = line[0:3]

            if tipo == '008':
                records.append({
                    'tipo_registro': '008',
                    'numero_pv': line[3:12].strip(),
                    'numero_rv': line[12:21].strip(),
                    'data_venda': parse_tricard_date(line[21:29]),
                    'numero_cv_nsu': line[86:98].strip(),
                    'numero_cartao': line[67:83].strip(),
                    'valor_bruto': parse_tricard_amount(line[37:52]),
                    'valor_gorjeta': parse_tricard_amount(line[52:67]),
                    'valor_desconto': parse_tricard_amount(line[111:126]),
                    'valor_liquido': parse_tricard_amount(line[203:218]),
                    'nr_autorizacao': line[126:132].strip(),
                    'hora_transacao': line[132:138].strip(),
                    'tipo_captura': line[202:203].strip(),
                    'nr_terminal': line[218:226].strip(),
                    'sigla_pais': line[226:229].strip(),
                    'numero_parcelas': 1,
                    'numero_referencia': line[98:111].strip(),
                })

            elif tipo == '012':
                num_parcelas = int(line[86:88]) if line[86:88].strip() else 1
                records.append({
                    'tipo_registro': '012',
                    'numero_pv': line[3:12].strip(),
                    'numero_rv': line[12:21].strip(),
                    'data_venda': parse_tricard_date(line[21:29]),
                    'numero_cv_nsu': line[88:100].strip(),
                    'numero_cartao': line[67:83].strip(),
                    'valor_bruto': parse_tricard_amount(line[37:52]),
                    'valor_gorjeta': parse_tricard_amount(line[52:67]),
                    'valor_desconto': parse_tricard_amount(line[113:128]),
                    'valor_liquido': parse_tricard_amount(line[205:220]),
                    'nr_autorizacao': line[128:134].strip(),
                    'hora_transacao': line[134:140].strip(),
                    'tipo_captura': line[204:205].strip(),
                    'nr_terminal': line[250:258].strip(),
                    'sigla_pais': line[258:261].strip(),
                    'numero_parcelas': num_parcelas,
                    'numero_referencia': line[100:113].strip(),
                })

        return records

    def parse_financeiro_records(self):
        """Parse reg 034 (créditos), 035 (ajustes), 036 (antecipações) from FINANCEIRO file"""
        records = []
        for line in self.data:
            line = line.rstrip('\n').rstrip('\r')
            tipo = line[0:3]

            if tipo == '034':
                records.append({
                    'tipo_registro': '034',
                    'numero_pv': line[3:12].strip(),
                    'numero_documento': line[12:23].strip(),
                    'data_lancamento': parse_tricard_date(line[23:31]),
                    'valor_lancamento': parse_tricard_amount(line[31:46]),
                    'indicador_cd': line[46:47].strip(),
                    'banco': line[47:50].strip(),
                    'agencia': line[50:56].strip(),
                    'conta_corrente': line[56:67].strip(),
                    'numero_rv': line[75:84].strip(),
                    'data_transacao_original': parse_tricard_date(line[84:92]),
                    'tipo_transacao': line[93:94].strip(),
                    'valor_bruto_rv': parse_tricard_amount(line[94:109]),
                    'valor_taxa_desconto': parse_tricard_amount(line[109:124]),
                    'parcela_total': line[124:129].strip(),
                    'status_credito': line[129:131].strip(),
                    'pv_original': line[131:140].strip(),
                    'motivo_ajuste': None,
                    'numero_cartao': None,
                    'valor_credito_original': None,
                    'data_vencimento_original': None,
                })

            elif tipo == '035':
                records.append({
                    'tipo_registro': '035',
                    'numero_pv': line[3:12].strip(),
                    'numero_documento': line[12:21].strip(),
                    'data_lancamento': parse_tricard_date(line[21:29]),
                    'valor_lancamento': parse_tricard_amount(line[29:44]),
                    'indicador_cd': line[44:45].strip(),
                    'banco': None,
                    'agencia': None,
                    'conta_corrente': None,
                    'numero_rv': line[99:108].strip() if len(line) > 108 else None,
                    'data_transacao_original': parse_tricard_date(line[91:99]) if len(line) > 99 else None,
                    'tipo_transacao': None,
                    'valor_bruto_rv': None,
                    'valor_taxa_desconto': None,
                    'parcela_total': None,
                    'status_credito': None,
                    'pv_original': line[137:146].strip() if len(line) > 146 else None,
                    'motivo_ajuste': line[47:75].strip() if len(line) > 75 else None,
                    'numero_cartao': line[75:91].strip() if len(line) > 91 else None,
                    'valor_credito_original': None,
                    'data_vencimento_original': None,
                })

            elif tipo == '036':
                records.append({
                    'tipo_registro': '036',
                    'numero_pv': line[3:12].strip(),
                    'numero_documento': line[12:23].strip(),
                    'data_lancamento': parse_tricard_date(line[23:31]),
                    'valor_lancamento': parse_tricard_amount(line[31:46]),
                    'indicador_cd': line[46:47].strip(),
                    'banco': line[47:50].strip(),
                    'agencia': line[50:56].strip(),
                    'conta_corrente': line[56:67].strip(),
                    'numero_rv': line[67:76].strip(),
                    'data_transacao_original': parse_tricard_date(line[76:84]),
                    'tipo_transacao': None,
                    'valor_bruto_rv': parse_tricard_amount(line[112:127]),
                    'valor_taxa_desconto': parse_tricard_amount(line[127:142]),
                    'parcela_total': line[107:112].strip(),
                    'status_credito': None,
                    'pv_original': line[142:151].strip(),
                    'motivo_ajuste': None,
                    'numero_cartao': None,
                    'valor_credito_original': parse_tricard_amount(line[84:99]),
                    'data_vencimento_original': parse_tricard_date(line[99:107]),
                })

        return records

    def parse_saldo_records(self):
        """Parse reg 062 (saldos em aberto) from SALDO file"""
        records = []
        for line in self.data:
            line = line.rstrip('\n').rstrip('\r')
            tipo = line[0:3]

            if tipo == '062':
                records.append({
                    'numero_oc': line[3:18].strip(),
                    'tipo_transacao': line[18:19].strip(),
                    'banco': line[19:22].strip(),
                    'agencia': line[22:31].strip(),
                    'conta_corrente': line[31:42].strip(),
                    'data_vencimento': parse_tricard_date(line[42:50]),
                    'numero_ec': line[50:59].strip(),
                    'valor_bruto': parse_tricard_amount(line[90:105]),
                    'valor_desconto': parse_tricard_amount(line[105:120]),
                    'valor_gorjeta': parse_tricard_amount(line[120:135]),
                    'valor_liquido': parse_tricard_amount(line[135:150]),
                    'numero_pv': line[150:159].strip(),
                    'numero_parcela': int(line[159:161]) if line[159:161].strip() else None,
                })

        return records

    def process_file(self):
        """Load and parse file, returning a DataFrame"""
        self.load_file()

        if not self.data:
            logger.error(f"Arquivo vazio: {self.file_path}")
            return None, None

        header = self.parse_header()
        if not header:
            return None, None

        if self.file_type == 'VENDA':
            records = self.parse_venda_records()
        elif self.file_type == 'FINANCEIRO':
            records = self.parse_financeiro_records()
        elif self.file_type == 'SALDO':
            records = self.parse_saldo_records()
        else:
            logger.error(f"Tipo de arquivo desconhecido: {self.file_type}")
            return None, None

        if not records:
            logger.info(f"Nenhum registro de detalhe encontrado em {self.file_path}")
            return header, pd.DataFrame()

        df = pd.DataFrame(records)
        logger.info(f"Parsed {len(df)} registros de {self.file_path}")
        return header, df
