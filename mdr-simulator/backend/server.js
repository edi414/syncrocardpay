const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3001;

app.use(cors());
app.use(express.json());

const pool = new Pool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: parseInt(process.env.DB_PORT || '5432'),
});

// Endpoint to get simulation data
app.get('/api/simulation-data', async (req, res) => {
  try {
    const query = `
      WITH ultimas_transacoes AS (
        SELECT 
          nsu_host_transacao,
          numero_parcela,
          MAX(created_at) AS max_created_at
        FROM unica_transactions.transacoes
        WHERE data_transacao >= CURRENT_DATE - INTERVAL '12 months'
        GROUP BY nsu_host_transacao, numero_parcela
      ),
      deduplicated_stats AS (
        SELECT 
          TO_CHAR(t.data_transacao, 'YYYY-MM') as mes,
          t.codigo_bandeira as bandeira,
          t.tipo_produto,
          CAST(NULLIF(t.numero_total_parcelas, '') AS INTEGER) as parcelas_num,
          CASE 
            WHEN t.numero_total_parcelas <> '0' THEN t.valor_bruto_parcela
            ELSE t.valor_bruto_venda
          END AS volume_bruto,
          CASE 
            WHEN t.numero_total_parcelas <> '0' THEN t.valor_desconto_parcela
            ELSE t.valor_desconto
          END AS mdr_cobrado
        FROM unica_transactions.transacoes t
        INNER JOIN ultimas_transacoes u
          ON t.nsu_host_transacao = u.nsu_host_transacao
          AND t.numero_parcela = u.numero_parcela
          AND t.created_at = u.max_created_at
      ),
      categorized_stats AS (
        SELECT
          mes,
          bandeira,
          CASE 
            WHEN tipo_produto = 'D' THEN 'Debit'
            WHEN tipo_produto = 'C' AND (parcelas_num <= 1 OR parcelas_num IS NULL) THEN 'Sight'
            WHEN tipo_produto = 'C' AND parcelas_num BETWEEN 2 AND 6 THEN '2-6x'
            WHEN tipo_produto = 'C' AND parcelas_num BETWEEN 7 AND 12 THEN '7-12x'
            ELSE 'Other'
          END as category,
          volume_bruto,
          mdr_cobrado
        FROM deduplicated_stats
      )
      SELECT 
        mes,
        bandeira,
        category,
        SUM(volume_bruto) as volume_bruto,
        SUM(mdr_cobrado) as mdr_cobrado
      FROM categorized_stats
      GROUP BY 1, 2, 3
      ORDER BY 1, 2, 3
    `;

    const result = await pool.query(query);
    
    // Group by month for easier frontend processing
    const groupedData = result.rows.reduce((acc, row) => {
      if (!acc[row.mes]) acc[row.mes] = [];
      acc[row.mes].push(row);
      return acc;
    }, {});

    res.json(groupedData);
  } catch (error) {
    console.error('Error fetching simulation data:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.listen(port, () => {
  console.log(`Backend server running on http://localhost:${port}`);
});
