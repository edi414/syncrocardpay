import React, { useState, useEffect, useMemo } from 'react';
import axios from 'axios';
import { 
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer 
} from 'recharts';
import { Calculator, TrendingDown, HelpCircle, DollarSign } from 'lucide-react';
import './App.css';

interface TransactionRow {
  mes: string;
  bandeira: string;
  category: string;
  volume_bruto: string;
  mdr_cobrado: string;
}

interface Rates {
  [bandeira: string]: {
    [category: string]: number;
  };
}

const App: React.FC = () => {
  const [data, setData] = useState<Record<string, TransactionRow[]>>({});
  const [rates, setRates] = useState<Rates>({});
  const [monthlyFee, setMonthlyFee] = useState<number>(0);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await axios.get('http://localhost:3001/api/simulation-data');
        setData(response.data);
        
        // Initialize rates for all flags and categories found
        const initialRates: Rates = {};
        const allFlags = new Set<string>();
        const categories = ['Debit', 'Sight', '2-6x', '7-12x'];

        Object.values(response.data as Record<string, TransactionRow[]>).forEach(rows => {
          rows.forEach(row => allFlags.add(row.bandeira));
        });

        allFlags.forEach(flag => {
          initialRates[flag] = {};
          categories.forEach(cat => {
            // Default rates
            if (cat === 'Debit') initialRates[flag][cat] = 1.2;
            else if (cat === 'Sight') initialRates[flag][cat] = 1.8;
            else if (cat === '2-6x') initialRates[flag][cat] = 2.5;
            else if (cat === '7-12x') initialRates[flag][cat] = 3.5;
          });
        });
        setRates(initialRates);
      } catch (error) {
        console.error('Error fetching data:', error);
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, []);

  const handleRateChange = (flag: string, category: string, value: string) => {
    const numValue = parseFloat(value) || 0;
    setRates(prev => ({
      ...prev,
      [flag]: {
        ...prev[flag],
        [category]: numValue
      }
    }));
  };

  const chartData = useMemo(() => {
    return Object.entries(data).map(([mes, rows]) => {
      const historicalMdr = rows.reduce((sum, row) => sum + parseFloat(row.mdr_cobrado), 0);
      
      const simulatedMdr = rows.reduce((sum, row) => {
        const rate = (rates[row.bandeira] && rates[row.bandeira][row.category]) || 0;
        return sum + (parseFloat(row.volume_bruto) * (rate / 100));
      }, 0) + monthlyFee;

      return {
        mes,
        'MDR Atual': parseFloat(historicalMdr.toFixed(2)),
        'Simulação': parseFloat(simulatedMdr.toFixed(2))
      };
    }).sort((a, b) => a.mes.localeCompare(b.mes));
  }, [data, rates, monthlyFee]);

  const totals = useMemo(() => {
    const totalActual = chartData.reduce((sum, d) => sum + d['MDR Atual'], 0);
    const totalSimulated = chartData.reduce((sum, d) => sum + d['Simulação'], 0);
    const diff = totalSimulated - totalActual;
    const diffPercent = (diff / totalActual) * 100;

    return { totalActual, totalSimulated, diff, diffPercent };
  }, [chartData]);

  if (loading) return <div className="loading">Carregando dados do banco...</div>;

  return (
    <div className="app-container">
      <header>
        <h1>Simulador de Taxas MDR</h1>
        <p>Compare suas taxas atuais com novos cenários de mercado</p>
      </header>

      <div className="stats-summary">
        <div className="stat-card">
          <div className="label">Total Pago (12m)</div>
          <div className="value">R$ {totals.totalActual.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</div>
        </div>
        <div className="stat-card simulated">
          <div className="label">Total Simulado (12m)</div>
          <div className="value">R$ {totals.totalSimulated.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</div>
        </div>
        <div className="stat-card">
          <div className="label">Diferença Anual</div>
          <div className={`value ${totals.diff > 0 ? 'danger' : 'success'}`}>
            {totals.diff > 0 ? '+' : ''} R$ {totals.diff.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
          </div>
          <div className={`diff ${totals.diff > 0 ? 'positive' : 'negative'}`}>
            ({totals.diffPercent.toFixed(1)}%)
          </div>
        </div>
      </div>

      <div className="main-grid">
        <div className="card">
          <h2><Calculator size={20} /> Configurações de Taxas</h2>
          
          <div className="input-group">
            <label>Mensalidade de Maquinetas</label>
            <div className="input-wrapper">
              <input 
                type="number" 
                value={monthlyFee} 
                onChange={(e) => setMonthlyFee(parseFloat(e.target.value) || 0)} 
              />
              <span className="suffix">R$ /mês</span>
            </div>
          </div>

          {Object.keys(rates).map(flag => (
            <div key={flag} className="flag-section">
              <div className="flag-title">{flag}</div>
              {['Debit', 'Sight', '2-6x', '7-12x'].map(cat => (
                <div key={cat} className="input-group">
                  <label>{cat === 'Debit' ? 'Débito' : cat === 'Sight' ? 'Crédito à Vista' : `Parcelado ${cat}`}</label>
                  <div className="input-wrapper">
                    <input 
                      type="number" 
                      step="0.01"
                      value={rates[flag][cat]} 
                      onChange={(e) => handleRateChange(flag, cat, e.target.value)} 
                    />
                    <span className="suffix">%</span>
                  </div>
                </div>
              ))}
            </div>
          ))}
        </div>

        <div className="card">
          <h2><TrendingDown size={20} /> Comparativo de Custo MDR</h2>
          <div className="chart-container">
            <ResponsiveContainer width="100%" height={450}>
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" vertical={false} />
                <XAxis 
                  dataKey="mes" 
                  stroke="#94a3b8" 
                  fontSize={12} 
                  tickLine={false} 
                  axisLine={false} 
                />
                <YAxis 
                  stroke="#94a3b8" 
                  fontSize={12} 
                  tickLine={false} 
                  axisLine={false} 
                  tickFormatter={(value) => `R$ ${value >= 1000 ? (value/1000).toFixed(0) + 'k' : value}`}
                />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '8px' }}
                  itemStyle={{ fontSize: '14px' }}
                />
                <Legend />
                <Line 
                  type="monotone" 
                  dataKey="MDR Atual" 
                  stroke="#94a3b8" 
                  strokeWidth={2} 
                  dot={{ r: 4, fill: '#94a3b8' }} 
                  activeDot={{ r: 6 }}
                />
                <Line 
                  type="monotone" 
                  dataKey="Simulação" 
                  stroke="#818cf8" 
                  strokeWidth={3} 
                  dot={{ r: 4, fill: '#818cf8' }} 
                  activeDot={{ r: 6 }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
};

export default App;
