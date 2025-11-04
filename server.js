import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import db from './db.js';

dotenv.config();
const app = express();

app.use(cors());
app.use(express.json());

// Test route
app.get('/', (req, res) => {
  res.json({ message: 'MGNREGA backend is running 🚀' });
});

// 1️⃣ Get all districts
app.get('/api/districts', (req, res) => {
  const rows = db.prepare('SELECT * FROM districts').all();
  res.json(rows);
});

// 2️⃣ Get records for a specific district (by district_code)
app.get('/api/data/:district_code', (req, res) => {
  const { district_code } = req.params;

  const rows = db.prepare(`
    SELECT fin_year, month, payload, created_at
    FROM mgnrega_records
    WHERE district_code = ?
    ORDER BY created_at DESC
    LIMIT 24
  `).all(district_code);

  if (!rows || rows.length === 0)
    return res.status(404).json({ error: 'No data found' });

  // Parse each record’s payload
  const timeseries = rows.reverse().map(r => {
    const p = JSON.parse(r.payload);
    return {
      fin_year: p.fin_year,
      month: p.month,
      Total_Households_Worked: Number(p.Total_Households_Worked) || 0,
      Total_Individuals_Worked: Number(p.Total_Individuals_Worked) || 0,
      expenditure: Number(p.Total_Exp) || 0
    };
  });

  // Extract KPIs from the latest record
  const latest = JSON.parse(rows[0].payload);

  const kpis = {
    district_name: latest.district_name,
    Total_Individuals_Worked: Number(latest.Total_Individuals_Worked) || 0,
    Total_Households_Worked: Number(latest.Total_Households_Worked) || 0,
    Total_Exp: Number(latest.Total_Exp) || 0,
    Women_Persondays: Number(latest.Women_Persondays) || 0,
    Avg_Days_Worked: Number(latest.Average_days_of_employment_provided_per_Household) || 0,
    Payment_within_15_days: Number(latest.percentage_payments_gererated_within_15_days) || 0
  };

  res.json({ kpis, timeseries });
});



// 3️⃣ Optional: Get metadata
app.get('/api/metadata', (req, res) => {
  const meta = db.prepare('SELECT * FROM metadata').all();
  res.json(meta);
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
