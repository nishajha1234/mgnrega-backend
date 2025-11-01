// routes/api.js
const express = require('express');
const db = require('../db');
const router = express.Router();

router.get('/districts', (req, res) => {
  const rows = db.prepare('SELECT district_code, district_name FROM districts ORDER BY district_name').all();
  res.json(rows);
});

router.get('/data/:district_code', (req, res) => {
  const code = req.params.district_code;
  const rows = db.prepare(`
    SELECT fin_year, month, payload, created_at
    FROM mgnrega_records
    WHERE district_code = ?
    ORDER BY created_at DESC
    LIMIT 24
  `).all(code);

  if (!rows || rows.length === 0) return res.status(404).json({ error: 'No data found' });

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

  const latest = JSON.parse(rows[0].payload);
  const kpis = {
    district_name: latest.district_name,
    Total_Individuals_Worked: Number(latest.Total_Individuals_Worked) || 0,
    Total_Households_Worked: Number(latest.Total_Households_Worked) || 0,
    Total_Exp: Number(latest.Total_Exp) || 0,
    Women_Persondays: Number(latest.Women_Persondays) || 0,
    Avg_Days_Worked: Number(latest.Average_days_of_employment_provided_per_Household) || 0
  };

  res.json({ kpis, timeseries });
});

router.get('/state-comparison', (req, res) => {
  const rows = db.prepare(`
    SELECT json_extract(payload, '$.fin_year') AS fin_year,
           SUM(CAST(json_extract(payload, '$.Total_Households_Worked') AS INTEGER)) AS total_households
    FROM mgnrega_records
    GROUP BY fin_year
    ORDER BY fin_year
  `).all();
  res.json(rows);
});

module.exports = router;
