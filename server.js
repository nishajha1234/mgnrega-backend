import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import db from './db.js';
import fetch from 'node-fetch';

dotenv.config();
const app = express();

app.use(cors());
app.use(express.json());

// 🧩 Test route
app.get('/', (req, res) => {
  res.json({ message: 'MGNREGA backend is running 🚀' });
});

// 1️⃣ Get all districts
app.get('/api/districts', (req, res) => {
  const rows = db.prepare('SELECT * FROM districts').all();
  res.json(rows);
});

// 2️⃣ Get data for a specific district (auto-fetch if missing)
app.get('/api/data/:district_code', async (req, res) => {
  try {
    const { district_code } = req.params;
    if (!district_code) return res.status(400).json({ error: 'Invalid district code' });

    // Step 1: Try reading from local DB
    let rows = db.prepare(`
      SELECT fin_year, month, payload, created_at
      FROM mgnrega_records
      WHERE district_code = ?
      ORDER BY created_at DESC
      LIMIT 24
    `).all(district_code);

    // Step 2: If no local data, fetch from data.gov.in and store
    if (!rows || rows.length === 0) {
      console.log(`⚙️ Fetching live data for district ${district_code} from data.gov.in ...`);

      const API_KEY = process.env.DATA_GOV_API_KEY;
      const STATE = process.env.STATE_NAME || 'BIHAR';
      const apiUrl = `https://api.data.gov.in/resource/ee03643a-ee4c-48c2-ac30-9f2ff26ab722?api-key=${encodeURIComponent(API_KEY)}&format=json&limit=5000&filters[state_name]=${encodeURIComponent(STATE)}&filters[district_code]=${encodeURIComponent(district_code)}`;

      const r = await fetch(apiUrl, { headers: { Accept: 'application/json' } });
      if (!r.ok) {
        console.error('❌ API fetch failed:', r.status, await r.text());
        return res.status(502).json({ error: 'Failed to fetch from data.gov.in' });
      }

      const json = await r.json();
      if (!json.records || json.records.length === 0) {
        return res.status(404).json({ error: 'No records found for this district from remote API' });
      }

      // Step 3: Insert new data into DB
      const insertStmt = db.prepare(`
        INSERT INTO mgnrega_records (fin_year, month, state_code, state_name, district_code, district_name, payload, created_at)
        VALUES (@fin_year,@month,@state_code,@state_name,@district_code,@district_name,@payload, datetime('now'))
        ON CONFLICT(district_code, fin_year, month)
        DO UPDATE SET payload = excluded.payload, created_at = datetime('now')
      `);

      const insertMany = db.transaction((records) => {
        for (const rec of records) {
          const row = {
            fin_year: rec.fin_year || null,
            month: rec.month || null,
            state_code: rec.state_code || null,
            state_name: rec.state_name || STATE,
            district_code: rec.district_code || district_code,
            district_name: rec.district_name || null,
            payload: JSON.stringify(rec)
          };
          insertStmt.run(row);
        }
      });

      insertMany(json.records);
      console.log(`✅ Saved ${json.records.length} records for ${district_code} locally.`);

      // Fetch again from DB (now cached)
      rows = db.prepare(`
        SELECT fin_year, month, payload, created_at
        FROM mgnrega_records
        WHERE district_code = ?
        ORDER BY created_at DESC
        LIMIT 24
      `).all(district_code);
    }

    // Step 4: Prepare response
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
      Avg_Days_Worked: Number(latest.Average_days_of_employment_provided_per_Household) || 0,
      Payment_within_15_days: Number(latest.percentage_payments_gererated_within_15_days) || 0
    };

    res.json({ kpis, timeseries });

  } catch (err) {
    console.error('🔥 Error in /api/data/:district_code', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// 3️⃣ Metadata route
app.get('/api/metadata', (req, res) => {
  const meta = db.prepare('SELECT * FROM metadata').all();
  res.json(meta);
});

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
