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

      const response = await fetch(apiUrl, { headers: { Accept: 'application/json' } });
      if (!response.ok) {
        console.error('❌ API fetch failed:', response.status);
        return res.status(502).json({ error: 'Failed to fetch from data.gov.in' });
      }

      const json = await response.json();
      if (!json.records || json.records.length === 0) {
        return res.status(404).json({ error: 'No records found for this district from API' });
      }

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

      // Fetch again from DB
      rows = db.prepare(`
        SELECT fin_year, month, payload, created_at
        FROM mgnrega_records
        WHERE district_code = ?
        ORDER BY created_at DESC
        LIMIT 24
      `).all(district_code);
    }

    // Step 3: Prepare response
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

    const safeNumber = (val) => {
      if (val === null || val === undefined) return 0;
      if (typeof val === 'string' && val.trim().toLowerCase() === 'na') return 0;
      const num = Number(val);
      return isNaN(num) ? 0 : num;
    };

    const kpis = {
      district_name: latest.district_name || 'Unknown',
      Total_Individuals_Worked: safeNumber(latest.Total_Individuals_Worked),
      Total_Households_Worked: safeNumber(latest.Total_Households_Worked),
      Total_Exp: safeNumber(latest.Total_Exp),
      Women_Persondays: safeNumber(latest.Women_Persondays),
      Avg_Days_Worked: safeNumber(latest.Average_days_of_employment_provided_per_Household),
      Payment_within_15_days: safeNumber(latest.percentage_payments_gererated_within_15_days)
    };

    res.json({ kpis, timeseries });

  } catch (err) {
    console.error('🔥 Error in /api/data/:district_code', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});



// 3️⃣ Optional: Get metadata
app.get('/api/metadata', (req, res) => {
  const meta = db.prepare('SELECT * FROM metadata').all();
  res.json(meta);
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
