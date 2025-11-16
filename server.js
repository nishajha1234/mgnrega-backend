import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import fetch from "node-fetch";
import db from "./db.js";

dotenv.config();
const app = express();

app.use(cors());
app.use(express.json());

// ✅ Root route
app.get("/", (req, res) => {
  res.json({ message: "MGNREGA backend is running 🚀 (real live data)" });
});

// ✅ Get all districts
app.get("/api/districts", (req, res) => {
  const rows = db.prepare("SELECT * FROM districts").all();
  res.json(rows);
});

// ✅ District-wise data (fetches live + caches)
app.get("/api/data/:district_code", async (req, res) => {
  try {
    const { district_code } = req.params;
    if (!district_code)
      return res.status(400).json({ error: "Invalid district code" });

    let rows = db
      .prepare(
        `SELECT fin_year, month, payload, created_at
         FROM mgnrega_records
         WHERE district_code = ?
         ORDER BY created_at DESC
         LIMIT 24`
      )
      .all(district_code);

    // Fetch if not in DB
    if (!rows || rows.length === 0) {
      console.log(`⚙️ Fetching live data for district ${district_code}...`);
      const API_KEY = process.env.DATA_GOV_API_KEY;
      const RESOURCE_ID = "ee03643a-ee4c-48c2-ac30-9f2ff26ab722";

      const apiUrl = `https://api.data.gov.in/resource/${RESOURCE_ID}?api-key=${encodeURIComponent(
        API_KEY
      )}&format=json&limit=10000`;

      const response = await fetch(apiUrl);
      const json = await response.json();

      if (!json.records || json.records.length === 0)
        return res
          .status(404)
          .json({ error: "No records found for this district" });

      const insertStmt = db.prepare(`
        INSERT INTO mgnrega_records (
          fin_year, month, state_code, state_name,
          district_code, district_name, payload, created_at
        )
        VALUES (
          @fin_year, @month, @state_code, @state_name,
          @district_code, @district_name, @payload, datetime('now')
        )
        ON CONFLICT(district_code, fin_year, month)
        DO UPDATE SET payload = excluded.payload, created_at = datetime('now')
      `);

      const insertMany = db.transaction((records) => {
        for (const rec of records) {
          insertStmt.run({
            fin_year: rec.fin_year || null,
            month: rec.month || null,
            state_code: rec.state_code || null,
            state_name: rec.state_name || null,
            district_code: rec.district_code || null,
            district_name: rec.district_name || null,
            payload: JSON.stringify(rec),
          });
        }
      });

      insertMany(json.records);
      console.log(`✅ Saved ${json.records.length} records locally.`);

      rows = db
        .prepare(
          `SELECT fin_year, month, payload, created_at
           FROM mgnrega_records
           WHERE district_code = ?
           ORDER BY created_at DESC
           LIMIT 24`
        )
        .all(district_code);
    }

const timeseries = rows.reverse().map((r) => {
  const p = JSON.parse(r.payload);
  return {
    fin_year: p.fin_year,
    month: p.month,
    persondays: Number(p.Persondays_of_Central_Liability_so_far) || 0,
    Total_Households_Worked: Number(p.Total_Households_Worked) || 0,
    Total_Individuals_Worked: Number(p.Total_Individuals_Worked) || 0,
    expenditure: Number(p.Total_Exp) || 0,
  };
});




    const latest = JSON.parse(rows[0].payload);
    const safeNumber = (v) =>
      isNaN(Number(v)) || v === null || v === undefined ? 0 : Number(v);

    const kpis = {
      district_name: latest.district_name || "Unknown",
      Total_Individuals_Worked: safeNumber(latest.Total_Individuals_Worked),
      Total_Households_Worked: safeNumber(latest.Total_Households_Worked),
      Total_Exp: safeNumber(latest.Total_Exp),
      Women_Persondays: safeNumber(latest.Women_Persondays),
      Avg_Days_Worked: safeNumber(
        latest.Average_days_of_employment_provided_per_Household
      ),
      Payment_within_15_days: safeNumber(
        latest.percentage_payments_gererated_within_15_days
      ),
    };

    res.json({ kpis, timeseries });
  } catch (err) {
    console.error("🔥 Error in /api/data/:district_code", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// ✅ State-level, multi-year real MGNREGA data
app.get("/api/state/:state_name", async (req, res) => {
  const { state_name } = req.params;
  const { years } = req.query;
  if (!years)
    return res.status(400).json({ error: "Please specify years in query" });

  const API_KEY = process.env.DATA_GOV_API_KEY;
  const RESOURCE_ID = "ee03643a-ee4c-48c2-ac30-9f2ff26ab722";
  const yearList = years.split(",");
  let resultData = {};

  try {
    for (const year of yearList) {
      const url = `https://api.data.gov.in/resource/${RESOURCE_ID}?api-key=${API_KEY}&format=json&limit=10000&filters[state_name]=${encodeURIComponent(
        state_name
      )}&filters[fin_year]=${encodeURIComponent(year)}`;

      const response = await fetch(url);
      const json = await response.json();
      if (!json.records || json.records.length === 0) continue;

     const formatted = json.records.map((item) => ({
  month: item.month || "Unknown",
  expenditure: Number(item.Total_Exp) || 0,
  persondays: (Number(item.SC_persondays) || 0) +
    (Number(item.ST_persondays) || 0) +
    (Number(item.Women_Persondays) || 0) +
    (Number(item.Persondays_of_Central_Liability_so_far) || 0),
}));


      resultData[year] = formatted;
    }

    if (Object.keys(resultData).length === 0)
      return res
        .status(404)
        .json({ error: "No MGNREGA data found for that state/year range" });

    res.json({ state_name: state_name.toUpperCase(), data: resultData });
  } catch (err) {
    console.error("🔥 Error in /api/state/:state_name", err);
    res.status(500).json({ error: "Failed to fetch state-level data" });
  }
});

// ✅ Get available states and years dynamically
app.get("/api/availability", async (req, res) => {
  try {
    const API_KEY = process.env.DATA_GOV_API_KEY;
    const RESOURCE_ID = "ee03643a-ee4c-48c2-ac30-9f2ff26ab722";
    const url = `https://api.data.gov.in/resource/${RESOURCE_ID}?api-key=${API_KEY}&format=json&limit=1000`;

    const response = await fetch(url);
    const json = await response.json();

    const states = new Set();
    const years = new Set();

    (json.records || []).forEach((r) => {
      if (r.state_name) states.add(r.state_name.trim());
      if (r.fin_year) years.add(r.fin_year.trim());
    });

    res.json({
      states: Array.from(states).sort(),
      years: Array.from(years).sort().reverse(),
    });
  } catch (err) {
    console.error("🔥 Error in /api/availability", err);
    res.status(500).json({ error: "Failed to fetch availability" });
  }
});

// ✅ Metadata route
app.get("/api/metadata", (req, res) => {
  const meta = db.prepare("SELECT * FROM metadata").all();
  res.json(meta);
});

// ✅ Start server
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
