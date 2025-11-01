// services/fetchGovData.js
const axios = require('axios');
const db = require('../db');

const API_BASE = 'https://api.data.gov.in/resource/ee03643a-ee4c-48c2-ac30-9f2ff26ab722';
const STATE = process.env.STATE_NAME || 'BIHAR';

async function fetchFromGov() {
  const params = {
    'filters[state_name]': STATE,
    format: 'json',
    limit: 5000,
    'api-key': process.env.DATA_GOV_API_KEY
  };

  try {
    const res = await axios.get(API_BASE, { params, timeout: 20000 });
    const records = res.data.records || [];

    const insertRecord = db.prepare(`
      INSERT INTO mgnrega_records (fin_year, month, state_code, state_name, district_code, district_name, payload)
      VALUES (@fin_year, @month, @state_code, @state_name, @district_code, @district_name, @payload)
      ON CONFLICT(district_code, fin_year, month)
      DO UPDATE SET payload=@payload, created_at=datetime('now')
    `);

    const insertDistrict = db.prepare(`
      INSERT INTO districts (district_code, district_name)
      VALUES (?, ?)
      ON CONFLICT(district_code) DO NOTHING
    `);

    const insertMeta = db.prepare(`
      INSERT INTO metadata (key, value)
      VALUES ('last_fetch', ?)
      ON CONFLICT(key) DO UPDATE SET value=excluded.value
    `);

    const tx = db.transaction(() => {
      for (const r of records) {
        const data = {
          fin_year: r.fin_year || null,
          month: r.month || null,
          state_code: r.state_code || null,
          state_name: r.state_name || null,
          district_code: r.district_code || null,
          district_name: r.district_name || null,
          payload: JSON.stringify(r)
        };
        insertRecord.run(data);
        if (r.district_code && r.district_name) {
          insertDistrict.run(r.district_code, r.district_name);
        }
      }
      insertMeta.run(new Date().toISOString());
    });

    tx();
    console.log(`Fetched ${records.length} records.`);
    return { success: true, count: records.length };
  } catch (err) {
    console.error('Fetch error', err.message || err);
    return { success: false, error: err.message || String(err) };
  }
}

module.exports = { fetchFromGov };
