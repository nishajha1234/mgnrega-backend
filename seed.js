import Database from "better-sqlite3";

const db = new Database("mgnrega.db");

const sample = {
  fin_year: "2024-2025",
  month: "Dec",
  state_code: "05",
  state_name: "BIHAR",
  district_code: "0501",
  district_name: "PATNA",
  payload: JSON.stringify({
    fin_year: "2024-2025",
    month: "Dec",
    Total_Households_Worked: 87155,
    Total_Individuals_Worked: 90928,
    Total_Exp: 15209.14166989,
    Women_Persondays: 1696959,
    Avg_Days_Worked: 39,
    Payment_within_15_days: 100.74
  }),
};

db.prepare(`
INSERT INTO districts (state_code, state_name, district_code, district_name)
VALUES (?, ?, ?, ?)
`).run(sample.state_code, sample.state_name, sample.district_code, sample.district_name);

db.prepare(`
INSERT INTO mgnrega_records (fin_year, month, state_code, state_name, district_code, district_name, payload)
VALUES (?, ?, ?, ?, ?, ?, ?)
`).run(
  sample.fin_year,
  sample.month,
  sample.state_code,
  sample.state_name,
  sample.district_code,
  sample.district_name,
  sample.payload
);

console.log("✅ Sample data inserted successfully!");
