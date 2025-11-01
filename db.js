import path from "path";
import Database from "better-sqlite3";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ✅ Use absolute path (Render safe)
const dbPath = path.join(__dirname, "mgnrega.db");

const db = new Database(dbPath);
export default db;
