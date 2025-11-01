import Database from 'better-sqlite3';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Correct absolute path
const dbPath = path.join(__dirname, 'mgnrega.db');

const db = new Database(dbPath);
export default db;
