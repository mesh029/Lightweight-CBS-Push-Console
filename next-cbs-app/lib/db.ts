import mysql, { Pool } from "mysql2/promise";
import { loadDbConfig } from "@lib/config";

let pool: Pool | null = null;

export function getDbPool() {
  if (!pool) {
    const cfg = loadDbConfig();

    pool = mysql.createPool({
      host: cfg.host,
      port: cfg.port,
      user: cfg.user,
      password: cfg.password,
      database: cfg.name,
      connectionLimit: 5
    });
  }

  return pool;
}

