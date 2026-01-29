import fs from "fs";
import path from "path";
import initSqlJs from "sql.js";

type DBHandle = {
  db: any;
};

const globalAny = global as any;

// Cache across requests when the serverless instance is reused
async function getDbCached(sector: "biotech" | "tech"): Promise<DBHandle> {
  const key = `__PV_DB_${sector}`;
  if (globalAny[key]) return globalAny[key];

  const SQL = await initSqlJs({
    locateFile: (file: string) => path.join(process.cwd(), "node_modules", "sql.js", "dist", file),
  });

  const dbPath = path.join(process.cwd(), "data", "db", `${sector}.sqlite`);
  const buf = fs.readFileSync(dbPath);

  const db = new SQL.Database(new Uint8Array(buf));
  globalAny[key] = { db };
  return globalAny[key];
}

export async function queryPatents(params: {
  sector: "biotech" | "tech";
  companyId: string;
  year?: number;
  q?: string;
  sort?: "recent" | "cited";
  page?: number;
  pageSize?: number;
  cap?: number; // applied when year is not specified
}) {
  const {
    sector,
    companyId,
    year,
    q = "",
    sort = "recent",
    page = 0,
    pageSize = 100,
    cap = 500,
  } = params;

  const handle = await getDbCached(sector);
  const db = handle.db;

  const safePageSize = Math.max(10, Math.min(200, pageSize));
  const safePage = Math.max(0, page);

  const where: string[] = ["company_id = ?"];
  const bind: any[] = [companyId];

  if (typeof year === "number") {
    where.push("patent_year = ?");
    bind.push(year);
  }

  if (q.trim()) {
    where.push("patent_title LIKE ?");
    bind.push(`%${q.trim()}%`);
  }

  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

  const orderSql = sort === "cited"
    ? "ORDER BY cited_by DESC, patent_date DESC, patent_id DESC"
    : "ORDER BY patent_date DESC, patent_id DESC";

  // Cap only applies to “all years” mode to keep it bounded (Top N recent/cited)
  // Year-filtered mode is paginated without the cap.
  const capSql = (typeof year !== "number") ? `LIMIT ${Math.max(50, Math.min(500, cap))}` : "";
  const baseSql = `
    SELECT patent_id, patent_date, patent_title, cited_by, cpc_subclass_ids
    FROM patents
    ${whereSql}
    ${orderSql}
    ${capSql}
  `;

  // If capped, we page within the capped result using OFFSET on a subquery
  const pagedSql = (typeof year !== "number")
    ? `
      SELECT * FROM (${baseSql})
      LIMIT ? OFFSET ?
    `
    : `
      ${baseSql}
      LIMIT ? OFFSET ?
    `;

  const pagedBind = [...bind, safePageSize, safePage * safePageSize];

  const stmt = db.prepare(pagedSql);
  stmt.bind(pagedBind);

  const rows: any[] = [];
  while (stmt.step()) {
    rows.push(stmt.getAsObject());
  }
  stmt.free();

  // total count (for pagination UI)
  const countSql = `
    SELECT COUNT(*) as n
    FROM patents
    ${whereSql}
  `;
  const countStmt = db.prepare(countSql);
  countStmt.bind(bind);
  countStmt.step();
  const countRow = countStmt.getAsObject() as any;
  countStmt.free();

  // If cap applies, total should be min(count, cap) so paging doesn't lie
  const rawTotal = Number(countRow.n || 0);
  const total = (typeof year !== "number") ? Math.min(rawTotal, Math.max(50, Math.min(500, cap))) : rawTotal;

  return {
    total,
    page: safePage,
    pageSize: safePageSize,
    sort,
    year: typeof year === "number" ? year : null,
    q,
    rows: rows.map(r => ({
      patent_id: String(r.patent_id ?? ""),
      patent_date: String(r.patent_date ?? ""),
      patent_title: String(r.patent_title ?? ""),
      cited_by: Number(r.cited_by ?? 0),
      cpc_subclass_ids: String(r.cpc_subclass_ids ?? "")
    }))
  };
}

export async function queryYears(params: { sector: "biotech" | "tech"; companyId: string }) {
  const { sector, companyId } = params;
  const handle = await getDbCached(sector);
  const db = handle.db;

  const sql = `
    SELECT DISTINCT patent_year as y
    FROM patents
    WHERE company_id = ?
    ORDER BY y DESC
  `;
  const stmt = db.prepare(sql);
  stmt.bind([companyId]);

  const years: number[] = [];
  while (stmt.step()) {
    const row = stmt.getAsObject() as any;
    const y = Number(row.y);
    if (!Number.isNaN(y)) years.push(y);
  }
  stmt.free();

  return { companyId, years };
}
