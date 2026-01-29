import initSqlJs from "sql.js";

type DBHandle = { db: any };
const globalAny = global as any;

async function fetchDbBytes(url: string): Promise<Uint8Array> {
  const res = await fetch(url, { cache: "force-cache" });
  if (!res.ok) throw new Error(`Failed to fetch DB: ${res.status} ${res.statusText}`);
  const buf = await res.arrayBuffer();
  return new Uint8Array(buf);
}

function locateSqlWasm(file: string) {
  // Resolves sql.js wasm file from node_modules on the server
  // Works in Next.js server runtime.
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  return require.resolve("sql.js/dist/" + file);
}

function dbUrlForSector(sector: "biotech" | "tech"): string {
  if (sector === "biotech") return process.env.BIOTECH_DB_URL || "";
  if (sector === "tech") return process.env.TECH_DB_URL || "";
  return "";
}

async function getDbFromUrlCached(sector: "biotech" | "tech"): Promise<DBHandle> {
  const key = `__PV_DB_${sector}`;
  if (globalAny[key]) return globalAny[key];

  const dbUrl = dbUrlForSector(sector);
  if (!dbUrl) {
    throw new Error(
      `Missing DB URL env var for sector=${sector}. Set BIOTECH_DB_URL and TECH_DB_URL in Vercel.`
    );
  }

  const SQL = await initSqlJs({
    locateFile: (file: string) => locateSqlWasm(file),
  });

  const bytes = await fetchDbBytes(dbUrl);
  const db = new SQL.Database(bytes);

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
  cap?: number; // used only when year is not set
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

  const handle = await getDbFromUrlCached(sector);
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

  const whereSql = `WHERE ${where.join(" AND ")}`;

  const orderSql =
    sort === "cited"
      ? "ORDER BY cited_by DESC, patent_date DESC, patent_id DESC"
      : "ORDER BY patent_date DESC, patent_id DESC";

  const capN = Math.max(50, Math.min(500, cap));

  // In "All years" mode we cap total rows returned by applying LIMIT capN before paging.
  const baseSql = `
    SELECT patent_id, patent_date, patent_title, cited_by, cpc_subclass_ids
    FROM patents
    ${whereSql}
    ${orderSql}
    ${typeof year !== "number" ? `LIMIT ${capN}` : ""}
  `;

  // Page within the base result
  const pagedSql = `
    SELECT * FROM (${baseSql})
    LIMIT ? OFFSET ?
  `;

  const stmt = db.prepare(pagedSql);
  stmt.bind([...bind, safePageSize, safePage * safePageSize]);

  const rows: any[] = [];
  while (stmt.step()) {
    rows.push(stmt.getAsObject());
  }
  stmt.free();

  // Total count (for pagination UI)
  const countStmt = db.prepare(`SELECT COUNT(*) as n FROM patents ${whereSql}`);
  countStmt.bind(bind);
  countStmt.step();
  const countRow = countStmt.getAsObject() as any;
  countStmt.free();

  const rawTotal = Number(countRow.n || 0);
  const total = typeof year !== "number" ? Math.min(rawTotal, capN) : rawTotal;

  return {
    total,
    page: safePage,
    pageSize: safePageSize,
    sort,
    year: typeof year === "number" ? year : null,
    q,
    rows: rows.map((r) => ({
      patent_id: String(r.patent_id ?? ""),
      patent_date: String(r.patent_date ?? ""),
      patent_title: String(r.patent_title ?? ""),
      cited_by: Number(r.cited_by ?? 0),
      cpc_subclass_ids: String(r.cpc_subclass_ids ?? ""),
    })),
  };
}

export async function queryYears(params: { sector: "biotech" | "tech"; companyId: string }) {
  const { sector, companyId } = params;

  const handle = await getDbFromUrlCached(sector);
  const db = handle.db;

  const stmt = db.prepare(`
    SELECT DISTINCT patent_year as y
    FROM patents
    WHERE company_id = ?
    ORDER BY y DESC
  `);

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
