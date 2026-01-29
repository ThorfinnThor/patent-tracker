import { sql } from "@vercel/postgres";

export type Sector = "biotech" | "tech";

export async function queryYears(params: { sector: Sector; companyId: string }) {
  const { sector, companyId } = params;

  const { rows } = await sql`
    SELECT DISTINCT patent_year AS y
    FROM patents
    WHERE sector = ${sector} AND company_id = ${companyId}
    ORDER BY y DESC
  `;

  return {
    companyId,
    years: rows.map(r => Number(r.y)).filter(n => Number.isFinite(n)),
  };
}

export async function queryPatents(params: {
  sector: Sector;
  companyId: string;
  year?: number;
  q?: string;
  sort?: "recent" | "cited";
  page?: number;
  pageSize?: number;
  cap?: number; // applies only when year is not set
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

  const safePageSize = Math.max(10, Math.min(200, pageSize));
  const safePage = Math.max(0, page);
  const capN = Math.max(50, Math.min(500, cap));
  const hasYear = typeof year === "number" && Number.isFinite(year);
  const hasQ = q.trim().length > 0;

  const orderSql =
    sort === "cited"
      ? sql`ORDER BY cited_by DESC, patent_date DESC, patent_id DESC`
      : sql`ORDER BY patent_date DESC, patent_id DESC`;

  // Count total (uncapped if year selected; capped if "all years" mode)
  const countQuery = hasYear
    ? hasQ
      ? sql`
          SELECT COUNT(*)::int AS n
          FROM patents
          WHERE sector=${sector} AND company_id=${companyId} AND patent_year=${year!}
            AND patent_title ILIKE ${"%" + q.trim() + "%"}
        `
      : sql`
          SELECT COUNT(*)::int AS n
          FROM patents
          WHERE sector=${sector} AND company_id=${companyId} AND patent_year=${year!}
        `
    : hasQ
      ? sql`
          SELECT COUNT(*)::int AS n
          FROM patents
          WHERE sector=${sector} AND company_id=${companyId}
            AND patent_title ILIKE ${"%" + q.trim() + "%"}
        `
      : sql`
          SELECT COUNT(*)::int AS n
          FROM patents
          WHERE sector=${sector} AND company_id=${companyId}
        `;

  const countRes = await countQuery;
  const rawTotal = Number(countRes.rows?.[0]?.n ?? 0);
  const total = hasYear ? rawTotal : Math.min(rawTotal, capN);

  // Data query:
  // - If no year selected: cap to Top N first, then page within it
  // - If year selected: page within full set
  if (!hasYear) {
    // "All years" capped mode
    const base = hasQ
      ? sql`
          SELECT patent_id, patent_date, patent_title, cited_by, cpc_subclass_ids
          FROM patents
          WHERE sector=${sector} AND company_id=${companyId}
            AND patent_title ILIKE ${"%" + q.trim() + "%"}
          ${orderSql}
          LIMIT ${capN}
        `
      : sql`
          SELECT patent_id, patent_date, patent_title, cited_by, cpc_subclass_ids
          FROM patents
          WHERE sector=${sector} AND company_id=${companyId}
          ${orderSql}
          LIMIT ${capN}
        `;

    const { rows } = await sql`
      SELECT *
      FROM (${base}) AS t
      LIMIT ${safePageSize} OFFSET ${safePage * safePageSize}
    `;

    return {
      total,
      page: safePage,
      pageSize: safePageSize,
      sort,
      year: null,
      q,
      rows: rows.map(r => ({
        patent_id: String(r.patent_id ?? ""),
        patent_date: String(r.patent_date ?? ""),
        patent_title: String(r.patent_title ?? ""),
        cited_by: Number(r.cited_by ?? 0),
        cpc_subclass_ids: String(r.cpc_subclass_ids ?? ""),
      })),
    };
  }

  // Year selected: full paginated mode
  const dataQuery = hasQ
    ? sql`
        SELECT patent_id, patent_date, patent_title, cited_by, cpc_subclass_ids
        FROM patents
        WHERE sector=${sector} AND company_id=${companyId} AND patent_year=${year!}
          AND patent_title ILIKE ${"%" + q.trim() + "%"}
        ${orderSql}
        LIMIT ${safePageSize} OFFSET ${safePage * safePageSize}
      `
    : sql`
        SELECT patent_id, patent_date, patent_title, cited_by, cpc_subclass_ids
        FROM patents
        WHERE sector=${sector} AND company_id=${companyId} AND patent_year=${year!}
        ${orderSql}
        LIMIT ${safePageSize} OFFSET ${safePage * safePageSize}
      `;

  const { rows } = await dataQuery;

  return {
    total,
    page: safePage,
    pageSize: safePageSize,
    sort,
    year: year!,
    q,
    rows: rows.map(r => ({
      patent_id: String(r.patent_id ?? ""),
      patent_date: String(r.patent_date ?? ""),
      patent_title: String(r.patent_title ?? ""),
      cited_by: Number(r.cited_by ?? 0),
      cpc_subclass_ids: String(r.cpc_subclass_ids ?? ""),
    })),
  };
}
