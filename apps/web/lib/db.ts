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
    years: rows.map((r: any) => Number(r.y)).filter((n) => Number.isFinite(n)),
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
  const qLike = "%" + q.trim() + "%";

  // Total count (uncapped if year selected; capped in "all years" mode)
  let rawTotal = 0;
  if (hasYear) {
    if (hasQ) {
      const c = await sql`
        SELECT COUNT(*)::int AS n
        FROM patents
        WHERE sector=${sector} AND company_id=${companyId} AND patent_year=${year!}
          AND patent_title ILIKE ${qLike}
      `;
      rawTotal = Number((c.rows?.[0] as any)?.n ?? 0);
    } else {
      const c = await sql`
        SELECT COUNT(*)::int AS n
        FROM patents
        WHERE sector=${sector} AND company_id=${companyId} AND patent_year=${year!}
      `;
      rawTotal = Number((c.rows?.[0] as any)?.n ?? 0);
    }
  } else {
    if (hasQ) {
      const c = await sql`
        SELECT COUNT(*)::int AS n
        FROM patents
        WHERE sector=${sector} AND company_id=${companyId}
          AND patent_title ILIKE ${qLike}
      `;
      rawTotal = Number((c.rows?.[0] as any)?.n ?? 0);
    } else {
      const c = await sql`
        SELECT COUNT(*)::int AS n
        FROM patents
        WHERE sector=${sector} AND company_id=${companyId}
      `;
      rawTotal = Number((c.rows?.[0] as any)?.n ?? 0);
    }
  }

  const total = hasYear ? rawTotal : Math.min(rawTotal, capN);

  // DATA QUERY
  // Year selected => full paginated
  if (hasYear) {
    if (sort === "cited") {
      if (hasQ) {
        const { rows } = await sql`
          SELECT patent_id, patent_date, patent_title, cited_by, cpc_subclass_ids
          FROM patents
          WHERE sector=${sector} AND company_id=${companyId} AND patent_year=${year!}
            AND patent_title ILIKE ${qLike}
          ORDER BY cited_by DESC, patent_date DESC, patent_id DESC
          LIMIT ${safePageSize} OFFSET ${safePage * safePageSize}
        `;
        return formatResult(rows, total, safePage, safePageSize, "cited", year!, q);
      } else {
        const { rows } = await sql`
          SELECT patent_id, patent_date, patent_title, cited_by, cpc_subclass_ids
          FROM patents
          WHERE sector=${sector} AND company_id=${companyId} AND patent_year=${year!}
          ORDER BY cited_by DESC, patent_date DESC, patent_id DESC
          LIMIT ${safePageSize} OFFSET ${safePage * safePageSize}
        `;
        return formatResult(rows, total, safePage, safePageSize, "cited", year!, q);
      }
    } else {
      // recent
      if (hasQ) {
        const { rows } = await sql`
          SELECT patent_id, patent_date, patent_title, cited_by, cpc_subclass_ids
          FROM patents
          WHERE sector=${sector} AND company_id=${companyId} AND patent_year=${year!}
            AND patent_title ILIKE ${qLike}
          ORDER BY patent_date DESC, patent_id DESC
          LIMIT ${safePageSize} OFFSET ${safePage * safePageSize}
        `;
        return formatResult(rows, total, safePage, safePageSize, "recent", year!, q);
      } else {
        const { rows } = await sql`
          SELECT patent_id, patent_date, patent_title, cited_by, cpc_subclass_ids
          FROM patents
          WHERE sector=${sector} AND company_id=${companyId} AND patent_year=${year!}
          ORDER BY patent_date DESC, patent_id DESC
          LIMIT ${safePageSize} OFFSET ${safePage * safePageSize}
        `;
        return formatResult(rows, total, safePage, safePageSize, "recent", year!, q);
      }
    }
  }

  // No year selected => "all years" capped Top N, then page within the capped set
  if (sort === "cited") {
    if (hasQ) {
      const { rows } = await sql`
        SELECT patent_id, patent_date, patent_title, cited_by, cpc_subclass_ids
        FROM patents
        WHERE sector=${sector} AND company_id=${companyId}
          AND patent_title ILIKE ${qLike}
        ORDER BY cited_by DESC, patent_date DESC, patent_id DESC
        LIMIT ${capN}
        OFFSET ${safePage * safePageSize}
      `;
      // total already capped in header
      return formatResult(rows, total, safePage, safePageSize, "cited", null, q);
    } else {
      const { rows } = await sql`
        SELECT patent_id, patent_date, patent_title, cited_by, cpc_subclass_ids
        FROM patents
        WHERE sector=${sector} AND company_id=${companyId}
        ORDER BY cited_by DESC, patent_date DESC, patent_id DESC
        LIMIT ${capN}
        OFFSET ${safePage * safePageSize}
      `;
      return formatResult(rows, total, safePage, safePageSize, "cited", null, q);
    }
  } else {
    // recent
    if (hasQ) {
      const { rows } = await sql`
        SELECT patent_id, patent_date, patent_title, cited_by, cpc_subclass_ids
        FROM patents
        WHERE sector=${sector} AND company_id=${companyId}
          AND patent_title ILIKE ${qLike}
        ORDER BY patent_date DESC, patent_id DESC
        LIMIT ${capN}
        OFFSET ${safePage * safePageSize}
      `;
      return formatResult(rows, total, safePage, safePageSize, "recent", null, q);
    } else {
      const { rows } = await sql`
        SELECT patent_id, patent_date, patent_title, cited_by, cpc_subclass_ids
        FROM patents
        WHERE sector=${sector} AND company_id=${companyId}
        ORDER BY patent_date DESC, patent_id DESC
        LIMIT ${capN}
        OFFSET ${safePage * safePageSize}
      `;
      return formatResult(rows, total, safePage, safePageSize, "recent", null, q);
    }
  }
}

function formatResult(
  rows: any[],
  total: number,
  page: number,
  pageSize: number,
  sort: "recent" | "cited",
  year: number | null,
  q: string
) {
  return {
    total,
    page,
    pageSize,
    sort,
    year,
    q,
    rows: rows.map((r: any) => ({
      patent_id: String(r.patent_id ?? ""),
      patent_date: String(r.patent_date ?? ""),
      patent_title: String(r.patent_title ?? ""),
      cited_by: Number(r.cited_by ?? 0),
      cpc_subclass_ids: String(r.cpc_subclass_ids ?? ""),
    })),
  };
}
