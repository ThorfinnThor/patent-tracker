import { NextRequest, NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";

export const runtime = "nodejs";
export const revalidate = 3600; // 1 hour CDN cache

type Sector = "biotech" | "tech";
type CpcLevel = "group" | "subclass" | "class";
type WindowKey = "90d" | "180d" | "1y" | "2y" | "5y";

function parseSector(v: string | null): Sector {
  if (v === "biotech" || v === "tech") return v;
  return "biotech";
}

function parseCpcLevel(v: string | null): CpcLevel {
  if (v === "group" || v === "subclass" || v === "class") return v;
  return "group";
}

function parseWindow(v: string | null): { key: WindowKey; days: number } {
  const key = (v || "1y") as WindowKey;
  switch (key) {
    case "90d":
      return { key, days: 90 };
    case "180d":
      return { key, days: 180 };
    case "2y":
      return { key, days: 730 };
    case "5y":
      return { key, days: 1825 };
    case "1y":
    default:
      return { key: "1y", days: 365 };
  }
}

function clampInt(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n));
}

function cpcFieldForLevel(level: CpcLevel) {
  // patents table stores:
  // - cpc_group_ids: pipe-delimited group ids
  // - cpc_subclass_ids: pipe-delimited subclass ids
  // "class" we derive from subclass (first 3 chars) or from group if subclass empty
  if (level === "group") return "cpc_group_ids";
  return "cpc_subclass_ids";
}

function titleJoinForLevel(level: CpcLevel) {
  // Return { table, idCol, titleCol } for joins
  if (level === "group") return { table: "cpc_group", idCol: "cpc_group_id", titleCol: "cpc_group_title" };
  if (level === "subclass") return { table: "cpc_subclass", idCol: "cpc_subclass_id", titleCol: "cpc_subclass_title" };
  return { table: "cpc_class", idCol: "cpc_class_id", titleCol: "cpc_class_title" };
}

export async function GET(req: NextRequest) {
  try {
    const url = new URL(req.url);

    const sector = parseSector(url.searchParams.get("sector"));
    const companyId = (url.searchParams.get("companyId") || "").trim();
    const level = parseCpcLevel(url.searchParams.get("cpcLevel"));
    const window = parseWindow(url.searchParams.get("window"));

    if (!companyId) {
      return NextResponse.json(
        { error: "Missing companyId" },
        { status: 400, headers: { "Cache-Control": "no-store" } }
      );
    }

    const days = clampInt(window.days, 30, 3650);

    // We’ll compute:
    // 1) top CPC topics in current window
    // 2) CPC momentum vs previous window (same length immediately before)
    // 3) top competitors by CPC overlap (distinct codes overlap)
    // 4) top inventors in current window
    //
    // IMPORTANT: No ambiguous "code". Always reference agg.code explicitly.

    const cpcField = cpcFieldForLevel(level);
    const join = titleJoinForLevel(level);

    // For "class", derive the class id from subclass or group code:
    // - If we use subclass ids like "A61K", class is "A61"
    // - If data is "A61K31/00" we still take first 3 chars.
    const deriveClassExpr =
      level === "class"
        ? `LEFT(code_raw, 3)` // class = first 3 chars
        : `code_raw`;

    const codesCte = `
      WITH base AS (
        SELECT
          p.company_id,
          p.patent_id,
          p.patent_date,
          UNNEST(string_to_array(COALESCE(p.${cpcField}, ''), '|')) AS code_raw
        FROM patents p
        WHERE
          p.sector = $1
          AND p.company_id = $2
          AND p.patent_date >= (NOW() - ($3 || ' days')::interval)
      ),
      cleaned AS (
        SELECT
          ${deriveClassExpr} AS code
        FROM base
        WHERE TRIM(code_raw) <> ''
      )
    `;

    // 1) Top CPC topics
    const topTopicsSql = `
      ${codesCte}
      , agg AS (
        SELECT
          cleaned.code AS code,
          COUNT(*)::int AS patents
        FROM cleaned
        GROUP BY cleaned.code
      )
      SELECT
        agg.code AS code,
        COALESCE(d.${join.titleCol}, '') AS title,
        agg.patents AS patents
      FROM agg
      LEFT JOIN ${join.table} d
        ON d.${join.idCol} = agg.code
      ORDER BY agg.patents DESC, agg.code ASC
      LIMIT 15
    `;

    // 2) CPC momentum: compare current window vs previous window (same duration)
    const momentumSql = `
      WITH cur AS (
        SELECT
          UNNEST(string_to_array(COALESCE(p.${cpcField}, ''), '|')) AS code_raw
        FROM patents p
        WHERE
          p.sector = $1
          AND p.company_id = $2
          AND p.patent_date >= (NOW() - ($3 || ' days')::interval)
      ),
      prev AS (
        SELECT
          UNNEST(string_to_array(COALESCE(p.${cpcField}, ''), '|')) AS code_raw
        FROM patents p
        WHERE
          p.sector = $1
          AND p.company_id = $2
          AND p.patent_date <  (NOW() - ($3 || ' days')::interval)
          AND p.patent_date >= (NOW() - (($3 * 2) || ' days')::interval)
      ),
      cur_clean AS (
        SELECT ${deriveClassExpr} AS code
        FROM (SELECT code_raw FROM cur) t
        WHERE TRIM(code_raw) <> ''
      ),
      prev_clean AS (
        SELECT ${deriveClassExpr} AS code
        FROM (SELECT code_raw FROM prev) t
        WHERE TRIM(code_raw) <> ''
      ),
      cur_agg AS (
        SELECT code, COUNT(*)::int AS cur_count
        FROM cur_clean
        GROUP BY code
      ),
      prev_agg AS (
        SELECT code, COUNT(*)::int AS prev_count
        FROM prev_clean
        GROUP BY code
      ),
      merged AS (
        SELECT
          COALESCE(c.code, p.code) AS code,
          COALESCE(c.cur_count, 0) AS cur_count,
          COALESCE(p.prev_count, 0) AS prev_count
        FROM cur_agg c
        FULL OUTER JOIN prev_agg p
          ON p.code = c.code
      )
      SELECT
        merged.code AS code,
        COALESCE(d.${join.titleCol}, '') AS title,
        merged.cur_count AS cur_count,
        merged.prev_count AS prev_count,
        (merged.cur_count - merged.prev_count) AS delta
      FROM merged
      LEFT JOIN ${join.table} d
        ON d.${join.idCol} = merged.code
      ORDER BY delta DESC, merged.cur_count DESC, merged.code ASC
      LIMIT 15
    `;

    // 3) Competitors by CPC overlap (distinct overlap codes in same window)
    // We compute the company's distinct code set, then for every other company count overlaps.
    const competitorsSql = `
      WITH my_codes_raw AS (
        SELECT
          UNNEST(string_to_array(COALESCE(p.${cpcField}, ''), '|')) AS code_raw
        FROM patents p
        WHERE
          p.sector = $1
          AND p.company_id = $2
          AND p.patent_date >= (NOW() - ($3 || ' days')::interval)
      ),
      my_codes AS (
        SELECT DISTINCT ${deriveClassExpr} AS code
        FROM my_codes_raw
        WHERE TRIM(code_raw) <> ''
      ),
      other_codes_raw AS (
        SELECT
          p.company_id AS other_company_id,
          UNNEST(string_to_array(COALESCE(p.${cpcField}, ''), '|')) AS code_raw
        FROM patents p
        WHERE
          p.sector = $1
          AND p.company_id <> $2
          AND p.patent_date >= (NOW() - ($3 || ' days')::interval)
      ),
      other_codes AS (
        SELECT
          other_company_id,
          ${deriveClassExpr} AS code
        FROM other_codes_raw
        WHERE TRIM(code_raw) <> ''
      ),
      overlaps AS (
        SELECT
          oc.other_company_id AS company_id,
          COUNT(DISTINCT oc.code)::int AS overlap_codes
        FROM other_codes oc
        INNER JOIN my_codes mc
          ON mc.code = oc.code
        GROUP BY oc.other_company_id
      )
      SELECT
        o.company_id AS company_id,
        COALESCE(c.display_name, o.company_id) AS display_name,
        o.overlap_codes AS overlap_codes
      FROM overlaps o
      LEFT JOIN companies c
        ON c.sector = $1 AND c.company_id = o.company_id
      ORDER BY o.overlap_codes DESC, display_name ASC
      LIMIT 15
    `;

    // 4) Top inventors (needs patent_inventors table)
    const inventorsSql = `
      SELECT
        COALESCE(pi.inventor_name, '') AS inventor_name,
        COUNT(DISTINCT pi.patent_id)::int AS patents
      FROM patent_inventors pi
      WHERE
        pi.sector = $1
        AND pi.company_id = $2
        AND pi.patent_date >= (NOW() - ($3 || ' days')::interval)
      GROUP BY COALESCE(pi.inventor_name, '')
      HAVING COALESCE(pi.inventor_name, '') <> ''
      ORDER BY patents DESC, inventor_name ASC
      LIMIT 15
    `;

    // Execute in parallel
    const [topTopics, momentum, competitors, inventors] = await Promise.all([
      prisma.$queryRawUnsafe<any[]>(topTopicsSql, sector, companyId, String(days)),
      prisma.$queryRawUnsafe<any[]>(momentumSql, sector, companyId, String(days)),
      prisma.$queryRawUnsafe<any[]>(competitorsSql, sector, companyId, String(days)),
      prisma.$queryRawUnsafe<any[]>(inventorsSql, sector, companyId, String(days)),
    ]);

    const res = {
      sector,
      companyId,
      window: { key: window.key, days },
      cpcLevel: level,
      topCpcTopics: (topTopics || []).map((r) => ({
        code: String(r.code ?? ""),
        title: String(r.title ?? ""),
        patents: Number(r.patents ?? 0),
      })),
      cpcMomentum: (momentum || []).map((r) => ({
        code: String(r.code ?? ""),
        title: String(r.title ?? ""),
        cur: Number(r.cur_count ?? 0),
        prev: Number(r.prev_count ?? 0),
        delta: Number(r.delta ?? 0),
      })),
      topCompetitors: (competitors || []).map((r) => ({
        companyId: String(r.company_id ?? ""),
        displayName: String(r.display_name ?? ""),
        overlapCodes: Number(r.overlap_codes ?? 0),
      })),
      topInventors: (inventors || []).map((r) => ({
        inventorName: String(r.inventor_name ?? ""),
        patents: Number(r.patents ?? 0),
      })),
    };

    return NextResponse.json(res, {
      status: 200,
      headers: {
        // CDN cache for 1 hour, allow stale while revalidate
        "Cache-Control": "public, s-maxage=3600, stale-while-revalidate=3600",
      },
    });
  } catch (e: any) {
    return NextResponse.json(
      {
        error: "Insights API error",
        message: String(e?.message || e),
        stack: String(e?.stack || ""),
      },
      { status: 500, headers: { "Cache-Control": "no-store" } }
    );
  }
}
