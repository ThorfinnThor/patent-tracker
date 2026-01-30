import { NextRequest, NextResponse } from "next/server";
import { prisma } from "../../../lib/prisma";

export const runtime = "nodejs";

type Level = "group" | "main_group" | "subclass" | "class";

function clamp(n: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, n));
}

export async function GET(req: NextRequest) {
  try {
    const { searchParams } = new URL(req.url);

    const sector = (searchParams.get("sector") || "") as "biotech" | "tech";
    const companyId = searchParams.get("companyId") || "";
    const days = clamp(Number(searchParams.get("days") || "365"), 30, 3650);
    const level = (searchParams.get("level") || "group") as Level;

    if (!["biotech", "tech"].includes(sector)) {
      return NextResponse.json({ error: "Invalid sector" }, { status: 400 });
    }
    if (!companyId) {
      return NextResponse.json({ error: "Missing companyId" }, { status: 400 });
    }
    if (!["group", "main_group", "subclass", "class"].includes(level)) {
      return NextResponse.json({ error: "Invalid level" }, { status: 400 });
    }

    // Controlled rollup expression (safe, no user SQL injection)
    const rollExpr =
      level === "group"
        ? "code"
        : level === "main_group"
        ? "split_part(code, '/', 1) || '/00'"
        : level === "subclass"
        ? "left(code, 4)"
        : "left(code, 3)";

    const titleJoin =
      level === "group" || level === "main_group"
        ? "LEFT JOIN cpc_group d ON d.cpc_group_id = rolled.code"
        : level === "subclass"
        ? "LEFT JOIN cpc_subclass d ON d.cpc_subclass_id = rolled.code"
        : "LEFT JOIN cpc_class d ON d.cpc_class_id = rolled.code";

    const titleSelect =
      level === "group" || level === "main_group"
        ? "COALESCE(d.cpc_group_title, '') AS title"
        : level === "subclass"
        ? "COALESCE(d.cpc_subclass_title, '') AS title"
        : "COALESCE(d.cpc_class_title, '') AS title";

    // Top CPC topics
    const topCpcSql = `
      WITH exploded AS (
        SELECT
          regexp_split_to_table(p.cpc_group_ids, '\\|') AS code
        FROM patents p
        WHERE p.sector = $1
          AND p.company_id = $2
          AND p.patent_date >= CURRENT_DATE - ($3::int || ' days')::interval
          AND p.cpc_group_ids <> ''
      ),
      rolled AS (
        SELECT (${rollExpr}) AS code
        FROM exploded
        WHERE code IS NOT NULL AND code <> ''
      )
      SELECT
        rolled.code AS code,
        ${titleSelect},
        COUNT(*)::int AS n
      FROM rolled
      ${titleJoin}
      GROUP BY rolled.code, title
      ORDER BY n DESC, rolled.code ASC
      LIMIT 20
    `;

    const topCpc = await prisma.$queryRawUnsafe<any[]>(topCpcSql, sector, companyId, days);

    // CPC trend: compare current window vs previous window
    const trendSql = `
      WITH cur AS (
        SELECT (${rollExpr}) AS code, COUNT(*)::int AS n
        FROM (
          SELECT regexp_split_to_table(p.cpc_group_ids, '\\|') AS code
          FROM patents p
          WHERE p.sector = $1 AND p.company_id = $2
            AND p.patent_date >= CURRENT_DATE - ($3::int || ' days')::interval
            AND p.cpc_group_ids <> ''
        ) x
        WHERE code IS NOT NULL AND code <> ''
        GROUP BY code
      ),
      prev AS (
        SELECT (${rollExpr}) AS code, COUNT(*)::int AS n
        FROM (
          SELECT regexp_split_to_table(p.cpc_group_ids, '\\|') AS code
          FROM patents p
          WHERE p.sector = $1 AND p.company_id = $2
            AND p.patent_date <  CURRENT_DATE - ($3::int || ' days')::interval
            AND p.patent_date >= CURRENT_DATE - (($3::int * 2) || ' days')::interval
            AND p.cpc_group_ids <> ''
        ) y
        WHERE code IS NOT NULL AND code <> ''
        GROUP BY code
      ),
      joined AS (
        SELECT
          COALESCE(cur.code, prev.code) AS code,
          COALESCE(prev.n, 0) AS prev_n,
          COALESCE(cur.n, 0) AS cur_n
        FROM cur
        FULL OUTER JOIN prev ON prev.code = cur.code
      )
      SELECT
        j.code,
        ${titleSelect.replace("rolled.code", "j.code")},
        j.prev_n,
        j.cur_n,
        (j.cur_n - j.prev_n) AS delta,
        CASE WHEN j.prev_n = 0 THEN NULL ELSE ROUND((100.0 * (j.cur_n - j.prev_n) / j.prev_n)::numeric, 2) END AS pct
      FROM joined j
      ${titleJoin.replace("rolled.code", "j.code")}
      WHERE j.cur_n > 0 OR j.prev_n > 0
      ORDER BY delta DESC, j.cur_n DESC
      LIMIT 20
    `;

    const cpcTrend = await prisma.$queryRawUnsafe<any[]>(trendSql, sector, companyId, days);

    // Competitors: overlap on top 6 CPC GROUPS (always detailed group codes)
    const competitorsSql = `
      WITH company_groups AS (
        SELECT code, COUNT(*)::int AS n
        FROM (
          SELECT regexp_split_to_table(p.cpc_group_ids, '\\|') AS code
          FROM patents p
          WHERE p.sector = $1 AND p.company_id = $2
            AND p.patent_date >= CURRENT_DATE - ($3::int || ' days')::interval
            AND p.cpc_group_ids <> ''
        ) x
        WHERE code IS NOT NULL AND code <> ''
        GROUP BY code
        ORDER BY n DESC, code ASC
        LIMIT 6
      ),
      others AS (
        SELECT
          p.company_id AS other_company_id,
          cg.code AS code,
          COUNT(DISTINCT p.patent_id)::int AS overlap
        FROM patents p
        JOIN LATERAL regexp_split_to_table(p.cpc_group_ids, '\\|') AS code ON TRUE
        JOIN company_groups cg ON cg.code = code
        WHERE p.sector = $1
          AND p.company_id <> $2
          AND p.patent_date >= CURRENT_DATE - ($3::int || ' days')::interval
          AND p.cpc_group_ids <> ''
        GROUP BY p.company_id, cg.code
      ),
      agg AS (
        SELECT
          other_company_id,
          SUM(overlap)::int AS score
        FROM others
        GROUP BY other_company_id
        ORDER BY score DESC, other_company_id ASC
        LIMIT 15
      )
      SELECT
        a.other_company_id AS company_id,
        COALESCE(c.display_name, a.other_company_id) AS display_name,
        a.score
      FROM agg a
      LEFT JOIN companies c
        ON c.sector = $1 AND c.company_id = a.other_company_id
      ORDER BY a.score DESC, display_name ASC
    `;

    const competitors = await prisma.$queryRawUnsafe<any[]>(competitorsSql, sector, companyId, days);

    // Co-assignees (within tracked set): other companies sharing same patent_id
    const coAssigneesSql = `
      SELECT
        p2.company_id AS company_id,
        COALESCE(c.display_name, p2.company_id) AS display_name,
        COUNT(DISTINCT p2.patent_id)::int AS n
      FROM patents p1
      JOIN patents p2
        ON p1.sector = p2.sector AND p1.patent_id = p2.patent_id
      LEFT JOIN companies c
        ON c.sector = p2.sector AND c.company_id = p2.company_id
      WHERE p1.sector = $1
        AND p1.company_id = $2
        AND p2.company_id <> $2
        AND p1.patent_date >= CURRENT_DATE - ($3::int || ' days')::interval
      GROUP BY p2.company_id, c.display_name
      ORDER BY n DESC, display_name ASC
      LIMIT 15
    `;

    const coAssignees = await prisma.$queryRawUnsafe<any[]>(coAssigneesSql, sector, companyId, days);

    // Top inventors
    const inventorsSql = `
      SELECT
        pi.inventor_name AS name,
        COUNT(DISTINCT pi.patent_id)::int AS n
      FROM patent_inventors pi
      WHERE pi.sector = $1
        AND pi.company_id = $2
        AND pi.patent_date >= CURRENT_DATE - ($3::int || ' days')::interval
        AND pi.inventor_name <> ''
      GROUP BY pi.inventor_name
      ORDER BY n DESC, name ASC
      LIMIT 15
    `;

    const topInventors = await prisma.$queryRawUnsafe<any[]>(inventorsSql, sector, companyId, days);

    return NextResponse.json(
      {
        sector,
        companyId,
        days,
        level,
        topCpc,
        cpcTrend,
        competitors,
        coAssignees,
        topInventors,
      },
      {
        headers: { "Cache-Control": "s-maxage=3600, stale-while-revalidate=86400" },
      }
    );
  } catch (e: any) {
    return NextResponse.json(
      { error: "Insights API error", message: String(e?.message || e), stack: String(e?.stack || "") },
      { status: 500 }
    );
  }
}
