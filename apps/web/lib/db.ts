import { prisma } from "./prisma";

export type Sector = "biotech" | "tech";

export async function queryYears(params: { sector: Sector; companyId: string }) {
  const { sector, companyId } = params;

  const rows = await prisma.patents.findMany({
    where: { sector, company_id: companyId },
    select: { patent_year: true },
    distinct: ["patent_year"],
    orderBy: { patent_year: "desc" },
  });

  return {
    companyId,
    years: rows.map((r) => r.patent_year),
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
  cap?: number; // only when year is not set
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

  const where: any = { sector, company_id: companyId };
  if (hasYear) where.patent_year = year!;
  if (hasQ) where.patent_title = { contains: q.trim(), mode: "insensitive" };

  // total
  const rawTotal = await prisma.patents.count({ where });
  const total = hasYear ? rawTotal : Math.min(rawTotal, capN);

  const orderBy =
    sort === "cited"
      ? [{ cited_by: "desc" as const }, { patent_date: "desc" as const }, { patent_id: "desc" as const }]
      : [{ patent_date: "desc" as const }, { patent_id: "desc" as const }];

  if (!hasYear) {
    // bounded tracker view: grab top capN then slice page in-memory.
    // This avoids complicated SQL, keeps deterministic cap.
    const top = await prisma.patents.findMany({
      where,
      orderBy,
      take: capN,
      select: {
        patent_id: true,
        patent_date: true,
        patent_title: true,
        cited_by: true,
        cpc_subclass_ids: true,
      },
    });

    const start = safePage * safePageSize;
    const slice = top.slice(start, start + safePageSize);

    return {
      total,
      page: safePage,
      pageSize: safePageSize,
      sort,
      year: null,
      q,
      rows: slice.map((r) => ({
        patent_id: r.patent_id,
        patent_date: r.patent_date.toISOString().slice(0, 10),
        patent_title: r.patent_title,
        cited_by: r.cited_by,
        cpc_subclass_ids: r.cpc_subclass_ids,
      })),
    };
  }

  // year selected: full pagination
  const rows = await prisma.patents.findMany({
    where,
    orderBy,
    skip: safePage * safePageSize,
    take: safePageSize,
    select: {
      patent_id: true,
      patent_date: true,
      patent_title: true,
      cited_by: true,
      cpc_subclass_ids: true,
    },
  });

  return {
    total,
    page: safePage,
    pageSize: safePageSize,
    sort,
    year: year!,
    q,
    rows: rows.map((r) => ({
      patent_id: r.patent_id,
      patent_date: r.patent_date.toISOString().slice(0, 10),
      patent_title: r.patent_title,
      cited_by: r.cited_by,
      cpc_subclass_ids: r.cpc_subclass_ids,
    })),
  };
}
