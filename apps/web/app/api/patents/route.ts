import { NextRequest, NextResponse } from "next/server";
import { queryPatents } from "../../../lib/db";

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);

  const sector = (searchParams.get("sector") || "") as "biotech" | "tech";
  const companyId = searchParams.get("companyId") || "";
  const yearStr = searchParams.get("year");
  const q = searchParams.get("q") || "";
  const sort = (searchParams.get("sort") || "recent") as "recent" | "cited";
  const page = Number(searchParams.get("page") || "0");
  const pageSize = Number(searchParams.get("pageSize") || "100");
  const cap = Number(searchParams.get("cap") || "500");

  if (!["biotech", "tech"].includes(sector)) {
    return NextResponse.json({ error: "Invalid sector" }, { status: 400 });
  }
  if (!companyId) {
    return NextResponse.json({ error: "Missing companyId" }, { status: 400 });
  }

  const year = yearStr ? Number(yearStr) : undefined;

  const data = await queryPatents({
    sector,
    companyId,
    year: typeof year === "number" && !Number.isNaN(year) ? year : undefined,
    q,
    sort: sort === "cited" ? "cited" : "recent",
    page: Number.isFinite(page) ? page : 0,
    pageSize: Number.isFinite(pageSize) ? pageSize : 100,
    cap: Number.isFinite(cap) ? cap : 500,
  });

  return NextResponse.json(data, {
    headers: {
      // Encourage caching at the edge for speed
      "Cache-Control": "s-maxage=3600, stale-while-revalidate=86400"
    }
  });
}
