import { NextRequest, NextResponse } from "next/server";
import { queryYears } from "../../../lib/db";

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);

  const sector = (searchParams.get("sector") || "") as "biotech" | "tech";
  const companyId = searchParams.get("companyId") || "";

  if (!["biotech", "tech"].includes(sector)) {
    return NextResponse.json({ error: "Invalid sector" }, { status: 400 });
  }
  if (!companyId) {
    return NextResponse.json({ error: "Missing companyId" }, { status: 400 });
  }

  const data = await queryYears({ sector, companyId });

  return NextResponse.json(data, {
    headers: {
      "Cache-Control": "s-maxage=3600, stale-while-revalidate=86400"
    }
  });
}
