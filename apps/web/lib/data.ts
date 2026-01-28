import fs from "fs";
import path from "path";
import { Company, PatentRow } from "./types";

function publicPath(...parts: string[]) {
  return path.join(process.cwd(), "public", ...parts);
}

export function readCompanies(sectorId: "biotech" | "tech"): Company[] {
  const p = publicPath("data", sectorId, "companies.json");
  const raw = fs.readFileSync(p, "utf-8");
  return JSON.parse(raw) as Company[];
}

export function listCompanyIds(sectorId: "biotech" | "tech"): string[] {
  return readCompanies(sectorId).map(c => c.companyId);
}

// Simple CSV parser for our small fixed schema
export function readCompanyPatents(sectorId: "biotech" | "tech", companyId: string): PatentRow[] {
  const p = publicPath("data", sectorId, "patents", `${companyId}.csv`);
  const raw = fs.readFileSync(p, "utf-8");
  const lines = raw.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return [];

  const header = lines[0].split(",");
  const idx = (name: string) => header.indexOf(name);

  const i_id = idx("patent_id");
  const i_date = idx("patent_date");
  const i_title = idx("patent_title");
  const i_cited = idx("patent_num_times_cited_by_us_patents");
  const i_cpc = idx("cpc_subclass_ids");

  const out: PatentRow[] = [];
  for (let i = 1; i < lines.length; i++) {
    const row = splitCsvLine(lines[i], header.length);
    out.push({
      patent_id: row[i_id] ?? "",
      patent_date: row[i_date] ?? "",
      patent_title: row[i_title] ?? "",
      patent_num_times_cited_by_us_patents: row[i_cited] ?? "",
      cpc_subclass_ids: row[i_cpc] ?? ""
    });
  }
  return out;
}

function splitCsvLine(line: string, expectedCols: number): string[] {
  // minimal CSV splitting for our generated CSVs (QUOTE_MINIMAL).
  // Handles quoted commas.
  const out: string[] = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];

    if (ch === '"' ) {
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
      continue;
    }
    cur += ch;
  }
  out.push(cur);

  // pad/truncate to expected
  while (out.length < expectedCols) out.push("");
  if (out.length > expectedCols) out.length = expectedCols;
  return out;
}
