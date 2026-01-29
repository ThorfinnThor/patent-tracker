import CompanyPatentsViewer from "../../../components/CompanyPatentsViewer";
import fs from "fs";
import path from "path";

type Company = {
  companyId: string;
  displayName: string;
  patentCount: number;
  totalCitations: number;
  citationsPerPatent: number;
  cpcBreadth: number;
};

function readCompanies(sector: "biotech" | "tech"): Company[] {
  const p = path.join(process.cwd(), "public", "data", sector, "companies.json");
  const raw = fs.readFileSync(p, "utf-8");
  return JSON.parse(raw) as Company[];
}

export default function BiotechCompanyPage({ params }: { params: { companyId: string } }) {
  const companies = readCompanies("biotech");
  const company = companies.find(c => c.companyId === params.companyId);

  return (
    <main style={{ display: "grid", gap: 12 }}>
      <a className="pill" href="/biotech" style={{ width: "fit-content" }}>← Back</a>

      <div className="card cardPad" style={{ display: "grid", gap: 10 }}>
        <h2 className="h2">{company?.displayName ?? params.companyId}</h2>

        {company && (
          <div className="kpiRow">
            <div className="kpi">
              <div className="kpiLabel">Patents (5y)</div>
              <div className="kpiValue">{company.patentCount}</div>
            </div>
            <div className="kpi">
              <div className="kpiLabel">Citations</div>
              <div className="kpiValue">{company.totalCitations}</div>
            </div>
            <div className="kpi">
              <div className="kpiLabel">Citations / Patent</div>
              <div className="kpiValue">{company.citationsPerPatent.toFixed(2)}</div>
            </div>
            <div className="kpi">
              <div className="kpiLabel">CPC Breadth</div>
              <div className="kpiValue">{company.cpcBreadth}</div>
            </div>
          </div>
        )}

        <p className="p">
          Tracker view defaults to Top 500 most recent (or most cited). Select a year for full paginated browsing.
        </p>
      </div>

      <CompanyPatentsViewer sector="biotech" companyId={params.companyId} />
    </main>
  );
}
