import { listCompanyIds, readCompanies, readCompanyPatents } from "../../../lib/data";
import PatentTable from "../../../components/PatentTable";

export function generateStaticParams() {
  return listCompanyIds("tech").map(companyId => ({ companyId }));
}

export default function TechCompanyPage({ params }: { params: { companyId: string } }) {
  const companies = readCompanies("tech");
  const company = companies.find(c => c.companyId === params.companyId);

  const rows = readCompanyPatents("tech", params.companyId);

  return (
    <main style={{ display: "grid", gap: 12 }}>
      <a href="/tech" style={{ textDecoration: "none" }}>← Back</a>

      <h2 style={{ margin: 0 }}>{company?.displayName ?? params.companyId}</h2>

      {company && (
        <div style={{ display: "flex", gap: 14, flexWrap: "wrap" }}>
          <Metric label="Patents (5y)" value={company.patentCount} />
          <Metric label="Citations" value={company.totalCitations} />
          <Metric label="Citations/Patent" value={company.citationsPerPatent.toFixed(2)} />
          <Metric label="CPC Breadth" value={company.cpcBreadth} />
        </div>
      )}

      <h3 style={{ margin: "12px 0 0 0" }}>Patents</h3>
      <PatentTable rows={rows} />
    </main>
  );
}

function Metric({ label, value }: { label: string; value: any }) {
  return (
    <div style={{ border: "1px solid #ddd", borderRadius: 12, padding: 12, minWidth: 160 }}>
      <div style={{ fontSize: 12, opacity: 0.7 }}>{label}</div>
      <div style={{ fontWeight: 700, fontSize: 18 }}>{value}</div>
    </div>
  );
}
