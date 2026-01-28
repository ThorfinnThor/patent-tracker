import CompanyTable from "../../components/CompanyTable";
import { readCompanies } from "../../lib/data";

export default function BiotechPage() {
  const companies = readCompanies("biotech");
  return (
    <main style={{ display: "grid", gap: 12 }}>
      <h2 style={{ margin: 0 }}>Biotech/Pharma — Top 200</h2>
      <p style={{ margin: 0, opacity: 0.8 }}>
        Default ranking: patent count (last 5 years). Click headers to sort by citations, citations/patent, or CPC breadth.
      </p>
      <CompanyTable sectorId="biotech" companies={companies} />
    </main>
  );
}
