"use client";

import { useMemo, useState } from "react";
import type { Company } from "../lib/types";

type SortKey = keyof Pick<Company, "displayName" | "patentCount" | "totalCitations" | "citationsPerPatent" | "cpcBreadth">;

export default function CompanyTable({
  sectorId,
  companies
}: {
  sectorId: "biotech" | "tech";
  companies: Company[];
}) {
  const [sortKey, setSortKey] = useState<SortKey>("patentCount");
  const [desc, setDesc] = useState(true);
  const [q, setQ] = useState("");

  const filtered = useMemo(() => {
    const qq = q.trim().toLowerCase();
    if (!qq) return companies;
    return companies.filter(c => c.displayName.toLowerCase().includes(qq));
  }, [companies, q]);

  const sorted = useMemo(() => {
    const arr = [...filtered];
    arr.sort((a, b) => {
      const av = a[sortKey] as any;
      const bv = b[sortKey] as any;

      if (typeof av === "string" && typeof bv === "string") {
        return desc ? bv.localeCompare(av) : av.localeCompare(bv);
      }
      const diff = (Number(av) || 0) - (Number(bv) || 0);
      return desc ? -diff : diff;
    });
    return arr;
  }, [filtered, sortKey, desc]);

  const onSort = (k: SortKey) => {
    if (k === sortKey) setDesc(!desc);
    else {
      setSortKey(k);
      setDesc(true);
    }
  };

  return (
    <div style={{ display: "grid", gap: 12 }}>
      <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Search company..."
          style={{ padding: 10, width: 320 }}
        />
        <div style={{ opacity: 0.7 }}>
          Showing {sorted.length} / {companies.length}
        </div>
      </div>

      <div style={{ overflowX: "auto", border: "1px solid #ddd", borderRadius: 12 }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr>
              <Th label="Company" onClick={() => onSort("displayName")} />
              <Th label="Patents (5y)" onClick={() => onSort("patentCount")} />
              <Th label="Citations" onClick={() => onSort("totalCitations")} />
              <Th label="Citations/Patent" onClick={() => onSort("citationsPerPatent")} />
              <Th label="CPC Breadth" onClick={() => onSort("cpcBreadth")} />
            </tr>
          </thead>
          <tbody>
            {sorted.map(c => (
              <tr key={c.companyId} style={{ borderTop: "1px solid #eee" }}>
                <td style={{ padding: 10 }}>
                  <a href={`/${sectorId}/${encodeURIComponent(c.companyId)}`} style={{ textDecoration: "none" }}>
                    {c.displayName}
                  </a>
                </td>
                <td style={{ padding: 10 }}>{c.patentCount}</td>
                <td style={{ padding: 10 }}>{c.totalCitations}</td>
                <td style={{ padding: 10 }}>{c.citationsPerPatent.toFixed(2)}</td>
                <td style={{ padding: 10 }}>{c.cpcBreadth}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function Th({ label, onClick }: { label: string; onClick: () => void }) {
  return (
    <th
      onClick={onClick}
      style={{
        textAlign: "left",
        padding: 10,
        cursor: "pointer",
        userSelect: "none",
        background: "#fafafa",
        borderBottom: "1px solid #eee"
      }}
    >
      {label}
    </th>
  );
}
