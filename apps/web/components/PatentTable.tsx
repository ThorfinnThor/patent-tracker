"use client";

import { useMemo, useState } from "react";
import type { PatentRow } from "../lib/types";

type SortKey = "patent_date" | "patent_num_times_cited_by_us_patents" | "patent_title";

export default function PatentTable({ rows }: { rows: PatentRow[] }) {
  const [sortKey, setSortKey] = useState<SortKey>("patent_date");
  const [desc, setDesc] = useState(true);
  const [q, setQ] = useState("");

  const filtered = useMemo(() => {
    const qq = q.trim().toLowerCase();
    if (!qq) return rows;
    return rows.filter(r => r.patent_title.toLowerCase().includes(qq));
  }, [rows, q]);

  const sorted = useMemo(() => {
    const arr = [...filtered];
    arr.sort((a, b) => {
      if (sortKey === "patent_title") {
        return desc ? b.patent_title.localeCompare(a.patent_title) : a.patent_title.localeCompare(b.patent_title);
      }
      if (sortKey === "patent_date") {
        // ISO sorts lexicographically
        return desc ? b.patent_date.localeCompare(a.patent_date) : a.patent_date.localeCompare(b.patent_date);
      }
      const av = Number(a.patent_num_times_cited_by_us_patents || 0);
      const bv = Number(b.patent_num_times_cited_by_us_patents || 0);
      return desc ? (bv - av) : (av - bv);
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
          placeholder="Search patent titles..."
          style={{ padding: 10, width: 420 }}
        />
        <div style={{ opacity: 0.7 }}>
          Showing {sorted.length} / {rows.length}
        </div>
      </div>

      <div style={{ overflowX: "auto", border: "1px solid #ddd", borderRadius: 12 }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr>
              <Th label="Date" onClick={() => onSort("patent_date")} />
              <Th label="Cited By (US patents)" onClick={() => onSort("patent_num_times_cited_by_us_patents")} />
              <Th label="Title" onClick={() => onSort("patent_title")} />
              <th style={{ textAlign: "left", padding: 10, background: "#fafafa", borderBottom: "1px solid #eee" }}>
                CPC subclasses
              </th>
            </tr>
          </thead>
          <tbody>
            {sorted.map(r => (
              <tr key={r.patent_id} style={{ borderTop: "1px solid #eee" }}>
                <td style={{ padding: 10, whiteSpace: "nowrap" }}>{r.patent_date}</td>
                <td style={{ padding: 10 }}>{r.patent_num_times_cited_by_us_patents || "0"}</td>
                <td style={{ padding: 10 }}>
                  {r.patent_title}
                  <div style={{ fontSize: 12, opacity: 0.6 }}>Patent: {r.patent_id}</div>
                </td>
                <td style={{ padding: 10, fontFamily: "monospace", fontSize: 12 }}>
                  {(r.cpc_subclass_ids || "").split("|").filter(Boolean).join(", ")}
                </td>
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
