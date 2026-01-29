"use client";

import { useEffect, useMemo, useState } from "react";

type PatentRow = {
  patent_id: string;
  patent_date: string;
  patent_title: string;
  cited_by: number;
  cpc_subclass_ids: string;
};

export default function CompanyPatentsViewer({
  sector,
  companyId
}: {
  sector: "biotech" | "tech";
  companyId: string;
}) {
  const [years, setYears] = useState<number[]>([]);
  const [year, setYear] = useState<string>(""); // "" = all years (bounded by cap)
  const [sort, setSort] = useState<"recent" | "cited">("recent");
  const [q, setQ] = useState("");
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(100);

  const cap = 500;

  const [loading, setLoading] = useState(false);
  const [total, setTotal] = useState(0);
  const [rows, setRows] = useState<PatentRow[]>([]);
  const [error, setError] = useState<string>("");

  const totalPages = useMemo(() => Math.max(1, Math.ceil(total / pageSize)), [total, pageSize]);
  const boundedMode = year === "";

  const patentsUrl = useMemo(() => {
    const params = new URLSearchParams({
      sector,
      companyId,
      sort,
      q,
      page: String(page),
      pageSize: String(pageSize),
      cap: String(cap),
    });
    if (year) params.set("year", year);
    return `/api/patents?${params.toString()}`;
  }, [sector, companyId, sort, q, page, pageSize, cap, year]);

  useEffect(() => {
    (async () => {
      setError("");
      try {
        const res = await fetch(`/api/years?sector=${sector}&companyId=${encodeURIComponent(companyId)}`);
        if (!res.ok) {
          const text = await res.text();
          throw new Error(`Years API failed (${res.status}): ${text}`);
        }
        const data = await res.json();
        setYears(Array.isArray(data.years) ? data.years : []);
      } catch (e: any) {
        setYears([]);
        setError(e?.message || "Failed to load years");
      }
    })();
  }, [sector, companyId]);

  useEffect(() => {
    setPage(0);
  }, [year, sort, q, pageSize]);

  useEffect(() => {
    (async () => {
      setLoading(true);
      setError("");
      try {
        const res = await fetch(patentsUrl);
        if (!res.ok) {
          const text = await res.text();
          throw new Error(`Patents API failed (${res.status}): ${text}`);
        }
        const data = await res.json();

        if (data?.error) {
          throw new Error(String(data.error));
        }

        setTotal(Number(data.total || 0));
        setRows(Array.isArray(data.rows) ? data.rows : []);
      } catch (e: any) {
        setTotal(0);
        setRows([]);
        setError(e?.message || "Failed to load patents");
      } finally {
        setLoading(false);
      }
    })();
  }, [patentsUrl]);

  return (
    <div className="card cardPad" style={{ display: "grid", gap: 12 }}>
      <div className="controls">
        <input
          className="input"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Search titles…"
          style={{ width: 320 }}
        />

        <select className="select" value={year} onChange={(e) => setYear(e.target.value)}>
          <option value="">All years (Top {cap})</option>
          {years.map((y) => (
            <option key={y} value={String(y)}>
              {y}
            </option>
          ))}
        </select>

        <button
          className={"btn " + (sort === "recent" ? "btnPrimary" : "")}
          onClick={() => setSort("recent")}
        >
          Most recent
        </button>
        <button
          className={"btn " + (sort === "cited" ? "btnPrimary" : "")}
          onClick={() => setSort("cited")}
        >
          Most cited
        </button>

        <select className="select" value={String(pageSize)} onChange={(e) => setPageSize(Number(e.target.value))}>
          <option value="50">50 / page</option>
          <option value="100">100 / page</option>
          <option value="200">200 / page</option>
        </select>

        <div className="small" style={{ marginLeft: "auto" }}>
          {loading ? "Loading…" : `${rows.length} rows · ${total} total`}
          {boundedMode ? ` · capped at ${cap}` : ""}
        </div>
      </div>

      {error && (
        <div
          className="card"
          style={{
            padding: 12,
            borderColor: "rgba(239,68,68,0.5)",
            background: "rgba(239,68,68,0.10)",
          }}
        >
          <div style={{ fontWeight: 800, marginBottom: 6 }}>Backend error</div>
          <div className="small" style={{ whiteSpace: "pre-wrap" }}>{error}</div>
          <div className="small" style={{ marginTop: 8 }}>
            Debug:{" "}
            <a href={patentsUrl} target="_blank" rel="noreferrer">
              Open patents API request
            </a>
          </div>
        </div>
      )}

      <div className="controls">
        <button className="btn" disabled={page <= 0} onClick={() => setPage((p) => Math.max(0, p - 1))}>
          ← Prev
        </button>
        <div className="small">
          Page {page + 1} / {totalPages}
        </div>
        <button className="btn" disabled={page + 1 >= totalPages} onClick={() => setPage((p) => p + 1)}>
          Next →
        </button>
      </div>

      <div className="tableWrap">
        <table className="table">
          <thead>
            <tr>
              <th className="th" style={{ width: 120 }}>Date</th>
              <th className="th" style={{ width: 120 }}>Cited by</th>
              <th className="th">Title</th>
              <th className="th" style={{ width: 280 }}>CPC subclasses</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.patent_id} className="rowHover">
                <td className="td" style={{ whiteSpace: "nowrap" }}>{r.patent_date}</td>
                <td className="td">{r.cited_by}</td>
                <td className="td">
                  <div style={{ fontWeight: 650 }}>{r.patent_title}</div>
                  <div className="small">Patent: {r.patent_id}</div>
                </td>
                <td className="td">
                  <span className="small" style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace" }}>
                    {r.cpc_subclass_ids ? r.cpc_subclass_ids.split("|").filter(Boolean).join(", ") : "—"}
                  </span>
                </td>
              </tr>
            ))}

            {!loading && rows.length === 0 && !error && (
              <tr>
                <td className="td" colSpan={4}>
                  <div className="small">No results.</div>
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <div className="small">
        Tip: “All years” is a bounded tracker view (Top {cap}). Select a year for full paginated browsing.
      </div>
    </div>
  );
}
