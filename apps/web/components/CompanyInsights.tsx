"use client";

import { useEffect, useMemo, useState } from "react";

type Row = { code: string; title: string; n: number };
type TrendRow = { code: string; title: string; prev_n: number; cur_n: number; delta: number; pct: number | null };
type SimpleRow = { company_id?: string; companyId?: string; display_name?: string; displayName?: string; score?: number; n?: number; name?: string };

export default function CompanyInsights({
  sector,
  companyId,
}: {
  sector: "biotech" | "tech";
  companyId: string;
}) {
  const [days, setDays] = useState(365);
  const [level, setLevel] = useState<"group" | "main_group" | "subclass" | "class">("group");
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const [data, setData] = useState<any>(null);

  const url = useMemo(() => {
    const qs = new URLSearchParams({
      sector,
      companyId,
      days: String(days),
      level,
    });
    return `/api/insights?${qs.toString()}`;
  }, [sector, companyId, days, level]);

  useEffect(() => {
    (async () => {
      setLoading(true);
      setErr("");
      try {
        const res = await fetch(url);
        const text = await res.text();
        if (!res.ok) throw new Error(text);
        setData(JSON.parse(text));
      } catch (e: any) {
        setData(null);
        setErr(e?.message || "Failed to load insights");
      } finally {
        setLoading(false);
      }
    })();
  }, [url]);

  const topCpc: Row[] = Array.isArray(data?.topCpc) ? data.topCpc : [];
  const trend: TrendRow[] = Array.isArray(data?.cpcTrend) ? data.cpcTrend : [];
  const competitors: SimpleRow[] = Array.isArray(data?.competitors) ? data.competitors : [];
  const coAssignees: SimpleRow[] = Array.isArray(data?.coAssignees) ? data.coAssignees : [];
  const inventors: SimpleRow[] = Array.isArray(data?.topInventors) ? data.topInventors : [];

  return (
    <div className="card cardPad" style={{ display: "grid", gap: 12 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
        <div style={{ fontWeight: 900, fontSize: 18 }}>Insights</div>

        <select className="select" value={String(days)} onChange={(e) => setDays(Number(e.target.value))}>
          <option value="90">Last 90 days</option>
          <option value="180">Last 180 days</option>
          <option value="365">Last 1 year</option>
          <option value="730">Last 2 years</option>
          <option value="1825">Last 5 years</option>
        </select>

        <select className="select" value={level} onChange={(e) => setLevel(e.target.value as any)}>
          <option value="group">CPC level: group</option>
          <option value="main_group">CPC level: main group</option>
          <option value="subclass">CPC level: subclass</option>
          <option value="class">CPC level: class</option>
        </select>

        <div className="small" style={{ marginLeft: "auto" }}>
          {loading ? "Loading…" : "Cached (1h)"} ·{" "}
          <a href={url} target="_blank" rel="noreferrer">
            open API
          </a>
        </div>
      </div>

      {err && (
        <div
          className="card"
          style={{
            padding: 12,
            borderColor: "rgba(239,68,68,0.5)",
            background: "rgba(239,68,68,0.10)",
          }}
        >
          <div style={{ fontWeight: 900, marginBottom: 6 }}>Insights error</div>
          <div className="small" style={{ whiteSpace: "pre-wrap" }}>
            {err}
          </div>
        </div>
      )}

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
        <Panel title="Top CPC topics">
          <MiniTable
            headers={["Code", "Title", "Count"]}
            rows={topCpc.map((r) => [r.code, r.title || "—", String(r.n)])}
          />
        </Panel>

        <Panel title="CPC momentum (vs previous window)">
          <MiniTable
            headers={["Code", "Title", "Prev", "Now", "Δ", "%"]}
            rows={trend.map((r) => [
              r.code,
              r.title || "—",
              String(r.prev_n),
              String(r.cur_n),
              String(r.delta),
              r.pct === null || r.pct === undefined ? "—" : `${r.pct}%`,
            ])}
          />
        </Panel>

        <Panel title="Top competitors (CPC overlap)">
          <MiniTable
            headers={["Company", "Score"]}
            rows={competitors.map((r) => [
              String(r.display_name ?? r.displayName ?? r.company_id ?? r.companyId ?? ""),
              String(r.score ?? ""),
            ])}
          />
          <div className="small" style={{ marginTop: 8 }}>
            Competitors are computed across the tracked set (top companies) by overlap on your top CPC groups.
          </div>
        </Panel>

        <Panel title="Co-assignees">
          <MiniTable
            headers={["Company", "Shared patents"]}
            rows={coAssignees.map((r) => [
              String(r.display_name ?? r.displayName ?? r.company_id ?? r.companyId ?? ""),
              String(r.n ?? ""),
            ])}
          />
          <div className="small" style={{ marginTop: 8 }}>
            Co-assignees are other tracked companies that appear on the same patent documents.
          </div>
        </Panel>

        <Panel title="Top inventors">
          <MiniTable
            headers={["Inventor", "Patents"]}
            rows={inventors.map((r) => [String(r.name ?? ""), String(r.n ?? "")])}
          />
          <div className="small" style={{ marginTop: 8 }}>
            Inventors are counted on patents where the company appears as an assignee.
          </div>
        </Panel>

        <Panel title="Notes">
          <div className="small" style={{ lineHeight: 1.5 }}>
            <ul style={{ margin: 0, paddingLeft: 18 }}>
              <li>CPC titles come from PatentsView CPC endpoints and are cached in Postgres.</li>
              <li>All insights are computed from cached weekly ingests (fast + deterministic).</li>
              <li>If you want competitors beyond the tracked set, we can add a live PatentsView “global competitor” mode later.</li>
            </ul>
          </div>
        </Panel>
      </div>
    </div>
  );
}

function Panel({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="card cardPad" style={{ display: "grid", gap: 10 }}>
      <div style={{ fontWeight: 900 }}>{title}</div>
      {children}
    </div>
  );
}

function MiniTable({ headers, rows }: { headers: string[]; rows: string[][] }) {
  return (
    <div className="tableWrap">
      <table className="table">
        <thead>
          <tr>
            {headers.map((h) => (
              <th key={h} className="th">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className="rowHover">
              {r.map((c, j) => (
                <td key={j} className="td">
                  <div className="small">{c}</div>
                </td>
              ))}
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="td" colSpan={headers.length}>
                <div className="small">No data.</div>
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
