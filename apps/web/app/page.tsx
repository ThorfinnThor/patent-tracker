export default function Home() {
  return (
    <main style={{ display: "grid", gap: 12 }}>
      <h2 style={{ margin: 0 }}>Overview</h2>
      <p style={{ margin: 0, opacity: 0.8 }}>
        Two sector views, each caching its own company list and per-company patent files.
        Rankings are based on patent count in the last 5 years, with re-sorting by citations and breadth.
      </p>
      <div style={{ display: "flex", gap: 12, marginTop: 12 }}>
        <a href="/biotech" style={cardStyle}>
          <div style={{ fontWeight: 600 }}>Biotech/Pharma</div>
          <div style={{ opacity: 0.7 }}>Top 200 companies</div>
        </a>
        <a href="/tech" style={cardStyle}>
          <div style={{ fontWeight: 600 }}>Tech</div>
          <div style={{ opacity: 0.7 }}>Top 200 companies</div>
        </a>
      </div>
    </main>
  );
}

const cardStyle: React.CSSProperties = {
  display: "block",
  padding: 16,
  border: "1px solid #ddd",
  borderRadius: 12,
  textDecoration: "none",
  color: "inherit",
  width: 240
};
