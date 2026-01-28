export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body style={{ margin: 0, fontFamily: "system-ui, -apple-system, Segoe UI, Roboto, sans-serif" }}>
        <div style={{ maxWidth: 1100, margin: "0 auto", padding: 24 }}>
          <header style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
            <a href="/" style={{ textDecoration: "none", color: "inherit" }}>
              <h1 style={{ margin: 0, fontSize: 20 }}>Patent Tracker</h1>
            </a>
            <nav style={{ display: "flex", gap: 14 }}>
              <a href="/biotech">Biotech/Pharma</a>
              <a href="/tech">Tech</a>
            </nav>
          </header>
          {children}
          <footer style={{ marginTop: 40, opacity: 0.7, fontSize: 12 }}>
            Data via PatentsView PatentSearch API.
          </footer>
        </div>
      </body>
    </html>
  );
}
