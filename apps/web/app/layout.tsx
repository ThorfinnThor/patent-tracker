import "./globals.css";

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <div className="container">
          <header className="header">
            <div className="brand">
              <div className="logo" />
              <div>
                <a href="/" style={{ textDecoration: "none" }}>
                  <h1 className="h1">Patent Tracker</h1>
                </a>
                <div className="small">Top companies · 5-year window · weekly refresh</div>
              </div>
            </div>

            <nav className="nav">
              <a className="pill" href="/biotech">Biotech/Pharma</a>
              <a className="pill" href="/tech">Tech</a>
            </nav>
          </header>

          {children}

          <footer className="footer">
            Data via PatentsView. Company identity uses assignee IDs + optional manual rollups.
          </footer>
        </div>
      </body>
    </html>
  );
}
