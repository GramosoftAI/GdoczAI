"use client";

export default function Footer() {
  const handleScroll = (e: React.MouseEvent<HTMLAnchorElement>, id: string) => {
    e.preventDefault();
    const element = document.getElementById(id);
    if (element) {
      element.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  };

  return (
    <footer className="site-footer">
      <div className="wrap">
        <div className="foot-grid">
          <div>
            <a className="logo" href="https://gramosoft.tech">
              {/* <span className="mark">G</span>Gdocz<em>AI</em> */}
              <img src="/integration/GdoczAI.svg" alt="logo" style={{ width: "140px", height: "auto", display: "block" }} />
            </a>
            <p style={{ fontSize: 14, marginTop: 14, maxWidth: 300, color: "#8FA8C7" }}>
              AI document data extraction, cloud or self-hosted. Built by Gramosoft — Chennai.
            </p>
          </div>
          <div>
            <h4>Product</h4>
            <ul>
              <li><a href="#how-it-works" onClick={(e) => handleScroll(e, "how-it-works")}>How it works</a></li>
              <li><a href="#editions" onClick={(e) => handleScroll(e, "editions")}>Cloud &amp; Self-Hosted</a></li>
              {/* <li><a href="https://gramosoft.tech/gdoczai/pricing">Pricing</a></li> */}
              <li><a href="https://gramosoft.tech/gdoczai/docs">API docs</a></li>
            </ul>
          </div>
          <div>
            <h4>Extract data from</h4>
            <ul>
              <li><a href="https://gramosoft.tech/gdoczai/invoice-ocr">Invoices</a></li>
              <li><a href="https://gramosoft.tech/gdoczai/pdf-parser">PDFs</a></li>
              <li><a href="https://gramosoft.tech/gdoczai/email-parser">Emails</a></li>
              <li><a href="https://gramosoft.tech/gdoczai/bank-statements">Bank statements</a></li>
            </ul>
          </div>
          <div>
            <h4>Company</h4>
            <ul>
              <li><a href="https://gramosoft.tech/about">About Gramosoft</a></li>
              <li><a href="https://gramosoft.tech/gcrawlai">GcrawlAI</a></li>
              <li><a href="https://gramosoft.tech/gsearchai">GsearchAI</a></li>
              <li><a href="https://gramosoft.tech/privacy">Privacy policy</a></li>
            </ul>
          </div>
        </div>
        <div className="foot-bottom">
          <span>© 2026 Gramosoft Private Limited. All rights reserved.</span>
          <span>hello@gramosoft.tech</span>
        </div>
      </div>
    </footer>
  );
}
