import { Button } from "antd";

export default function Hero() {
  return (
    <header className="hero gs-hero">
      <div className="wrap">
        <div className="row align-items-center gy-5">
          <div className="col-12 col-lg-6">
            <h1>
              Messy documents in.<br />
              <span>Clean data out.</span><br />
              Automatically.
            </h1>
            <p className="lead">
              Go live in minutes on GdoczAI Cloud. AI data extraction that turns invoices, PDFs,
              emails and scans into production-ready data — no model training, no dataset prep.
            </p>
            <div className="hero-ctas gs-hero-actions d-flex flex-wrap gap-3 mt-4">
              <Button className="btn btn-primary" type="primary" href={`${process.env.NEXT_PUBLIC_APP_URL}/auth/demo`}>
                Upload a document — free
              </Button>
              <Button className="btn btn-outline" href={`${process.env.NEXT_PUBLIC_APP_URL}/auth/contact-us`}>
                Book a demo
              </Button>
            </div>
            <div className="hero-proof d-flex align-items-center gap-3 mt-4">
              <div className="avatars" aria-hidden="true">
                <i></i><i></i><i></i><i></i><i></i>
              </div>
              <small>
                <span className="stars">★★★★★</span><br />
                Trusted by finance &amp; ops teams processing 100K+ documents a month
              </small>
            </div>
          </div>

          <div className="col-12 col-lg-6">
            <div className="demo w-100" aria-hidden="true">
              <div className="bar d-flex justify-content-between align-items-center mb-3">
                <b>invoice-2026-0448.pdf</b>
                <span className="live">PROCESSING LIVE</span>
              </div>
              <div className="demo-doc">
                <div className="row ok mx-0 px-2 py-2 d-flex justify-content-between mb-2">
                  <span>Vendor</span>
                  <span>Skyline Aero Services</span>
                </div>
                <div className="row ok mx-0 px-2 py-2 d-flex justify-content-between mb-2">
                  <span>Invoice No.</span>
                  <span>INV-2026-0448</span>
                </div>
                <div className="row ok mx-0 px-2 py-2 d-flex justify-content-between mb-2">
                  <span>Due date</span>
                  <span>15 Aug 2026</span>
                </div>
                <div className="row ok mx-0 px-2 py-2 d-flex justify-content-between mb-2">
                  <span>Line items</span>
                  <span>17 rows extracted</span>
                </div>
                <div className="row ok mx-0 px-2 py-2 d-flex justify-content-between">
                  <span>Total</span>
                  <span>₹4,82,600.00 ✓</span>
                </div>
              </div>
              <div className="demo-out d-flex flex-wrap gap-2 mt-3">
                <span className="sent">→ Google Sheets · delivered</span>
                <span>→ Odoo</span>
                <span>→ Webhook</span>
              </div>
              <div className="demo-count text-end mt-3">
                <b>13,206</b> documents processed this week
              </div>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}

