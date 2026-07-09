export default function Production() {
  const rows = [
    { idx: "01", cat: "Reliability", h3: "Depend on it for critical workflows", p: "When document processing powers daily operations, downtime isn't an option. GdoczAI Cloud is designed for high availability so your workflows keep running." },
    { idx: "02", cat: "Scalability", h3: "Scale from ten documents to a hundred thousand", p: "Throughput adjusts automatically with your volume — a handful of files a day or aviation-scale monthly loads — with no infrastructure changes on your end." },
    { idx: "03", cat: "Adaptability", h3: "Handle layout changes effortlessly", p: "Extraction is instruction-based, not trained on fixed samples, so the AI absorbs layout variations automatically. New document type? Adjust fields in seconds, not hours." },
    { idx: "04", cat: "Formatting", h3: "Get data your systems can actually use", p: "Dates, numbers, currencies and GST fields are normalized to the format your systems expect. Define it once — receive standardized data every time." },
    { idx: "05", cat: "Exports", h3: "Move data where it belongs, instantly", p: "Download CSV, Excel or JSON. Push in real time to Google Sheets, Odoo, ERPNext, SAP, Tally or your own systems via webhooks and REST API." },
    { idx: "06", cat: "Monitoring", h3: "Track every document, even at volume", p: "Detailed audit logs, per-document status and error alerts give full visibility. Nothing fails silently, whether you process hundreds or hundreds of thousands." },
  ];

  return (
    <section className="prod" aria-label="Production capabilities">
      <div className="wrap">
        <div className="center">
          <span className="kicker">Built for production</span>
          <h2>Production-ready from day one</h2>
          <p className="lead">Everything you need to run document automation at scale.</p>
        </div>
        <div className="prod-list">
          {rows.map((r) => (
            <div className="prod-row" key={r.idx}>
              <div className="idx">{r.idx}</div>
              <div className="cat">{r.cat}</div>
              <div>
                <h3>{r.h3}</h3>
                <p>{r.p}</p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
