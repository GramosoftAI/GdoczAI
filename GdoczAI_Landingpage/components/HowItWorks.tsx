export default function HowItWorks() {
  const steps = [
    { n: 1, h3: "Teach", p: "Upload one sample document. GdoczAI's AI identifies the fields — vendor, totals, dates, line items. Review the suggestions and adjust anything that's off." },
    { n: 2, h3: "Send", p: "Forward emails to your GdoczAI mailbox, drag-and-drop files, watch a folder, or push documents through the REST API. Any source, any volume." },
    { n: 3, h3: "Deliver", p: "GdoczAI extracts, validates and delivers structured data to Google Sheets, your ERP or any app in real time — with every document tracked." },
  ];

  return (
    <section id="how-it-works" className="how" aria-label="How GdoczAI works">
      <div className="wrap">
        <div className="center">
          <span className="kicker">How it works</span>
          <h2>The simplest way to automate document data</h2>
          <p className="lead">Three steps between your inbox and your systems.</p>
        </div>
        <div className="row row-cols-1 row-cols-lg-3 g-4 mt-5">
          {steps.map((s) => (
            <div className="col" key={s.n}>
              <div className="how-card h-100">
                <div className="n">{s.n}</div>
                <h3>{s.h3}</h3>
                <p>{s.p}</p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

