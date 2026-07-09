export default function Results() {
  const cards = [
    { big: "150+ HOURS SAVED / MONTH", h3: "Hire software, not headcount", p: "Teams handle growing document volume without adding data-entry staff — that's lakhs in labor cost recovered every year." },
    { big: "LIVE IN MINUTES", h3: "No training, no waiting on IT", p: "Most teams extract their first document within 10 minutes. Define your fields, forward a document, watch the data arrive." },
    { big: "FEWER ERRORS DOWNSTREAM", h3: "Clean data before it hits your books", p: "One wrong invoice number delays a payment by days. GdoczAI normalizes dates, amounts and references before delivery." },
    { big: "BUILT FOR SCALE", h3: "From 10 documents to 100,000+", p: "Automatic scaling, real-time integrations and full audit logs. Nothing fails silently — even at aviation-scale volume." },
  ];

  return (
    <section aria-label="Results">
      <div className="wrap">
        <div className="center">
          <span className="kicker">Real results</span>
          <h2>Stop copy-pasting. Start automating.</h2>
          <p className="lead">
            Every day, teams re-type data from invoices, emails and documents into their tools.
            GdoczAI does it automatically — with fewer errors and zero extra hires.
          </p>
        </div>
        <div className="results-grid">
          {/* EDIT ME: replace with a real customer quote once approved */}
          <div className="quote-card">
            <blockquote>
              &quot;We were re-keying thousands of supplier invoices across three entities every
              month. With GdoczAI the data lands in our ERP the same day it arrives — validated,
              matched to POs, with an audit trail on every document.&quot;
            </blockquote>
            <div className="who">
              <b>Finance Operations Lead</b>Aviation group · 100K+ documents/month
            </div>
          </div>
          {cards.map((c) => (
            <div className="result-card" key={c.h3}>
              <span className="big">{c.big}</span>
              <h3>{c.h3}</h3>
              <p>{c.p}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
