export default function Security() {
  const badges = ["GDPR aligned", "India DPDP", "Singapore PDPA", "AES-256 at rest", "TLS 1.2+ in transit"];
  const items = [
    { h3: "Enterprise-grade encryption", p: "TLS 1.2+ in transit, AES-256 at rest. Credentials and API keys are hashed and never stored in plaintext." },
    { h3: "Your data stays yours", p: "Customer documents are never sold, shared, or reused to train shared AI models. They're used only to deliver your results." },
    { h3: "Retention you control", p: "Set how long documents remain stored, per mailbox. Delete on delivery if you want — you decide the policy." },
    { h3: "Privacy regulations covered", p: "Designed to align with EU GDPR, India's DPDP Act and Singapore's PDPA across our operating regions." },
    { h3: "Audit trail on everything", p: "Every document, extraction and delivery is logged. Roles and permissions keep access on a need-to-know basis." },
    { h3: "Need more control? Self-host.", p: "The Enterprise Self-Hosted edition runs GdoczAI entirely inside your infrastructure — on-premise or air-gapped." },
  ];

  return (
    <section id="security" aria-label="Security and compliance">
      <div className="wrap">
        <div className="center">
          <span className="kicker">Security</span>
          <h2>Designed for privacy, backed by compliance</h2>
          <p className="lead">Your documents and data are sensitive. GdoczAI is built to keep them that way.</p>
          <div className="sec-badges d-flex flex-wrap justify-content-center gap-2 mt-4">
            {badges.map((b) => (
              <span className="badge" key={b}>{b}</span>
            ))}
          </div>
        </div>
        <div className="row row-cols-1 row-cols-md-2 row-cols-lg-3 g-4 mt-5">
          {items.map((it) => (
            <div className="col" key={it.h3}>
              <div className="sec-item h-100">
                <h3>{it.h3}</h3>
                <p>{it.p}</p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

