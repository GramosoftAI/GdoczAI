export default function UseCases() {
  const cases = [
    { href: "https://gramosoft.tech/gdoczai/accounts-payable", h3: "Accounts Payable", p: "Extract invoice and PO data automatically and post it to your accounting system with three-way matching." },
    { href: "https://gramosoft.tech/gdoczai/aviation", h3: "Aviation & Logistics", p: "Fuel, handling, catering and freight documents converted into structured data across carriers and currencies." },
    { href: "https://gramosoft.tech/gdoczai/automotive", h3: "Automotive Dealerships", p: "Vehicle invoices, job cards and OEM statements flow into your DMS and books — no re-keying across branches." },
    { href: "https://gramosoft.tech/gdoczai/insurance", h3: "Insurance & BFSI", p: "Claim forms, surveyor reports and KYC documents extracted into clean records for core systems." },
  ];

  return (
    <section id="use-cases" className="prod" aria-label="Use cases">
      <div className="wrap">
        <div className="center">
          <span className="kicker">Use cases</span>
          <h2>Top ways teams use GdoczAI</h2>
          <p className="lead">Teams across industries rely on GdoczAI to turn documents into structured data.</p>
        </div>
        <div className="uc-grid">
          {cases.map((c) => (
            <a className="uc" href={c.href} key={c.h3}>
              <h3>{c.h3}</h3>
              <p>{c.p}</p>
              <span className="more">Learn more →</span>
            </a>
          ))}
        </div>
      </div>
    </section>
  );
}
