import Image from "next/image";
import { integrations } from "./integration";


function IntegrationList() {
  return (
    <div className="integration-wrapper">
      {integrations.map((item) => (
        <div className="integration-card" key={item.name} title={item.name}>
          <Image
            src={item.icon}
            alt={item.name}
            width={36}
            height={36}
            style={{ objectFit: "contain" }}
          />
        </div>
      ))}
    </div>
  );
}



export default function Integrations() {
  const highlighted = ["Google Sheets", "Odoo", "ERPNext", "Tally", "SAP"];
  const rest = ["Zoho Books", "QuickBooks", "Excel", "Zapier", "Make", "n8n", "Slack", "Webhooks", "REST API"];

  return (
    <section aria-label="Integrations">
      <div className="wrap center">
        <span className="kicker">Integrations</span>
        <h2>Built to fit in your workflow</h2>
        <p className="lead">GdoczAI connects to the tools your team already uses. No custom connectors needed.</p>
        {/* <div className="int-cloud">
          {highlighted.map((i) => (
            <span className="int hl" key={i}>{i}</span>
          ))}
          {rest.map((i) => (
            <span className="int" key={i}>{i}</span>
          ))}
        </div> */}
        <IntegrationList />
      </div>
    </section>
  );
}



