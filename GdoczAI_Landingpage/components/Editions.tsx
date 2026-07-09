import { Button } from "antd";

export default function Editions() {
  return (
    <section id="editions" aria-label="Cloud and self-hosted editions">
      <div className="wrap">
        <div className="center">
          <span className="kicker">Two editions</span>
          <h2>Start in the cloud. Self-host when you must.</h2>
          <p className="lead">Same extraction engine, two ways to run it.</p>
        </div>
        <div className="row gy-4 mt-5 justify-content-center">
          <div className="col-12 col-md-6 d-flex">
            <div className="ed featured w-100 d-flex flex-column justify-content-between">
              <div>
                <span className="flag">RECOMMENDED</span>
                <h3>GdoczAI Cloud</h3>
                <p className="sub">Fully managed SaaS. Sign up and start extracting today.</p>
                <ul className="ps-0 mb-4">
                  <li>Live in minutes — free plan, no credit card</li>
                  <li>Automatic scaling and updates, zero maintenance</li>
                  <li>All integrations, API and webhooks included</li>
                  <li>Encrypted storage with configurable retention</li>
                  <li>Pay as you grow with transparent volume tiers</li>
                </ul>
              </div>
              <Button className="btn btn-primary w-100" type="primary" href={`${process.env.NEXT_PUBLIC_APP_URL}/auth/sign_in`}>
                Start free on Cloud
              </Button>
            </div>
          </div>
          <div className="col-12 col-md-6 d-flex">
            <div className="ed w-100 d-flex flex-column justify-content-between">
              <div>
                <h3>Enterprise Self-Hosted</h3>
                <p className="sub">The same engine, deployed inside your infrastructure.</p>
                <ul className="ps-0 mb-4">
                  <li>On-premise, private cloud or fully air-gapped</li>
                  <li>Documents never leave your environment</li>
                  <li>Models fine-tuned on your document types</li>
                  <li>Flat annual license — no per-page pricing</li>
                  <li>Built for banks, insurers, airlines and regulated teams</li>
                </ul>
              </div>
              <Button className="btn btn-outline w-100" href={`${process.env.NEXT_PUBLIC_APP_URL}/auth/contact-us`}>
                {/* Talk to us about self-hosting */}
                Self-hosting Info
              </Button>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

