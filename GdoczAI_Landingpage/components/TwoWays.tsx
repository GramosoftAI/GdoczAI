import { Button } from "antd";

export default function TwoWays() {
  return (
    <section className="prod" aria-label="Web app and API">
      <div className="wrap">
        <div className="center">
          <span className="kicker">Two ways to work</span>
          <h2>Web app for Ops. API for Developers.</h2>
        </div>
        <div className="row gy-4 mt-5 justify-content-center">
          <div className="col-12 col-md-6 d-flex">
            <div className="way w-100 d-flex flex-column justify-content-between">
              <div>
                <h3>Web App</h3>
                <p>
                  Set up extraction, monitor workflows and manage operations from a simple interface.
                  No code required — finance and ops teams run it themselves.
                </p>
              </div>
              <Button className="btn btn-primary w-100" type="primary" href={`${process.env.NEXT_PUBLIC_APP_URL}/auth/sign_in`}>
                Try the web app
              </Button>
            </div>
          </div>
          <div className="col-12 col-md-6 d-flex">
            <div className="way dev w-100 d-flex flex-column justify-content-between">
              <div>
                <h3>API</h3>
                <p>
                  Programmatically send documents, receive structured JSON and embed extraction into
                  your own products and systems.
                </p>
                <code>
                  POST /api/v1/parsers/:id/upload<br />
                  → {`{ "vendor": "Skyline Aero", "total": 482600.00 }`}
                </code>
              </div>
              <Button className="btn btn-white w-100" href="https://gramosoft.tech/gdoczai/docs">
                Explore the API
              </Button>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

