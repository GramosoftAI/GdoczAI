import { Button } from "antd";

export default function CtaFinal() {
  return (
    <div className="cta gs-final">
      <div className="wrap">
        <span className="kicker">Get started</span>
        <h2>Ready to remove manual work<br />from your operations?</h2>
        <p className="lead">Start free in minutes and see how GdoczAI fits into your workflow.</p>
        <div className="d-flex flex-wrap justify-content-center gap-3 mt-4">
          <Button className="btn btn-primary" type="primary" href={`${process.env.NEXT_PUBLIC_APP_URL}/auth/sign_in`}>
            Sign up for free
          </Button>
          <Button className="btn btn-outline" href={`${process.env.NEXT_PUBLIC_APP_URL}/auth/contact-us`}>
            Book a demo
          </Button>
        </div>
        <div className="checks">
          <span>No model training required</span>
          <span>Free plan, no credit card</span>
          <span>Self-hosted edition available</span>
        </div>
      </div>
    </div>
  );
}
