import Link from "next/link";
import { Button } from "antd";

export default function NotFound() {
  return (
    <div className="d-flex flex-column align-items-center justify-content-center min-vh-100 bg-light text-center p-4">
      <div className="card shadow-sm p-5" style={{ maxWidth: 480, borderRadius: 16 }}>
        <h1 className="display-4 fw-bold text-primary mb-3">404</h1>
        <h2 className="h4 fw-semibold mb-3">Page Not Found</h2>
        <p className="text-muted mb-4">
          The page you are looking for does not exist or has been moved.
        </p>
        <Button className="btn btn-primary px-4 py-2" type="primary" href="/">
          Return Home
        </Button>
      </div>
    </div>
  );
}
