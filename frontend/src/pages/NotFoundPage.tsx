import { Link } from "react-router-dom";

export function NotFoundPage() {
  return (
    <main className="page-layout">
      <section className="card page-panel">
        <p className="card-label">404</p>
        <h2>That route does not exist yet.</h2>
        <p>Use the inventory view to get back to the current Phase 1 workflow.</p>
        <Link to="/" className="back-link">
          Go to inventory
        </Link>
      </section>
    </main>
  );
}
