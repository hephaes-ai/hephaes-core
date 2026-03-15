import { Link } from "react-router-dom";

export function InventoryPage() {
  return (
    <main className="page-layout">
      <section className="card page-panel">
        <p className="card-label">Inventory</p>
        <h2>Frontend bootstrap is ready for backend integration.</h2>
        <p>
          The next step will connect this page to `POST /assets/register` and `GET /assets` so the
          inventory becomes the real Backend Phase 1 verification surface.
        </p>
      </section>

      <section className="inventory-grid">
        <article className="card panel-card">
          <p className="card-label">Registration form placeholder</p>
          <h3>Register a `.bag` or `.mcap` file</h3>
          <p>
            This panel will become the path-based registration form that submits to the backend.
          </p>
        </article>

        <article className="card panel-card">
          <p className="card-label">Asset inventory placeholder</p>
          <h3>Inventory list</h3>
          <p>
            This panel will become the Phase 1 asset table with rows linked to asset detail pages.
          </p>
          <Link to="/assets/example-asset" className="inline-link">
            Open a placeholder detail route
          </Link>
        </article>
      </section>
    </main>
  );
}
