import { Link, useParams } from "react-router-dom";

export function AssetDetailPage() {
  const { assetId } = useParams<{ assetId: string }>();

  return (
    <main className="page-layout">
      <section className="card page-panel">
        <p className="card-label">Asset detail</p>
        <h2>{assetId || "Unknown asset"}</h2>
        <p>
          This route is ready for the real <code>GET /assets/{"{assetId}"}</code> integration in
          the next task.
        </p>
      </section>

      <section className="detail-grid">
        <article className="card detail-card">
          <p className="card-label">Backend field group</p>
          <h3>Base file information</h3>
          <p>File name, path, type, size, indexing status, and timestamps will render here.</p>
        </article>

        <article className="card detail-card">
          <p className="card-label">Future phase placeholders</p>
          <h3>Metadata, tags, conversions</h3>
          <p>These sections are intentionally reserved so the detail layout can grow cleanly.</p>
        </article>
      </section>

      <Link to="/" className="back-link">
        Return to inventory
      </Link>
    </main>
  );
}
