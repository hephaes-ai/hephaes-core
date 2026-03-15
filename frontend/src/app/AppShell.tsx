import { NavLink, Outlet } from "react-router-dom";

import { ConnectionStatus } from "../components/ConnectionStatus";
import { FeedbackViewport } from "../components/FeedbackViewport";

export function AppShell() {
  return (
    <div className="shell">
      <header className="shell-header">
        <div>
          <p className="eyebrow">Hephaes</p>
          <h1 className="shell-title">Local asset workflows for ROS logs.</h1>
        </div>

        <nav className="shell-nav" aria-label="Primary">
          <NavLink
            to="/"
            end
            className={({ isActive }) => (isActive ? "nav-link nav-link-active" : "nav-link")}
          >
            Inventory
          </NavLink>
        </nav>
      </header>

      <section className="status-grid">
        <ConnectionStatus />
        <article className="card status-card">
          <h2>Phase 1 focus</h2>
          <p>Use the frontend to validate the real Backend Phase 1 routes end to end.</p>
        </article>
      </section>

      <Outlet />
      <FeedbackViewport />
    </div>
  );
}
