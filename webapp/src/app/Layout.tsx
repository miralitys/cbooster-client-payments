import { NavLink, Outlet, useLocation } from "react-router-dom";

const NAV_ITEMS = [
  { to: "/client-payments", label: "Client Payments" },
  { to: "/dashboard", label: "Dashboard" },
  { to: "/quickbooks", label: "QuickBooks" },
];

const PAGE_TITLES: Record<string, string> = {
  "/client-payments": "Client Payments",
  "/dashboard": "Dashboard",
  "/quickbooks": "QuickBooks",
};

function resolvePageTitle(pathname: string): string {
  if (pathname.startsWith("/client-payments")) {
    return PAGE_TITLES["/client-payments"];
  }

  if (pathname.startsWith("/dashboard")) {
    return PAGE_TITLES["/dashboard"];
  }

  if (pathname.startsWith("/quickbooks")) {
    return PAGE_TITLES["/quickbooks"];
  }

  return "Client Payments";
}

export function Layout() {
  const location = useLocation();
  const pageTitle = resolvePageTitle(location.pathname);

  return (
    <div className="app-shell">
      <aside className="app-sidebar">
        <div className="app-brand">
          <span className="app-brand__eyebrow">Credit Booster</span>
          <strong className="app-brand__title">Web App</strong>
        </div>

        <nav className="app-nav" aria-label="Application pages">
          {NAV_ITEMS.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) =>
                `app-nav__link ${isActive ? "is-active" : ""}`.trim()
              }
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
      </aside>

      <div className="app-main">
        <header className="app-header">
          <div>
            <p className="app-header__label">CREDIT BOOSTER</p>
            <h1 className="app-header__title">{pageTitle}</h1>
          </div>
        </header>

        <main className="app-content">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
