import { useEffect, useRef, useState } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";

import { AssistantWidget } from "@/features/assistant/AssistantWidget";
import { ModalStackProvider, ToastHost } from "@/shared/ui";

interface NavigationItem {
  to: string;
  label: string;
  external?: boolean;
}

const NAV_ITEMS: NavigationItem[] = [
  { to: "/dashboard", label: "Dashboard" },
  { to: "/custom-dashboard", label: "Custom Dashboard" },
  { to: "/client-payments", label: "Client Payments" },
  { to: "/payment-probability", label: "Client Payment Probability" },
  { to: "/identityiq-score", label: "IdentityIQ Scores" },
  { to: "/ghl-contracts", label: "GHL Contract Text" },
  { to: "/quickbooks", label: "QuickBooks" },
  { to: "/leads", label: "Leads" },
  { to: "/access-control", label: "Access Control" },
];

function resolvePageTitle(pathname: string): string {
  if (pathname.startsWith("/dashboard")) {
    return "Dashboard";
  }

  if (pathname.startsWith("/custom-dashboard")) {
    return "Custom Dashboard";
  }

  if (pathname.startsWith("/quickbooks")) {
    return "QuickBooks";
  }

  if (pathname.startsWith("/leads")) {
    return "Leads";
  }

  if (pathname.startsWith("/payment-probability") || pathname.startsWith("/client-score")) {
    return "Client Payment Probability";
  }

  if (pathname.startsWith("/identityiq-score")) {
    return "IdentityIQ Scores";
  }

  if (pathname.startsWith("/ghl-contracts")) {
    return "GHL Contract Text";
  }
  if (pathname.startsWith("/access-control")) {
    return "Access Control";
  }

  if (pathname.startsWith("/client-payments")) {
    return "Client Payments";
  }

  return "Client Payments";
}

export function Layout() {
  const location = useLocation();
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement | null>(null);
  const pageTitle = resolvePageTitle(location.pathname);

  useEffect(() => {
    function onPointerDown(event: MouseEvent) {
      if (!menuRef.current) {
        return;
      }

      const target = event.target;
      if (target instanceof Node && !menuRef.current.contains(target)) {
        setMenuOpen(false);
      }
    }

    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setMenuOpen(false);
      }
    }

    document.addEventListener("mousedown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("mousedown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, []);

  return (
    <ModalStackProvider>
      <main className="page-shell">
        <div className="container">
          <header className="section page-header">
            <div className="page-header__title">
              <p className="eyebrow">Credit Booster</p>
              <h1>{pageTitle}</h1>
            </div>

            <div ref={menuRef} className={`account-menu ${menuOpen ? "is-open" : ""}`.trim()}>
              <button
                type="button"
                className="account-menu__toggle"
                aria-haspopup="menu"
                aria-expanded={menuOpen}
                aria-controls="app-account-menu-panel"
                aria-label="Open account menu"
                onClick={() => setMenuOpen((prev) => !prev)}
              >
                <span className="account-menu__line" aria-hidden="true" />
                <span className="account-menu__line" aria-hidden="true" />
                <span className="account-menu__line" aria-hidden="true" />
              </button>

              <div id="app-account-menu-panel" className="account-menu__panel" role="menu" hidden={!menuOpen}>
                {NAV_ITEMS.map((item) => {
                  if (item.external) {
                    return (
                      <a
                        key={item.to}
                        href={item.to}
                        className="account-menu__item"
                        role="menuitem"
                        onClick={() => setMenuOpen(false)}
                      >
                        {item.label}
                      </a>
                    );
                  }

                  return (
                    <NavLink
                      key={item.to}
                      to={item.to}
                      className="account-menu__item"
                      role="menuitem"
                      onClick={() => setMenuOpen(false)}
                    >
                      {item.label}
                    </NavLink>
                  );
                })}
                <div className="account-menu__divider" aria-hidden="true" />
                <a href="/logout" className="account-menu__item" role="menuitem" onClick={() => setMenuOpen(false)}>
                  Log Out
                </a>
              </div>
            </div>
          </header>

          <Outlet />
        </div>
        <ToastHost />
        <AssistantWidget />
      </main>
    </ModalStackProvider>
  );
}
