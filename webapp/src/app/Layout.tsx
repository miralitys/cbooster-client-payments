import { useEffect, useRef, useState } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";

import { AssistantWidget } from "@/features/assistant/AssistantWidget";
import { ModalStackProvider, ToastHost } from "@/shared/ui";

const NAV_ITEMS = [
  { to: "/dashboard", label: "Dashboard" },
  { to: "/client-payments", label: "Client Payments" },
  { to: "/quickbooks", label: "QuickBooks" },
  { to: "/client-managers", label: "Client Managers" },
  { to: "/ghl-contracts", label: "GHL Documents" },
  { to: "/access-control", label: "Access Control" },
  { to: "/legacy/client-payments", label: "Legacy Client Payments", external: true },
  { to: "/legacy/dashboard", label: "Legacy Dashboard", external: true },
];

function resolvePageTitle(pathname: string): string {
  if (pathname.startsWith("/dashboard")) {
    return "Dashboard";
  }

  if (pathname.startsWith("/quickbooks")) {
    return "QuickBooks";
  }

  if (pathname.startsWith("/client-managers")) {
    return "Client - Manager Test";
  }

  if (pathname.startsWith("/ghl-contracts")) {
    return "GHL Documents Test";
  }

  if (pathname.startsWith("/access-control")) {
    return "Access Control";
  }

  return "Client Payments Dashboard";
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
