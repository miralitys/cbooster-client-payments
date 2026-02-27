import { useEffect, useRef, useState } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";

import { AssistantWidget } from "@/features/assistant/AssistantWidget";
import { getSession } from "@/shared/api/session";
import { canViewClientHealthSession, canViewClientMatchSession, isOwnerOrAdminSession } from "@/shared/lib/access";
import { ModalStackProvider, NotificationCenter, ToastHost } from "@/shared/ui";

interface NavigationItem {
  to: string;
  label: string;
  external?: boolean;
  visibility?: "owner-admin" | "owner-admin-or-accounting" | "owner-admin-or-client-service-head";
}

const NAV_ITEMS: NavigationItem[] = [
  { to: "/dashboard", label: "Dashboard" },
  { to: "/custom-dashboard", label: "Custom Dashboard" },
  { to: "/client-payments", label: "Client Payments" },
  { to: "/client-health", label: "Здоровье клиента", visibility: "owner-admin-or-client-service-head" },
  { to: "/clients", label: "Clients" },
  { to: "/client-match", label: "Client Match", visibility: "owner-admin-or-accounting" },
  { to: "/payment-probability", label: "Client Payment Probability" },
  { to: "/identityiq-score", label: "IdentityIQ Scores" },
  { to: "/ghl-contracts", label: "GHL Contract Text" },
  { to: "/quickbooks", label: "QuickBooks", visibility: "owner-admin" },
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

  if (pathname.startsWith("/client-match")) {
    return "Client Match";
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

  if (pathname.startsWith("/client-health")) {
    return "Здоровье клиента";
  }

  if (pathname.startsWith("/clients")) {
    return "Clients";
  }

  return "Client Payments";
}

export function Layout() {
  const location = useLocation();
  const [menuOpen, setMenuOpen] = useState(false);
  const [canViewOwnerAdminOnly, setCanViewOwnerAdminOnly] = useState(false);
  const [canViewClientMatch, setCanViewClientMatch] = useState(false);
  const [canViewClientHealth, setCanViewClientHealth] = useState(false);
  const [canViewNotifications, setCanViewNotifications] = useState(false);
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

  useEffect(() => {
    let active = true;

    void getSession()
      .then((payload) => {
        if (!active) {
          return;
        }
        const ownerOrAdmin = isOwnerOrAdminSession(payload);
        setCanViewOwnerAdminOnly(ownerOrAdmin);
        setCanViewClientMatch(canViewClientMatchSession(payload));
        setCanViewClientHealth(canViewClientHealthSession(payload));
        setCanViewNotifications(Boolean(payload?.permissions?.view_client_payments));
      })
      .catch(() => {
        if (!active) {
          return;
        }
        setCanViewOwnerAdminOnly(false);
        setCanViewClientMatch(false);
        setCanViewClientHealth(false);
        setCanViewNotifications(false);
      });

    return () => {
      active = false;
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

            <div className="page-header__controls">
              {canViewNotifications ? <NotificationCenter /> : null}

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
                  {NAV_ITEMS.filter((item) => {
                    if (item.visibility === "owner-admin") {
                      return canViewOwnerAdminOnly;
                    }
                    if (item.visibility === "owner-admin-or-accounting") {
                      return canViewClientMatch;
                    }
                    if (item.visibility === "owner-admin-or-client-service-head") {
                      return canViewClientHealth;
                    }
                    return true;
                  }).map((item) => {
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
