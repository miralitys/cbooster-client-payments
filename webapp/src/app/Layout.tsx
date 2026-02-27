import { useEffect, useMemo, useRef, useState } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";

import { AssistantWidget } from "@/features/assistant/AssistantWidget";
import { getSession } from "@/shared/api/session";
import { canViewClientHealthSession, canViewClientMatchSession, isOwnerOrAdminSession } from "@/shared/lib/access";
import { ModalStackProvider, NotificationCenter, ToastHost } from "@/shared/ui";

interface NavigationItem {
  to: string;
  label: string;
  hint?: string;
  group: "operations" | "clients" | "analytics" | "system";
  external?: boolean;
  visibility?: "owner-admin" | "owner-admin-or-accounting" | "owner-admin-or-client-service-head";
}

const NAV_ITEMS: NavigationItem[] = [
  { to: "/dashboard", label: "Dashboard", hint: "Overview", group: "operations" },
  { to: "/custom-dashboard", label: "Custom Dashboard", hint: "Personalized", group: "operations" },
  { to: "/leads", label: "Leads", hint: "Pipeline", group: "operations" },
  { to: "/client-payments", label: "Client Payments", hint: "Collections", group: "clients" },
  { to: "/client-health", label: "Здоровье клиента", hint: "Health", group: "clients", visibility: "owner-admin-or-client-service-head" },
  { to: "/clients", label: "Clients", hint: "Directory", group: "clients" },
  { to: "/client-match", label: "Client Match", hint: "Matching", group: "clients", visibility: "owner-admin-or-accounting" },
  { to: "/payment-probability", label: "Client Payment Probability", hint: "Forecast", group: "analytics" },
  { to: "/identityiq-score", label: "IdentityIQ Scores", hint: "Scores", group: "analytics" },
  { to: "/ghl-contracts", label: "GHL Contract Text", hint: "Contracts", group: "analytics" },
  { to: "/quickbooks", label: "QuickBooks", hint: "Accounting", group: "system", visibility: "owner-admin" },
  { to: "/access-control", label: "Access Control", hint: "Roles", group: "system" },
];

const NAV_GROUPS: Array<{ key: NavigationItem["group"]; title: string }> = [
  { key: "operations", title: "Operations" },
  { key: "clients", title: "Clients" },
  { key: "analytics", title: "Analytics" },
  { key: "system", title: "System" },
];

const WEB_CSRF_COOKIE_NAME = "cbooster_auth_csrf";

function readCookieValueByName(name: string): string {
  if (typeof document === "undefined") {
    return "";
  }

  const rawCookie = String(document.cookie || "");
  if (!rawCookie) {
    return "";
  }

  const chunks = rawCookie.split(";");
  for (const chunk of chunks) {
    const [rawKey, ...rawValueParts] = chunk.split("=");
    if ((rawKey || "").trim() !== name) {
      continue;
    }
    const rawValue = rawValueParts.join("=").trim();
    if (!rawValue) {
      return "";
    }
    try {
      return decodeURIComponent(rawValue);
    } catch {
      return rawValue;
    }
  }

  return "";
}

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
  const visibleNavItems = useMemo(() => {
    return NAV_ITEMS.filter((item) => {
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
    });
  }, [canViewClientHealth, canViewClientMatch, canViewOwnerAdminOnly]);

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

  function handleLogoutClick() {
    setMenuOpen(false);

    const csrfToken = readCookieValueByName(WEB_CSRF_COOKIE_NAME);
    const form = document.createElement("form");
    form.method = "POST";
    form.action = "/logout";
    form.style.display = "none";

    const csrfInput = document.createElement("input");
    csrfInput.type = "hidden";
    csrfInput.name = "_csrf";
    csrfInput.value = csrfToken;
    form.appendChild(csrfInput);

    document.body.appendChild(form);
    form.submit();
  }

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
                  <div className="account-menu__groups">
                    {NAV_GROUPS.map((group) => {
                      const groupItems = visibleNavItems.filter((item) => item.group === group.key);
                      if (!groupItems.length) {
                        return null;
                      }

                      return (
                        <section key={group.key} className="account-menu__group" aria-label={group.title}>
                          <p className="account-menu__group-title">{group.title}</p>
                          <div className="account-menu__group-items">
                            {groupItems.map((item) => {
                              if (item.external) {
                                return (
                                  <a
                                    key={item.to}
                                    href={item.to}
                                    className="account-menu__item"
                                    role="menuitem"
                                    onClick={() => setMenuOpen(false)}
                                  >
                                    <span className="account-menu__item-label">{item.label}</span>
                                    {item.hint ? <span className="account-menu__item-hint">{item.hint}</span> : null}
                                  </a>
                                );
                              }

                              return (
                                <NavLink
                                  key={item.to}
                                  to={item.to}
                                  className={({ isActive }) => `account-menu__item ${isActive ? "is-active" : ""}`.trim()}
                                  role="menuitem"
                                  onClick={() => setMenuOpen(false)}
                                >
                                  <span className="account-menu__item-label">{item.label}</span>
                                  {item.hint ? <span className="account-menu__item-hint">{item.hint}</span> : null}
                                </NavLink>
                              );
                            })}
                          </div>
                        </section>
                      );
                    })}
                  </div>
                  <div className="account-menu__divider" aria-hidden="true" />
                  <button
                    type="button"
                    className="account-menu__item account-menu__item--logout"
                    role="menuitem"
                    onClick={handleLogoutClick}
                  >
                    <span className="account-menu__item-label">Log Out</span>
                    <span className="account-menu__item-hint">Exit session</span>
                  </button>
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
