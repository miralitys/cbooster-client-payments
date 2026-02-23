import type { ReactElement } from "react";
import { useEffect, useState } from "react";
import { Navigate, useLocation } from "react-router-dom";

import { getSession } from "@/shared/api/session";
import { isOwnerOrAdminSession } from "@/shared/lib/access";

interface OwnerAdminRouteProps {
  children: ReactElement;
}

export function OwnerAdminRoute({ children }: OwnerAdminRouteProps) {
  const location = useLocation();
  const [access, setAccess] = useState<"checking" | "allowed" | "denied">("checking");

  useEffect(() => {
    let active = true;

    void getSession()
      .then((session) => {
        if (!active) {
          return;
        }
        setAccess(isOwnerOrAdminSession(session) ? "allowed" : "denied");
      })
      .catch(() => {
        if (!active) {
          return;
        }
        setAccess("denied");
      });

    return () => {
      active = false;
    };
  }, []);

  if (access === "checking") {
    return (
      <section className="section">
        <p className="dashboard-message">Checking access...</p>
      </section>
    );
  }

  if (access !== "allowed") {
    return <Navigate to="/client-payments" replace state={{ deniedFrom: location.pathname }} />;
  }

  return children;
}
