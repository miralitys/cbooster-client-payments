import { useEffect } from "react";
import { Navigate, Route, Routes } from "react-router-dom";

import { Layout } from "@/app/Layout";
import ClientPaymentsPage from "@/features/client-payments/page";
import { Card } from "@/shared/ui";

function LegacyRouteRedirect({ targetPath, title }: { targetPath: string; title: string }) {
  useEffect(() => {
    window.location.assign(targetPath);
  }, [targetPath]);

  return (
    <Card title={title} subtitle="Opening full page">
      <p className="app-placeholder-copy">Redirecting to {targetPath}...</p>
    </Card>
  );
}

export function AppRouter() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Navigate to="client-payments" replace />} />
        <Route path="client-payments" element={<ClientPaymentsPage />} />
        <Route path="dashboard" element={<LegacyRouteRedirect title="Dashboard" targetPath="/dashboard" />} />
        <Route
          path="quickbooks"
          element={<LegacyRouteRedirect title="QuickBooks" targetPath="/quickbooks-payments" />}
        />
      </Route>
      <Route path="*" element={<Navigate to="/client-payments" replace />} />
    </Routes>
  );
}
