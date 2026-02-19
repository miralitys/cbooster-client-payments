import { Navigate, Route, Routes } from "react-router-dom";

import { Layout } from "@/app/Layout";
import ClientPaymentsPage from "@/features/client-payments/page";
import { Card } from "@/shared/ui";

function PlaceholderPage({ title, description }: { title: string; description: string }) {
  return (
    <Card title={title} subtitle={description}>
      <p className="app-placeholder-copy">This section is scaffolded for the React migration.</p>
    </Card>
  );
}

export function AppRouter() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Navigate to="client-payments" replace />} />
        <Route path="client-payments" element={<ClientPaymentsPage />} />
        <Route
          path="dashboard"
          element={<PlaceholderPage title="Dashboard" description="Placeholder route for the new SPA." />}
        />
        <Route
          path="quickbooks"
          element={<PlaceholderPage title="QuickBooks" description="Placeholder route for the new SPA." />}
        />
      </Route>
      <Route path="*" element={<Navigate to="/client-payments" replace />} />
    </Routes>
  );
}
