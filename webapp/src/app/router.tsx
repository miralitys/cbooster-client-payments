import { Navigate, Route, Routes } from "react-router-dom";

import { Layout } from "@/app/Layout";
import AccessControlPage from "@/features/access-control/page";
import ClientPaymentsPage from "@/features/client-payments/page";
import ClientScorePage from "@/features/client-score/page";
import ClientManagersPage from "@/features/client-managers/page";
import CustomDashboardPage from "@/features/custom-dashboard/page";
import DashboardPage from "@/features/dashboard/page";
import GhlContractsPage from "@/features/ghl-contracts/page";
import QuickBooksPage from "@/features/quickbooks/page";

export function AppRouter() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Navigate to="client-payments" replace />} />
        <Route path="client-payments" element={<ClientPaymentsPage />} />
        <Route path="client-score" element={<ClientScorePage />} />
        <Route path="payment-probability" element={<ClientScorePage />} />
        <Route path="dashboard" element={<DashboardPage />} />
        <Route path="custom-dashboard" element={<CustomDashboardPage />} />
        <Route path="quickbooks" element={<QuickBooksPage />} />
        <Route path="quickbooks-payments" element={<QuickBooksPage />} />
        <Route path="client-managers" element={<ClientManagersPage />} />
        <Route path="ghl-contracts" element={<GhlContractsPage />} />
        <Route path="access-control" element={<AccessControlPage />} />
      </Route>
      <Route path="*" element={<Navigate to="/client-payments" replace />} />
    </Routes>
  );
}
