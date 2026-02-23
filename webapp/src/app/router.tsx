import { Navigate, Route, Routes } from "react-router-dom";

import { Layout } from "@/app/Layout";
import AccessControlPage from "@/features/access-control/page";
import ClientsPage from "@/features/clients/page";
import ClientPaymentsPage from "@/features/client-payments/page";
import ClientScorePage from "@/features/client-score/page";
import CustomDashboardPage from "@/features/custom-dashboard/page";
import DashboardPage from "@/features/dashboard/page";
import GhlContractsPage from "@/features/ghl-contracts/page";
import IdentityIqScorePage from "@/features/identityiq-score/page";
import LeadsPage from "@/features/leads/page";
import QuickBooksPage from "@/features/quickbooks/page";

export function AppRouter() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Navigate to="client-payments" replace />} />
        <Route path="client-payments" element={<ClientPaymentsPage />} />
        <Route path="client-score" element={<ClientScorePage />} />
        <Route path="payment-probability" element={<ClientScorePage />} />
        <Route path="identityiq-score" element={<IdentityIqScorePage />} />
        <Route path="ghl-contracts" element={<GhlContractsPage />} />
        <Route path="dashboard" element={<DashboardPage />} />
        <Route path="custom-dashboard" element={<CustomDashboardPage />} />
        <Route path="clients" element={<ClientsPage />} />
        <Route path="quickbooks" element={<QuickBooksPage />} />
        <Route path="quickbooks-payments" element={<QuickBooksPage />} />
        <Route path="client-managers" element={<Navigate to="/client-payments" replace />} />
        <Route path="leads" element={<LeadsPage />} />
        <Route path="access-control" element={<AccessControlPage />} />
      </Route>
      <Route path="*" element={<Navigate to="/client-payments" replace />} />
    </Routes>
  );
}
