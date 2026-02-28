import { Navigate, Route, Routes } from "react-router-dom";

import { Layout } from "@/app/Layout";
import { OwnerAdminRoute } from "@/app/OwnerAdminRoute";
import { OwnerAdminOrClientServiceHeadRoute } from "@/app/OwnerAdminOrClientServiceHeadRoute";
import { OwnerAdminOrAccountingRoute } from "@/app/OwnerAdminOrAccountingRoute";
import AccessControlPage from "@/features/access-control/page";
import ClientsPage from "@/features/clients/page";
import ClientHealthPage from "@/features/client-health/page";
import ClientMatchPage from "@/features/client-match/page";
import ClientPaymentsPage from "@/features/client-payments/page";
import ClientScorePage from "@/features/client-score/page";
import CustomDashboardPage from "@/features/custom-dashboard/page";
import DashboardPage from "@/features/dashboard/page";
import GhlContractsPage from "@/features/ghl-contracts/page";
import IdentityIqScorePage from "@/features/identityiq-score/page";
import LeadsPage from "@/features/leads/page";
import ClientManagerKpiPage from "@/features/client-manager-kpi/page";
import QuickBooksPage from "@/features/quickbooks/page";
import SupportPage from "@/features/support/page";

export function AppRouter() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Navigate to="client-payments" replace />} />
        <Route path="client-payments" element={<ClientPaymentsPage />} />
        <Route
          path="client-health"
          element={(
            <OwnerAdminOrClientServiceHeadRoute>
              <ClientHealthPage />
            </OwnerAdminOrClientServiceHeadRoute>
          )}
        />
        <Route path="client-score" element={<ClientScorePage />} />
        <Route path="payment-probability" element={<ClientScorePage />} />
        <Route path="identityiq-score" element={<IdentityIqScorePage />} />
        <Route path="ghl-contracts" element={<GhlContractsPage />} />
        <Route
          path="kpi-client-manager"
          element={(
            <OwnerAdminRoute>
              <ClientManagerKpiPage />
            </OwnerAdminRoute>
          )}
        />
        <Route path="dashboard" element={<DashboardPage />} />
        <Route path="custom-dashboard" element={<CustomDashboardPage />} />
        <Route path="clients" element={<ClientsPage />} />
        <Route
          path="support"
          element={(
            <OwnerAdminRoute>
              <SupportPage />
            </OwnerAdminRoute>
          )}
        />
        <Route
          path="client-match"
          element={(
            <OwnerAdminOrAccountingRoute>
              <ClientMatchPage />
            </OwnerAdminOrAccountingRoute>
          )}
        />
        <Route
          path="quickbooks"
          element={(
            <OwnerAdminRoute>
              <QuickBooksPage />
            </OwnerAdminRoute>
          )}
        />
        <Route
          path="quickbooks-payments"
          element={(
            <OwnerAdminRoute>
              <QuickBooksPage />
            </OwnerAdminRoute>
          )}
        />
        <Route path="client-managers" element={<Navigate to="/client-payments" replace />} />
        <Route path="leads" element={<LeadsPage />} />
        <Route path="access-control" element={<AccessControlPage />} />
      </Route>
      <Route path="*" element={<Navigate to="/client-payments" replace />} />
    </Routes>
  );
}
