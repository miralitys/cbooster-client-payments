import { Navigate, Route, Routes } from "react-router-dom";

import { Layout } from "@/app/Layout";
import ClientPaymentsPage from "@/features/client-payments/page";
import DashboardPage from "@/features/dashboard/page";
import QuickBooksPage from "@/features/quickbooks/page";

export function AppRouter() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Navigate to="client-payments" replace />} />
        <Route path="client-payments" element={<ClientPaymentsPage />} />
        <Route path="dashboard" element={<DashboardPage />} />
        <Route path="quickbooks" element={<QuickBooksPage />} />
      </Route>
      <Route path="*" element={<Navigate to="/client-payments" replace />} />
    </Routes>
  );
}
