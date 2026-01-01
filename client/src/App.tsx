import { Switch, Route } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { AuthProvider } from "@/contexts/AuthContext";
import { ProtectedRoute } from "@/components/ProtectedRoute";
import Login from "@/pages/login";
import Dashboard from "@/pages/dashboard-new";
import DebugMapping from "@/pages/debug-mapping";
import DatabaseTransparency from "@/pages/database-transparency";
import RtsRtoReconciliationPage from "@/pages/rts-rto-reconciliation";
import ReportsExport from "@/pages/reports-export";
import SettlementScheduler from "@/pages/settlement-scheduler";
import PayoutPlanner from "@/pages/payout-planner";
import AdvancedAnalytics from "@/pages/advanced-analytics";
import NotFound from "@/pages/not-found";

function Router() {
  return (
    <Switch>
      <Route path="/login" component={Login} />
      <Route path="/">
        <ProtectedRoute>
          <Dashboard />
        </ProtectedRoute>
      </Route>
      <Route path="/debug">
        <ProtectedRoute>
          <DebugMapping />
        </ProtectedRoute>
      </Route>
      <Route path="/database-transparency">
        <ProtectedRoute>
          <DatabaseTransparency />
        </ProtectedRoute>
      </Route>
      <Route path="/rts-rto-reconciliation">
        <ProtectedRoute>
          <RtsRtoReconciliationPage />
        </ProtectedRoute>
      </Route>
      <Route path="/reports-export">
        <ProtectedRoute>
          <ReportsExport />
        </ProtectedRoute>
      </Route>
      <Route path="/settlement-scheduler">
        <ProtectedRoute>
          <SettlementScheduler />
        </ProtectedRoute>
      </Route>
      <Route path="/payout-planner">
        <ProtectedRoute>
          <PayoutPlanner />
        </ProtectedRoute>
      </Route>
      <Route path="/advanced-analytics">
        <ProtectedRoute>
          <AdvancedAnalytics />
        </ProtectedRoute>
      </Route>
      <Route component={NotFound} />
    </Switch>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <TooltipProvider>
          <Toaster />
          <Router />
        </TooltipProvider>
      </AuthProvider>
    </QueryClientProvider>
  );
}

export default App;
