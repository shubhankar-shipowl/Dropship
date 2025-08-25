import { Switch, Route } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
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
      <Route path="/" component={Dashboard} />
      <Route path="/debug" component={DebugMapping} />
      <Route path="/database-transparency" component={DatabaseTransparency} />
      <Route path="/rts-rto-reconciliation" component={RtsRtoReconciliationPage} />
      <Route path="/reports-export" component={ReportsExport} />
      <Route path="/settlement-scheduler" component={SettlementScheduler} />
      <Route path="/payout-planner" component={PayoutPlanner} />
      <Route path="/advanced-analytics" component={AdvancedAnalytics} />
      <Route component={NotFound} />
    </Switch>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <Toaster />
        <Router />
      </TooltipProvider>
    </QueryClientProvider>
  );
}

export default App;
