import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Calculator, Settings, User, Download, Search } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import FileUpload from "@/components/file-upload";
import DateRangeFilters from "@/components/date-range-filters";
import SummaryCards from "@/components/summary-cards";
import PayoutDataTable from "@/components/payout-data-table";
import SettingsDialog from "@/components/settings-dialog";
import SettingsUpload from "@/components/settings-upload";
import DataReset from "@/components/data-reset";
import MissingDataDisplay from "@/components/missing-data-display";
import { apiRequest } from "@/lib/queryClient";
import type { PayoutSummary, PayoutRow } from "@shared/schema";

export default function Dashboard() {
  // Current month default dates
  const getCurrentMonthDates = () => {
    const now = new Date();
    const year = now.getFullYear();
    const month = now.getMonth();
    const firstDay = new Date(year, month, 1).toISOString().split('T')[0];
    const lastDay = new Date(year, month + 1, 0).toISOString().split('T')[0];
    return { firstDay, lastDay };
  };

  const { firstDay, lastDay } = getCurrentMonthDates();

  const [orderDateFrom, setOrderDateFrom] = useState("2025-07-29");
  const [orderDateTo, setOrderDateTo] = useState("2025-08-12");
  const [deliveredDateFrom, setDeliveredDateFrom] = useState("2025-07-24");
  const [deliveredDateTo, setDeliveredDateTo] = useState("2025-08-09");
  const [selectedDropshipper, setSelectedDropshipper] = useState<string>("thedaazarastore@gmail.com");
  
  // Applied filters state (what's actually being used for API calls)
  // Set to specific date ranges for thedaazarastore@gmail.com by default
  const [appliedOrderDateFrom, setAppliedOrderDateFrom] = useState("2025-07-29");
  const [appliedOrderDateTo, setAppliedOrderDateTo] = useState("2025-08-12");
  const [appliedDeliveredDateFrom, setAppliedDeliveredDateFrom] = useState("2025-07-24");
  const [appliedDeliveredDateTo, setAppliedDeliveredDateTo] = useState("2025-08-09");
  const [appliedDropshipper, setAppliedDropshipper] = useState<string>("thedaazarastore@gmail.com");
  const [showSettings, setShowSettings] = useState(false);
  const [uploadSuccess, setUploadSuccess] = useState<{
    processedRows: number;
    cancelledRows: number;
  } | null>(null);

  // Fetch dropshippers
  const { data: dropshippers = [] } = useQuery<string[]>({
    queryKey: ['/api/dropshippers'],
  });

  // Fetch payout calculations using applied filters
  const { data: payoutData, isLoading: isCalculating, refetch: refetchPayouts } = useQuery<{
    summary: PayoutSummary;
    rows: PayoutRow[];
    adjustments: Array<{
      orderId: string;
      reason: string;
      amount: number;
      reference: string;
    }>;
  }>({
    queryKey: ['/api/calculate-payouts', appliedOrderDateFrom, appliedOrderDateTo, appliedDeliveredDateFrom, appliedDeliveredDateTo, appliedDropshipper],
    queryFn: async () => {
      const response = await apiRequest('POST', '/api/calculate-payouts', {
        orderDateFrom: appliedOrderDateFrom,
        orderDateTo: appliedOrderDateTo,
        deliveredDateFrom: appliedDeliveredDateFrom,
        deliveredDateTo: appliedDeliveredDateTo,
        dropshipperEmail: appliedDropshipper === "all" ? undefined : appliedDropshipper,
      });
      return response.json();
    },
    enabled: Boolean(appliedOrderDateFrom && appliedOrderDateTo && appliedDeliveredDateFrom && appliedDeliveredDateTo),
  });

  // Check for missing data
  const { data: missingData } = useQuery<{
    missingPrices: Array<{ dropshipperEmail: string; productUid: string; productName: string; sku: string | null }>;
    missingRates: string[];
  }>({
    queryKey: ['/api/missing-data'],
  });

  const handleDateRangeChange = (type: "delivered" | "order", from: string, to: string) => {
    if (type === 'order') {
      setOrderDateFrom(from);
      setOrderDateTo(to);
    } else {
      setDeliveredDateFrom(from);
      setDeliveredDateTo(to);
    }
  };

  const handleApplyFilters = async () => {
    setAppliedOrderDateFrom(orderDateFrom);
    setAppliedOrderDateTo(orderDateTo);
    setAppliedDeliveredDateFrom(deliveredDateFrom);
    setAppliedDeliveredDateTo(deliveredDateTo);
    setAppliedDropshipper(selectedDropshipper);
  };

  const handleExportWorkbook = async () => {
    try {
      const response = await apiRequest('POST', '/api/export-workbook', {
        orderDateFrom: appliedOrderDateFrom,
        orderDateTo: appliedOrderDateTo,
        deliveredDateFrom: appliedDeliveredDateFrom,
        deliveredDateTo: appliedDeliveredDateTo,
        dropshipperEmail: appliedDropshipper === "all" ? undefined : appliedDropshipper,
      });

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.style.display = 'none';
      a.href = url;
      a.download = `Payout_${new Date().toISOString().split('T')[0].replace(/-/g, '')}.xlsx`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
    } catch (error) {
      console.error('Error exporting workbook:', error);
    }
  };

  const handleUploadSuccess = (data: { processedRows: number; cancelledRows: number }) => {
    setUploadSuccess(data);
    refetchPayouts();
  };

  const hasMissingData = missingData && (missingData.missingPrices.length > 0 || missingData.missingRates.length > 0);

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Top Navigation */}
      <nav className="bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between h-16">
            <div className="flex items-center">
              <div className="flex items-center space-x-4">
                <h1 className="text-xl font-semibold text-gray-900">Dropshipper Payout System</h1>
                <a 
                  href="/transparency" 
                  className="px-3 py-2 text-sm font-medium text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-md transition-colors"
                >
                  Database Transparency
                </a>
              </div>
              <div className="flex-shrink-0">
                <h1 className="text-xl font-semibold text-gray-900" data-testid="header-title">
                  <Calculator className="inline-block text-primary mr-2" />
                  Dropshipper Payout System
                </h1>
              </div>
            </div>
            <div className="flex items-center space-x-4">
              <Button
                variant="ghost"
                size="sm"
                onClick={() => window.location.href = '/transparency'}
                data-testid="button-transparency"
              >
                <Settings className="h-4 w-4 mr-2" />
                Database
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => window.location.href = '/debug'}
                data-testid="button-debug"
              >
                <Search className="h-4 w-4 mr-2" />
                Debug
              </Button>
              <Button
                variant="ghost"
                size="icon"
                onClick={() => setShowSettings(true)}
                data-testid="button-settings"
              >
                <Settings className="h-5 w-5" />
              </Button>
              <Button variant="ghost" size="icon" data-testid="button-profile">
                <User className="h-5 w-5" />
              </Button>
            </div>
          </div>
        </div>
      </nav>

      <div className="max-w-7xl mx-auto py-6 px-4 sm:px-6 lg:px-8">
        {/* Missing Data Display */}
        <MissingDataDisplay onConfigureClick={() => setShowSettings(true)} />
        
        {/* Settings Upload Section */}
        <SettingsUpload />
        
        {/* Data Reset Section */}
        <DataReset />
        
        {/* File Upload Section */}
        <FileUpload onUploadSuccess={handleUploadSuccess} />

        {/* Upload Success Alert */}
        {uploadSuccess && (
          <Card className="mb-6 border-green-200 bg-green-50">
            <CardContent className="pt-6">
              <div className="flex">
                <div className="flex-shrink-0">
                  <div className="w-2 h-2 rounded-full bg-green-400 mt-2"></div>
                </div>
                <div className="ml-3">
                  <h3 className="text-sm font-medium text-green-800" data-testid="text-upload-success">
                    Upload Successful
                  </h3>
                  <p className="text-sm text-green-700 mt-1">
                    <span data-testid="text-processed-rows">{uploadSuccess.processedRows}</span> rows processed, {" "}
                    <span data-testid="text-cancelled-rows">{uploadSuccess.cancelledRows}</span> cancelled orders removed
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Missing Data Alert */}
        {hasMissingData && (
          <Card className="mb-6 border-orange-200 bg-orange-50">
            <CardContent className="pt-6">
              <div className="flex justify-between items-start">
                <div>
                  <h3 className="text-sm font-medium text-orange-800" data-testid="text-missing-data">
                    Missing Prices/Rates Detected
                  </h3>
                  <p className="text-sm text-orange-700 mt-1">
                    {missingData.missingPrices.length} product prices and {missingData.missingRates.length} shipping rates need to be configured.
                  </p>
                </div>
                <Button 
                  variant="outline" 
                  size="sm" 
                  onClick={() => setShowSettings(true)}
                  data-testid="button-configure-settings"
                >
                  Configure Settings
                </Button>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Date Range Filters */}
        <DateRangeFilters
          orderDateFrom={orderDateFrom}
          orderDateTo={orderDateTo}
          deliveredDateFrom={deliveredDateFrom}
          deliveredDateTo={deliveredDateTo}
          selectedDropshipper={selectedDropshipper}
          dropshippers={dropshippers}
          onDateRangeChange={handleDateRangeChange}
          onDropshipperChange={setSelectedDropshipper}
          onApplyFilters={handleApplyFilters}
          isCalculating={isCalculating}
        />

        {/* Summary Cards */}
        {payoutData && (
          <>
            <SummaryCards 
              summary={payoutData.summary} 
              isLoading={isCalculating}
            />
            {/* Order Count Breakdown Card */}
            <Card className="mb-6 bg-gray-50">
              <CardContent className="p-5">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                  <div className="flex items-center justify-between">
                    <span className="text-gray-600">Orders with Shipping Charges:</span>
                    <Badge variant="outline" className="bg-blue-100 text-blue-800">
                      {payoutData.summary.ordersWithShippingCharges > 0 
                        ? `${payoutData.summary.ordersWithShippingCharges} orders (cancelled excluded)` 
                        : 'Click "Apply Filters" to see count'
                      }
                    </Badge>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-gray-600">Orders with Product Amount:</span>
                    <Badge variant="outline" className="bg-green-100 text-green-800">
                      {payoutData.summary.ordersWithProductAmount > 0 
                        ? `${payoutData.summary.ordersWithProductAmount} orders (delivered only)` 
                        : 'Click "Apply Filters" to see count'
                      }
                    </Badge>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-gray-600">Total Processed Orders:</span>
                    <Badge variant="outline" className="bg-purple-100 text-purple-800">
                      {payoutData.summary.totalOrdersProcessed > 0 
                        ? `${payoutData.summary.totalOrdersProcessed} total rows` 
                        : 'Click "Apply Filters" to see count'
                      }
                    </Badge>
                  </div>
                </div>
                <div className="mt-3 pt-3 border-t border-gray-200">
                  <p className="text-xs text-gray-500 flex items-center gap-1">
                    <span className="inline-block w-2 h-2 bg-blue-500 rounded-full"></span>
                    Shipping charges exclude cancelled orders from order date range
                  </p>
                </div>
              </CardContent>
            </Card>
          </>
        )}

        {/* Main Content */}
        <Card className="bg-white rounded-lg shadow-sm border border-gray-200">
          <Tabs defaultValue="payout-data" className="w-full">
            <div className="border-b border-gray-200">
              <TabsList className="grid w-full grid-cols-3 bg-transparent h-auto p-0">
                <TabsTrigger 
                  value="payout-data" 
                  className="data-[state=active]:border-primary data-[state=active]:text-primary border-b-2 border-transparent py-4 px-1"
                  data-testid="tab-payout-data"
                >
                  <div className="flex items-center gap-2">
                    <Calculator className="h-4 w-4" />
                    Payout Data
                  </div>
                </TabsTrigger>
                <TabsTrigger 
                  value="settings" 
                  className="data-[state=active]:border-primary data-[state=active]:text-primary border-b-2 border-transparent py-4 px-1"
                  data-testid="tab-settings"
                  onClick={() => setShowSettings(true)}
                >
                  <div className="flex items-center gap-2">
                    <Settings className="h-4 w-4" />
                    Settings
                  </div>
                </TabsTrigger>
                <TabsTrigger 
                  value="audit" 
                  className="data-[state=active]:border-primary data-[state=active]:text-primary border-b-2 border-transparent py-4 px-1"
                  data-testid="tab-audit"
                >
                  <div className="flex items-center gap-2">
                    <div className="w-4 h-4">📋</div>
                    Audit Log
                  </div>
                </TabsTrigger>
              </TabsList>
            </div>

            <TabsContent value="payout-data" className="p-6">
              <div className="flex justify-between items-center mb-4">
                <div>
                  <h3 className="text-lg font-medium text-gray-900" data-testid="text-payout-title">
                    Payout Details
                  </h3>
                  <p className="text-sm text-gray-500">
                    Detailed breakdown of payout calculations
                  </p>
                </div>
                <Button 
                  onClick={handleExportWorkbook}
                  disabled={!payoutData || isCalculating}
                  data-testid="button-export-workbook"
                >
                  <Download className="mr-2 h-4 w-4" />
                  Export Workbook
                </Button>
              </div>

              {payoutData && (
                <PayoutDataTable 
                  rows={payoutData.rows} 
                  adjustments={payoutData.adjustments}
                  isLoading={isCalculating}
                />
              )}
            </TabsContent>

            <TabsContent value="audit" className="p-6">
              <div className="text-center py-12">
                <h3 className="text-lg font-medium text-gray-900 mb-2" data-testid="text-audit-placeholder">
                  Audit Log
                </h3>
                <p className="text-gray-500">
                  Audit trail functionality will be displayed here
                </p>
              </div>
            </TabsContent>
          </Tabs>
        </Card>
      </div>

      {/* Settings Dialog */}
      <SettingsDialog 
        open={showSettings} 
        onOpenChange={setShowSettings}
        missingData={missingData}
        onSettingsUpdated={refetchPayouts}
      />
    </div>
  );
}
