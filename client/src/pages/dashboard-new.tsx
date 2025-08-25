import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Calculator, Settings, User, Download, Search, TrendingUp, Package, Truck, Coins, Database, Eye, RotateCcw, Calendar } from "lucide-react";
import { Link } from "wouter";
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
import AnalyticsDashboard from "@/components/analytics-dashboard";
import { UploadHistory } from "@/components/upload-history";
import ShippingCostBreakdown from "@/components/shipping-cost-breakdown";
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
  const [appliedOrderDateFrom, setAppliedOrderDateFrom] = useState("2025-07-29");
  const [appliedOrderDateTo, setAppliedOrderDateTo] = useState("2025-08-12");
  const [appliedDeliveredDateFrom, setAppliedDeliveredDateFrom] = useState("2025-07-24");
  const [appliedDeliveredDateTo, setAppliedDeliveredDateTo] = useState("2025-08-09");
  const [appliedDropshipper, setAppliedDropshipper] = useState<string>("thedaazarastore@gmail.com");
  const [showSettings, setShowSettings] = useState(false);
  const [isApplyingFilters, setIsApplyingFilters] = useState(false);
  const [uploadSuccess, setUploadSuccess] = useState<{
    processedRows: number;
    cancelledRows: number;
  } | null>(null);

  // Clean dropshippers fetch
  const { data: dropshippers = [] } = useQuery<string[]>({
    queryKey: ['dropshippers'],
    queryFn: async () => {
      const response = await fetch('/api/dropshippers');
      if (!response.ok) throw new Error('Failed to fetch dropshippers');
      return response.json();
    },
  });

  // Clean API call for payout calculations
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
    queryKey: ['calculate-payouts', appliedOrderDateFrom, appliedOrderDateTo, appliedDeliveredDateFrom, appliedDeliveredDateTo, appliedDropshipper],
    queryFn: async () => {
      const response = await fetch('/api/calculate-payouts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          orderDateFrom: appliedOrderDateFrom,
          orderDateTo: appliedOrderDateTo,
          deliveredDateFrom: appliedDeliveredDateFrom,
          deliveredDateTo: appliedDeliveredDateTo,
          dropshipperEmail: appliedDropshipper === "all" ? undefined : appliedDropshipper,
        }),
      });
      if (!response.ok) throw new Error('API call failed');
      return response.json();
    },
    enabled: true,
    refetchOnMount: true,
    refetchOnWindowFocus: false,
  });

  // Handle applying filters with loading state
  const handleApplyFilters = async () => {
    setIsApplyingFilters(true);
    try {
      setAppliedOrderDateFrom(orderDateFrom);
      setAppliedOrderDateTo(orderDateTo);
      setAppliedDeliveredDateFrom(deliveredDateFrom);
      setAppliedDeliveredDateTo(deliveredDateTo);
      setAppliedDropshipper(selectedDropshipper);
      
      // Small delay to show loading state
      await new Promise(resolve => setTimeout(resolve, 300));
    } finally {
      setIsApplyingFilters(false);
    }
  };

  const handleDateRangeChange = (type: "delivered" | "order", from: string, to: string) => {
    if (type === 'order') {
      setOrderDateFrom(from);
      setOrderDateTo(to);
    } else {
      setDeliveredDateFrom(from);
      setDeliveredDateTo(to);
    }
  };

  const handleDropshipperChange = (email: string) => {
    setSelectedDropshipper(email);
  };

  const handleUploadSuccess = (processedRows: number, cancelledRows: number) => {
    setUploadSuccess({ processedRows, cancelledRows });
    refetchPayouts();
  };

  const [isExporting, setIsExporting] = useState(false);

  const handleExportWorkbook = async () => {
    setIsExporting(true);
    try {
      const response = await fetch('/api/export-workbook', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          orderDateFrom: appliedOrderDateFrom,
          orderDateTo: appliedOrderDateTo,
          deliveredDateFrom: appliedDeliveredDateFrom,
          deliveredDateTo: appliedDeliveredDateTo,
          dropshipperEmail: appliedDropshipper === "all" ? undefined : appliedDropshipper,
        }),
      });

      if (!response.ok) {
        throw new Error('Export failed');
      }

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.style.display = 'none';
      a.href = url;
      a.download = response.headers.get('Content-Disposition')?.match(/filename="(.+)"/)?.[1] || 'payout-report.xlsx';
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
    } catch (error) {
      console.error('Export failed:', error);
    } finally {
      setIsExporting(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-purple-50">
      <div className="max-w-7xl mx-auto p-3 md:p-6 space-y-4 md:space-y-8">
        {/* Header Section */}
        <div className="bg-white rounded-xl shadow-xl border border-gray-100 p-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <div className="bg-gradient-to-r from-blue-500 to-purple-600 p-3 rounded-xl shadow-lg">
                <Calculator className="h-8 w-8 text-white" />
              </div>
              <div>
                <h1 className="text-3xl font-bold bg-gradient-to-r from-blue-600 to-purple-600 bg-clip-text text-transparent">
                  Payout Dashboard
                </h1>
              </div>
            </div>
            <div className="flex flex-col sm:flex-row w-full lg:w-auto gap-2 sm:gap-3">
              {/* Mobile: Show as full-width buttons */}
              <div className="flex flex-col sm:flex-row gap-2 sm:gap-3 w-full lg:w-auto">
                <Link href="/database-transparency" className="w-full sm:w-auto">
                  <Button
                    variant="default"
                    size="sm"
                    className="w-full gap-1 sm:gap-2 bg-gradient-to-r from-green-500 to-emerald-600 hover:from-green-600 hover:to-emerald-700 border-0 px-3 py-2 sm:px-6 sm:py-3 md:px-8 md:py-4 rounded-lg sm:rounded-xl shadow-xl text-white font-semibold text-xs sm:text-sm md:text-base lg:text-lg"
                    data-testid="button-database-transparency"
                  >
                    <Eye className="h-4 w-4 sm:h-5 sm:w-5 md:h-6 md:w-6" />
                    <span className="hidden xs:inline sm:hidden md:inline">View All Data</span>
                    <span className="xs:hidden sm:inline md:hidden">Data</span>
                  </Button>
                </Link>
                
              </div>
            </div>
          </div>
        </div>

        {/* Success Message */}
        {uploadSuccess && (
          <div className="bg-gradient-to-r from-green-50 to-emerald-50 border-2 border-green-200 rounded-xl md:rounded-2xl p-4 md:p-6 shadow-lg">
            <div className="flex flex-col sm:flex-row items-start sm:items-center gap-3 sm:gap-4">
              <div className="bg-green-500 p-2 rounded-lg md:rounded-xl">
                <Package className="h-5 w-5 md:h-6 md:w-6 text-white" />
              </div>
              <div>
                <h3 className="text-base md:text-lg font-semibold text-green-800">Upload Successful!</h3>
                <p className="text-sm md:text-base text-green-700">
                  Processed {uploadSuccess.processedRows} orders, {uploadSuccess.cancelledRows} cancelled orders preserved
                </p>
              </div>
            </div>
          </div>
        )}

        {/* Main Content Tabs */}
        <Tabs defaultValue="payout" className="space-y-4 md:space-y-6">
          <div className="overflow-x-auto px-2 md:px-0">
            <TabsList className="inline-flex w-max min-w-full bg-white rounded-xl shadow-lg p-2 h-auto gap-1">
              <TabsTrigger 
                value="analytics" 
                className="flex items-center gap-2 py-2 px-3 md:px-4 rounded-lg data-[state=active]:bg-gradient-to-r data-[state=active]:from-blue-500 data-[state=active]:to-purple-600 data-[state=active]:text-white hover:bg-gray-50 transition-all whitespace-nowrap"
                data-testid="tab-analytics"
              >
                <TrendingUp className="h-4 w-4" />
                <span className="text-sm font-medium">Analytics</span>
              </TabsTrigger>
              
              <TabsTrigger 
                value="upload" 
                className="flex items-center gap-2 py-2 px-3 md:px-4 rounded-lg data-[state=active]:bg-gradient-to-r data-[state=active]:from-blue-500 data-[state=active]:to-purple-600 data-[state=active]:text-white hover:bg-gray-50 transition-all whitespace-nowrap"
                data-testid="tab-upload"
              >
                <Package className="h-4 w-4" />
                <span className="text-sm font-medium">Upload</span>
              </TabsTrigger>
              
              <TabsTrigger 
                value="payout" 
                className="flex items-center gap-2 py-2 px-3 md:px-4 rounded-lg data-[state=active]:bg-gradient-to-r data-[state=active]:from-blue-500 data-[state=active]:to-purple-600 data-[state=active]:text-white hover:bg-gray-50 transition-all whitespace-nowrap"
                data-testid="tab-payout"
              >
                <Calculator className="h-4 w-4" />
                <span className="text-sm font-medium">Payout</span>
              </TabsTrigger>
              
              <TabsTrigger 
                value="history" 
                className="flex items-center gap-2 py-2 px-3 md:px-4 rounded-lg data-[state=active]:bg-gradient-to-r data-[state=active]:from-indigo-500 data-[state=active]:to-blue-600 data-[state=active]:text-white hover:bg-gray-50 transition-all whitespace-nowrap"
                data-testid="tab-history"
              >
                <Database className="h-4 w-4" />
                <span className="text-sm font-medium">History</span>
              </TabsTrigger>
              
              <TabsTrigger 
                value="shipping" 
                className="flex items-center gap-2 py-2 px-3 md:px-4 rounded-lg data-[state=active]:bg-gradient-to-r data-[state=active]:from-green-500 data-[state=active]:to-teal-600 data-[state=active]:text-white hover:bg-gray-50 transition-all whitespace-nowrap"
                data-testid="tab-shipping"
              >
                <Truck className="h-4 w-4" />
                <span className="text-sm font-medium">Shipping</span>
              </TabsTrigger>
              
              <TabsTrigger 
                value="settings" 
                className="flex items-center gap-2 py-2 px-3 md:px-4 rounded-lg data-[state=active]:bg-gradient-to-r data-[state=active]:from-orange-500 data-[state=active]:to-red-600 data-[state=active]:text-white hover:bg-gray-50 transition-all whitespace-nowrap"
                data-testid="tab-settings"
              >
                <Coins className="h-4 w-4" />
                <span className="text-sm font-medium">Settings</span>
              </TabsTrigger>
            </TabsList>
          </div>

          {/* Analytics Dashboard Tab */}
          <TabsContent value="analytics" className="space-y-4 md:space-y-6">
            <div className="px-2 md:px-0">
              <AnalyticsDashboard />
            </div>
          </TabsContent>

          {/* Payout Tab - Mobile optimized */}
          <TabsContent value="payout" className="space-y-4 md:space-y-8">
            {/* Filters Card */}
            <Card className="bg-white rounded-xl md:rounded-2xl shadow-xl border border-gray-100 mx-2 md:mx-0">
              <CardHeader className="bg-gradient-to-r from-blue-50 to-purple-50 rounded-t-xl md:rounded-t-2xl p-4 md:p-6">
                <CardTitle className="text-lg md:text-2xl font-bold text-gray-800 flex items-center gap-2 md:gap-3">
                  <Calculator className="h-5 w-5 md:h-6 md:w-6 text-blue-600" />
                  Calculate Payouts
                </CardTitle>
                <CardDescription className="text-sm md:text-lg text-gray-600">
                  Configure date ranges and calculate dropshipper payouts
                </CardDescription>
              </CardHeader>
              <CardContent className="p-4 md:p-8">
                <DateRangeFilters
                  orderDateFrom={orderDateFrom}
                  orderDateTo={orderDateTo}
                  deliveredDateFrom={deliveredDateFrom}
                  deliveredDateTo={deliveredDateTo}
                  selectedDropshipper={selectedDropshipper}
                  dropshippers={dropshippers}
                  onDateRangeChange={handleDateRangeChange}
                  onDropshipperChange={handleDropshipperChange}
                  onApplyFilters={handleApplyFilters}
                  isCalculating={isCalculating || isApplyingFilters}
                />
              </CardContent>
            </Card>

            {/* Results */}
            {payoutData && (
              <div className="space-y-4 md:space-y-8 mx-2 md:mx-0">
                <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4 sm:gap-0">
                  <h2 className="text-lg md:text-2xl font-bold text-gray-800">Calculation Results</h2>
                  <Button
                    onClick={handleExportWorkbook}
                    disabled={isExporting}
                    variant="outline"
                    className="w-full sm:w-auto gap-2 bg-gradient-to-r from-green-500 to-emerald-600 text-white border-0 hover:from-green-600 hover:to-emerald-700 px-4 py-2 text-sm md:text-base"
                    data-testid="button-download-payout"
                  >
                    {isExporting ? (
                      <>
                        <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
                        Exporting...
                      </>
                    ) : (
                      <>
                        <Download className="h-4 w-4" />
                        Download Excel
                      </>
                    )}
                  </Button>
                </div>
                <SummaryCards summary={payoutData.summary} isLoading={isCalculating} viewType="webview" />
              </div>
            )}

            {/* Data Table */}
            {payoutData && (
              <Card className="bg-white rounded-xl md:rounded-2xl shadow-xl border border-gray-100 mx-2 md:mx-0">
                <CardHeader className="bg-gradient-to-r from-gray-50 to-slate-50 rounded-t-xl md:rounded-t-2xl p-4 md:p-6">
                  <CardTitle className="text-lg md:text-2xl font-bold text-gray-800 flex items-center gap-2 md:gap-3">
                    <Truck className="h-5 w-5 md:h-6 md:w-6 text-blue-600" />
                    Order Details
                  </CardTitle>
                  <CardDescription className="text-sm md:text-lg text-gray-600">
                    Detailed breakdown of all orders and calculations
                  </CardDescription>
                </CardHeader>
                <CardContent className="p-2 md:p-8">
                  <div className="overflow-x-auto">
                    <PayoutDataTable
                      rows={payoutData.rows}
                      adjustments={payoutData.adjustments}
                    />
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Missing Data Display */}
            <div className="mx-2 md:mx-0">
              <MissingDataDisplay onConfigureClick={() => setShowSettings(true)} />
            </div>
          </TabsContent>

          {/* Shipping Cost Breakdown Tab */}
          <TabsContent value="shipping" className="space-y-4 md:space-y-6">
            <div className="px-2 md:px-0">
              <ShippingCostBreakdown />
            </div>
          </TabsContent>

          {/* Calculate Payouts Tab */}
          <TabsContent value="calculate" className="space-y-8">
            {/* Filters Card */}
            <Card className="bg-white rounded-2xl shadow-xl border border-gray-100">
              <CardHeader className="bg-gradient-to-r from-blue-50 to-purple-50 rounded-t-2xl">
                <CardTitle className="text-2xl font-bold text-gray-800 flex items-center gap-3">
                  <Search className="h-6 w-6 text-blue-600" />
                  Filter & Calculate
                </CardTitle>
                <CardDescription className="text-lg text-gray-600">
                  Set your date ranges and dropshipper to calculate accurate payouts
                </CardDescription>
              </CardHeader>
              <CardContent className="p-8">
                <DateRangeFilters
                  orderDateFrom={orderDateFrom}
                  orderDateTo={orderDateTo}
                  deliveredDateFrom={deliveredDateFrom}
                  deliveredDateTo={deliveredDateTo}
                  selectedDropshipper={selectedDropshipper}
                  dropshippers={dropshippers}
                  onDateRangeChange={handleDateRangeChange}
                  onDropshipperChange={handleDropshipperChange}
                  onApplyFilters={handleApplyFilters}
                  isCalculating={isCalculating || isApplyingFilters}
                />
              </CardContent>
            </Card>

            {/* Calculation Status */}
            {isCalculating && (
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-4">
                <div className="flex items-center gap-3">
                  <div className="animate-spin rounded-full h-5 w-5 border-2 border-blue-600 border-t-transparent"></div>
                  <p className="text-sm text-blue-800">Calculating payouts...</p>
                </div>
              </div>
            )}

            {/* Summary Cards */}
            {payoutData && (
              <div className="bg-white rounded-2xl shadow-xl p-8 border border-gray-100">
                <div className="mb-6 flex items-center justify-between">
                  <h2 className="text-3xl font-bold text-gray-800 flex items-center gap-3">
                    <Coins className="h-8 w-8 text-green-600" />
                    Payout Summary
                  </h2>
                  <Button
                    onClick={handleExportWorkbook}
                    disabled={isExporting}
                    className="gap-2 bg-gradient-to-r from-green-500 to-emerald-600 hover:from-green-600 hover:to-emerald-700 px-6 py-3 rounded-xl shadow-lg text-lg font-medium disabled:opacity-50"
                    data-testid="button-export"
                  >
                    {isExporting ? (
                      <>
                        <div className="animate-spin rounded-full h-5 w-5 border-2 border-white border-t-transparent"></div>
                        Exporting...
                      </>
                    ) : (
                      <>
                        <Download className="h-5 w-5" />
                        Export Report
                      </>
                    )}
                  </Button>
                </div>
                <SummaryCards summary={payoutData.summary} isLoading={isCalculating} viewType="webview" />
              </div>
            )}

            {/* Data Table */}
            {payoutData && (
              <Card className="bg-white rounded-2xl shadow-xl border border-gray-100">
                <CardHeader className="bg-gradient-to-r from-gray-50 to-slate-50 rounded-t-2xl">
                  <CardTitle className="text-2xl font-bold text-gray-800 flex items-center gap-3">
                    <Truck className="h-6 w-6 text-blue-600" />
                    Order Details
                  </CardTitle>
                  <CardDescription className="text-lg text-gray-600">
                    Detailed breakdown of all orders and calculations
                  </CardDescription>
                </CardHeader>
                <CardContent className="p-8">
                  <PayoutDataTable
                    rows={payoutData.rows}
                    adjustments={payoutData.adjustments}
                  />
                </CardContent>
              </Card>
            )}

            {/* Missing Data Display */}
            <MissingDataDisplay onConfigureClick={() => setShowSettings(true)} />
          </TabsContent>

          {/* Upload Data Tab */}
          <TabsContent value="upload" className="space-y-4 md:space-y-6">
            <Card className="bg-white rounded-xl md:rounded-2xl shadow-xl border border-gray-100 mx-2 md:mx-0">
              <CardHeader className="bg-gradient-to-r from-green-50 to-emerald-50 rounded-t-xl md:rounded-t-2xl p-4 md:p-6">
                <CardTitle className="text-lg md:text-2xl font-bold text-gray-800 flex items-center gap-2 md:gap-3">
                  <Package className="h-5 w-5 md:h-6 md:w-6 text-green-600" />
                  Upload Order Data
                </CardTitle>
                <CardDescription className="text-sm md:text-lg text-gray-600">
                  Upload Excel/CSV files with order data for payout calculations
                </CardDescription>
              </CardHeader>
              <CardContent className="p-4 md:p-8">
                <FileUpload onUploadSuccess={(data) => handleUploadSuccess(data.processedRows, data.cancelledRows)} />
              </CardContent>
            </Card>
          </TabsContent>

          {/* Settings Tab */}
          <TabsContent value="settings" className="space-y-4 md:space-y-6">
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 md:gap-6 mx-2 md:mx-0">
              <Card className="bg-white rounded-xl md:rounded-2xl shadow-xl border border-gray-100">
                <CardHeader className="bg-gradient-to-r from-blue-50 to-indigo-50 rounded-t-xl md:rounded-t-2xl p-4 md:p-6">
                  <CardTitle className="text-lg md:text-xl font-bold text-gray-800">
                    Product Prices & Shipping Rates
                  </CardTitle>
                  <CardDescription className="text-sm md:text-base text-gray-600">
                    Manage product costs and shipping rates
                  </CardDescription>
                </CardHeader>
                <CardContent className="p-4 md:p-6">
                  <SettingsUpload />
                </CardContent>
              </Card>
              
              <Card className="bg-white rounded-xl md:rounded-2xl shadow-xl border border-gray-100">
                <CardHeader className="bg-gradient-to-r from-red-50 to-pink-50 rounded-t-xl md:rounded-t-2xl p-4 md:p-6">
                  <CardTitle className="text-lg md:text-xl font-bold text-gray-800 flex items-center gap-2">
                    <Database className="h-5 w-5 md:h-6 md:w-6 text-red-600" />
                    Reset Data
                  </CardTitle>
                  <CardDescription className="text-sm md:text-base text-red-600">
                    Dangerous operations - use with caution
                  </CardDescription>
                </CardHeader>
                <CardContent className="p-4 md:p-6">
                  <DataReset />
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          {/* Upload History Tab */}
          <TabsContent value="history" className="space-y-4 md:space-y-6">
            <div className="mx-2 md:mx-0">
              <UploadHistory />
            </div>
          </TabsContent>
          
        </Tabs>
        
        {/* Settings Dialog */}
        <SettingsDialog
          open={showSettings}
          onOpenChange={setShowSettings}
          onSettingsUpdated={() => refetchPayouts()}
        />
      </div>
    </div>
  );
}