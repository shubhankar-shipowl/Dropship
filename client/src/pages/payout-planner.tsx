import { useState, useEffect } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Calendar, Download, Calculator, FileSpreadsheet, TrendingUp, Clock } from 'lucide-react';
import { Link } from 'wouter';
import { useToast } from '@/hooks/use-toast';
import { cn } from '@/lib/utils';

interface PayoutPlannerRow {
  runDate: string;
  cycle: string;
  orderStart: string;
  orderEnd: string;
  delStart: string;
  delEnd: string;
  ordersCount: number;
  codTotal: number;
  productCostTotal: number;
  shippingTotal: number;
  adjustmentsTotal: number;
  finalPayable: number;
  dropshipperEmail: string;
  purposeStatement?: string;
}

interface PayoutPlannerReport {
  rows: PayoutPlannerRow[];
  detailedRows?: any[];
  summary: {
    totalRuns: number;
    grandCodTotal: number;
    grandProductCostTotal: number;
    grandShippingTotal: number;
    grandAdjustmentsTotal: number;
    grandPayable: number;
  };
  missingPrices: number;
  missingProviders: number;
  generatedRunDates: string[];
}

const cutoffOptions = [
  { value: 1, label: 'D+1 (1 day cutoff)' },
  { value: 2, label: 'D+2 (2 days cutoff)' },
  { value: 3, label: 'D+3 (3 days cutoff)' },
  { value: 4, label: 'D+4 (4 days cutoff)' },
  { value: 5, label: 'D+5 (5 days cutoff)' },
  { value: 6, label: 'D+6 (6 days cutoff)' },
  { value: 7, label: 'D+7 (7 days cutoff)' },
];

const frequencyOptions = [
  { value: 'monthly', label: 'Monthly (मासिक)', desc: 'Month-end in range' },
  { value: 'twice_weekly', label: 'Twice Weekly (सप्ताह में दो बार)', desc: 'Tuesday & Friday' },
  { value: 'thrice_weekly', label: 'Thrice Weekly (सप्ताह में तीन बार)', desc: 'Monday, Wednesday & Friday' },
  { value: 'daily', label: 'Daily Weekdays (दैनिक)', desc: 'Monday to Friday only' },
];

const datePresets = [
  { value: 'this_month', label: 'This Month (इस महीने)', desc: 'Current month to today' },
  { value: 'last_30_days', label: 'Last 30 Days (पिछले 30 दिन)', desc: 'Rolling 30 days' },
  { value: 'custom', label: 'Custom Range (कस्टम रेंज)', desc: 'Select specific dates' },
];

export default function PayoutPlanner() {
  // Form states
  const [selectedDropshipper, setSelectedDropshipper] = useState<string>('all');
  const [datePreset, setDatePreset] = useState<string>('this_month');
  const [customFromDate, setCustomFromDate] = useState<string>('');
  const [customToDate, setCustomToDate] = useState<string>('');
  const [cutoffOffset, setCutoffOffset] = useState<number>(2);
  const [frequency, setFrequency] = useState<string>('twice_weekly');
  const [anchoredMode, setAnchoredMode] = useState<boolean>(false);
  const [useFixedWindows, setUseFixedWindows] = useState<boolean>(false);
  const [fixedWindowsConfig, setFixedWindowsConfig] = useState<any>({});
  
  // Report data
  const [reportData, setReportData] = useState<PayoutPlannerReport | null>(null);
  const [isGenerating, setIsGenerating] = useState(false);

  const { toast } = useToast();

  // Fetch dropshippers for dropdown
  const { data: dropshippers = [] } = useQuery<string[]>({
    queryKey: ['/api/dropshippers'],
    queryFn: async () => {
      const response = await fetch('/api/dropshippers');
      if (!response.ok) throw new Error('Failed to fetch dropshippers');
      return response.json();
    },
  });

  // Generate date range based on preset
  const getDateRange = () => {
    const today = new Date();
    
    switch (datePreset) {
      case 'this_month': {
        const firstDay = new Date(today.getFullYear(), today.getMonth(), 1);
        return {
          fromDate: firstDay.toISOString().split('T')[0],
          toDate: today.toISOString().split('T')[0]
        };
      }
      case 'last_30_days': {
        const thirtyDaysAgo = new Date(today);
        thirtyDaysAgo.setDate(today.getDate() - 29);
        return {
          fromDate: thirtyDaysAgo.toISOString().split('T')[0],
          toDate: today.toISOString().split('T')[0]
        };
      }
      case 'custom': {
        return {
          fromDate: customFromDate,
          toDate: customToDate
        };
      }
      default:
        return { fromDate: '', toDate: '' };
    }
  };

  // Generate payout planner report mutation
  const generateReportMutation = useMutation({
    mutationFn: async (params: any) => {
      const response = await fetch('/api/payout-planner', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(params),
      });
      if (!response.ok) throw new Error('Failed to generate payout planner report');
      return response.json();
    },
    onSuccess: (data) => {
      setReportData(data);
      toast({ 
        title: `Report Generated Successfully!`, 
        description: `${data.summary.totalRuns} payment sheets, Total Payable: ₹${data.summary.grandPayable.toLocaleString('en-IN')}`
      });
    },
    onError: () => {
      toast({ title: 'Error generating report', variant: 'destructive' });
    },
  });

  const generateReport = () => {
    const { fromDate, toDate } = getDateRange();
    
    if (!fromDate || !toDate) {
      toast({ title: 'Please select valid date range', variant: 'destructive' });
      return;
    }

    setIsGenerating(true);
    // Example fixed windows for D+2 Twice a Week case
    const fixedWindows = useFixedWindows ? {
      '2025-08-12': {
        orderStart: '2025-07-28',
        orderEnd: '2025-08-12', 
        delStart: '2025-07-24',
        delEnd: '2025-08-09'
      }
    } : null;

    generateReportMutation.mutate({
      dropshipper: selectedDropshipper !== 'all' ? selectedDropshipper : null,
      fromDate,
      toDate,
      cutoffOffset,
      frequency,
      anchoredMode,
      fixedWindows,
    });
    setIsGenerating(false);
  };

  // Download report as Excel
  const downloadExcel = async () => {
    if (!reportData) return;
    
    const { fromDate, toDate } = getDateRange();
    const dropshipperStr = selectedDropshipper === 'all' ? 'All' : selectedDropshipper.split('@')[0];
    
    try {
      const response = await fetch('/api/payout-planner/export', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          reportData,
          filename: `PayoutPlanner_${dropshipperStr}_${fromDate}_to_${toDate}_D+${cutoffOffset}_${frequency}.xlsx`
        }),
      });

      if (response.ok) {
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.style.display = 'none';
        a.href = url;
        a.download = `PayoutPlanner_${dropshipperStr}_${fromDate}_to_${toDate}_D+${cutoffOffset}_${frequency}.xlsx`;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        toast({ title: 'Report downloaded successfully!' });
      }
    } catch (error) {
      toast({ title: 'Download failed', variant: 'destructive' });
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 dark:from-gray-900 dark:to-gray-800">
      <div className="container mx-auto p-4 md:p-6 max-w-7xl">
        
        {/* Header */}
        <div className="flex flex-col md:flex-row md:items-center md:justify-between mb-6 gap-4">
          <div>
            <h1 className="text-2xl md:text-3xl font-bold text-gray-900 dark:text-white">
              Payout Planner Report
            </h1>
            <p className="text-gray-600 dark:text-gray-300 mt-1">
              Generate comprehensive payment cycle reports with custom frequencies • कस्टम भुगतान चक्र रिपोर्ट
            </p>
          </div>
          
          <div className="flex items-center gap-2">
            <Link href="/dashboard">
              <Button variant="outline" size="sm" data-testid="button-back-dashboard">
                ← Back to Dashboard
              </Button>
            </Link>
          </div>
        </div>

        <Tabs defaultValue="generator" className="space-y-6">
          <TabsList className="grid w-full grid-cols-2">
            <TabsTrigger value="generator">Report Generator</TabsTrigger>
            <TabsTrigger value="results">Results & Export</TabsTrigger>
          </TabsList>

          <TabsContent value="generator" className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Calculator className="h-5 w-5" />
                  Configuration • कॉन्फ़िगरेशन
                </CardTitle>
                <CardDescription>
                  Select dropshipper(s) and date preset. Then choose payment cycle (D+N + frequency).
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                
                {/* Dropshipper Selection */}
                <div className="space-y-2">
                  <label className="text-sm font-medium">Dropshipper • ड्रॉपशिपर</label>
                  <Select value={selectedDropshipper} onValueChange={setSelectedDropshipper}>
                    <SelectTrigger data-testid="select-dropshipper">
                      <SelectValue placeholder="Select dropshipper" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All Dropshippers • सभी ड्रॉपशिपर</SelectItem>
                      {dropshippers.map((email) => (
                        <SelectItem key={email} value={email}>
                          {email}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                {/* Date Range Selection */}
                <div className="space-y-4">
                  <label className="text-sm font-medium">Date Range • दिनांक सीमा</label>
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    {datePresets.map((preset) => (
                      <Card 
                        key={preset.value}
                        className={cn(
                          "cursor-pointer transition-colors border-2",
                          datePreset === preset.value 
                            ? "border-blue-500 bg-blue-50 dark:bg-blue-950" 
                            : "border-gray-200 dark:border-gray-700 hover:border-gray-300"
                        )}
                        onClick={() => setDatePreset(preset.value)}
                        data-testid={`card-preset-${preset.value}`}
                      >
                        <CardContent className="p-4">
                          <h3 className="font-medium">{preset.label}</h3>
                          <p className="text-xs text-gray-500 mt-1">{preset.desc}</p>
                        </CardContent>
                      </Card>
                    ))}
                  </div>
                  
                  {datePreset === 'custom' && (
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
                      <div className="space-y-2">
                        <label className="text-sm font-medium">From Date</label>
                        <input 
                          type="date" 
                          value={customFromDate}
                          onChange={(e) => setCustomFromDate(e.target.value)}
                          className="w-full px-3 py-2 border border-gray-300 rounded-md"
                          data-testid="input-custom-from-date"
                        />
                      </div>
                      <div className="space-y-2">
                        <label className="text-sm font-medium">To Date</label>
                        <input 
                          type="date" 
                          value={customToDate}
                          onChange={(e) => setCustomToDate(e.target.value)}
                          className="w-full px-3 py-2 border border-gray-300 rounded-md"
                          data-testid="input-custom-to-date"
                        />
                      </div>
                    </div>
                  )}
                </div>

                {/* Payment Cycle Configuration */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Cutoff Offset • कटऑफ ऑफसेट</label>
                    <Select value={cutoffOffset.toString()} onValueChange={(v) => setCutoffOffset(parseInt(v))}>
                      <SelectTrigger data-testid="select-cutoff-offset">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {cutoffOptions.map((option) => (
                          <SelectItem key={option.value} value={option.value.toString()}>
                            {option.label}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <p className="text-xs text-gray-500">
                      Delivered cutoff = run_date - {cutoffOffset} days
                    </p>
                  </div>

                  <div className="space-y-2">
                    <label className="text-sm font-medium">Frequency • आवृत्ति</label>
                    <Select value={frequency} onValueChange={setFrequency}>
                      <SelectTrigger data-testid="select-frequency">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {frequencyOptions.map((option) => (
                          <SelectItem key={option.value} value={option.value}>
                            <div>
                              <div className="font-medium">{option.label}</div>
                              <div className="text-xs text-gray-500">{option.desc}</div>
                            </div>
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                {/* Advanced Options */}
                <div className="space-y-4">
                  <div className="space-y-2">
                    <label className="text-sm font-medium flex items-center gap-2">
                      Window Mode • विंडो मोड
                      <Button
                        variant={anchoredMode ? "default" : "outline"}
                        size="sm"
                        onClick={() => setAnchoredMode(!anchoredMode)}
                        data-testid="toggle-anchored-mode"
                      >
                        {anchoredMode ? 'Anchored' : 'Quick Report'}
                      </Button>
                    </label>
                    <p className="text-xs text-gray-500">
                      {anchoredMode 
                        ? 'Windows continue from last payment dates (persisted)' 
                        : 'Windows partition only within selected date range'}
                    </p>
                  </div>
                  
                  <div className="space-y-2">
                    <label className="text-sm font-medium flex items-center gap-2">
                      Fixed Windows Override • फिक्स्ड विंडो ओवरराइड
                      <Button
                        variant={useFixedWindows ? "default" : "outline"}
                        size="sm"
                        onClick={() => setUseFixedWindows(!useFixedWindows)}
                        data-testid="toggle-fixed-windows"
                      >
                        {useFixedWindows ? 'Fixed' : 'Calculated'}
                      </Button>
                    </label>
                    <p className="text-xs text-gray-500">
                      {useFixedWindows 
                        ? 'Use business-defined fixed windows (e.g., D+2 TwiceWeek: Delivered 2025-07-24→2025-08-09, Order 2025-07-28→2025-08-12)' 
                        : 'Calculate windows automatically based on cutoff and frequency'}
                    </p>
                  </div>
                </div>

                {/* Generate Button */}
                <Button 
                  onClick={generateReport}
                  disabled={isGenerating || generateReportMutation.isPending}
                  size="lg"
                  className="w-full"
                  data-testid="button-generate-report"
                >
                  <Calculator className="h-4 w-4 mr-2" />
                  {isGenerating || generateReportMutation.isPending ? 'Generating Report...' : 'Generate Payout Planner Report'}
                </Button>

              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="results" className="space-y-6">
            {reportData ? (
              <>
                {/* Purpose Statement */}
                {reportData.rows[0]?.purposeStatement && (
                  <Card className="border-blue-200 bg-blue-50">
                    <CardContent className="p-4">
                      <h3 className="font-semibold text-blue-800 mb-2">Purpose Statement</h3>
                      <p className="text-sm text-blue-700">{reportData.rows[0].purposeStatement}</p>
                    </CardContent>
                  </Card>
                )}

                {/* Fixed Windows Display */}
                <Card>
                  <CardContent className="p-4">
                    <h3 className="font-semibold mb-3">Fixed Windows (D+2 Twice Weekly)</h3>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div className="bg-green-50 p-3 rounded-lg">
                        <div className="font-medium text-green-800">Delivered Window (COD & Product Cost)</div>
                        <div className="text-sm text-green-600">24 Jul 2025 → 09 Aug 2025</div>
                      </div>
                      <div className="bg-orange-50 p-3 rounded-lg">
                        <div className="font-medium text-orange-800">Order Window (Shipping)</div>
                        <div className="text-sm text-orange-600">29 Jul 2025 → 12 Aug 2025</div>
                      </div>
                    </div>
                    <p className="text-xs text-gray-500 mt-2">
                      Shipping computed by Order Date window; COD & Product Cost by Delivered Date window.
                    </p>
                  </CardContent>
                </Card>

                {/* Summary Cards */}
                <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
                  <Card>
                    <CardContent className="p-4">
                      <div className="text-2xl font-bold text-blue-600">{reportData.summary.totalRuns}</div>
                      <div className="text-sm text-gray-500">Payment Sheets</div>
                    </CardContent>
                  </Card>
                  <Card>
                    <CardContent className="p-4">
                      <div className="text-2xl font-bold text-green-600">₹{reportData.summary.grandCodTotal.toLocaleString('en-IN')}</div>
                      <div className="text-sm text-gray-500">Total COD</div>
                    </CardContent>
                  </Card>
                  <Card>
                    <CardContent className="p-4">
                      <div className="text-2xl font-bold text-orange-600">₹{reportData.summary.grandShippingTotal.toLocaleString('en-IN')}</div>
                      <div className="text-sm text-gray-500">Shipping Costs</div>
                    </CardContent>
                  </Card>
                  <Card>
                    <CardContent className="p-4">
                      <div className="text-2xl font-bold text-purple-600">₹{reportData.summary.grandProductCostTotal.toLocaleString('en-IN')}</div>
                      <div className="text-sm text-gray-500">Product Costs</div>
                    </CardContent>
                  </Card>
                  <Card>
                    <CardContent className="p-4">
                      <div className="text-2xl font-bold text-indigo-600">₹{reportData.summary.grandPayable.toLocaleString('en-IN')}</div>
                      <div className="text-sm text-gray-500">Final Payable</div>
                    </CardContent>
                  </Card>
                </div>

                {/* Download Actions */}
                <Card>
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div>
                        <h3 className="text-lg font-semibold">Report Ready</h3>
                        <p className="text-sm text-gray-500">
                          {reportData.summary.totalRuns} sheets, Total Payable: ₹{reportData.summary.grandPayable.toLocaleString('en-IN')}. Download to share.
                        </p>
                        {reportData.generatedRunDates.length > 0 && (
                          <p className="text-xs text-gray-400 mt-1">
                            Generated run dates: {reportData.generatedRunDates.join(', ')}
                          </p>
                        )}
                      </div>
                      <Button onClick={downloadExcel} size="lg" data-testid="button-download-excel">
                        <Download className="h-4 w-4 mr-2" />
                        Download Excel
                      </Button>
                    </div>
                  </CardContent>
                </Card>

                {/* Data Table */}
                <Card>
                  <CardHeader>
                    <CardTitle>Payout Schedule Details</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b">
                            <th className="text-left p-2">Run Date</th>
                            <th className="text-left p-2">Cycle</th>
                            <th className="text-left p-2">Order Window</th>
                            <th className="text-left p-2">Delivered Window</th>
                            <th className="text-right p-2">Orders</th>
                            <th className="text-right p-2">COD Total</th>
                            <th className="text-right p-2">Shipping</th>
                            <th className="text-right p-2">Product Cost</th>
                            <th className="text-right p-2">Final Payable</th>
                            <th className="text-left p-2">Dropshipper</th>
                          </tr>
                        </thead>
                        <tbody>
                          {reportData.rows.map((row, index) => (
                            <tr key={index} className="border-b hover:bg-gray-50 dark:hover:bg-gray-800">
                              <td className="p-2">{new Date(row.runDate).toLocaleDateString('en-IN')}</td>
                              <td className="p-2">{row.cycle}</td>
                              <td className="p-2">{row.orderStart} to {row.orderEnd}</td>
                              <td className="p-2">{row.delStart} to {row.delEnd}</td>
                              <td className="p-2 text-right">{row.ordersCount}</td>
                              <td className="p-2 text-right">₹{row.codTotal.toLocaleString('en-IN')}</td>
                              <td className="p-2 text-right">₹{row.shippingTotal.toLocaleString('en-IN')}</td>
                              <td className="p-2 text-right">₹{row.productCostTotal.toLocaleString('en-IN')}</td>
                              <td className="p-2 text-right font-semibold">₹{row.finalPayable.toLocaleString('en-IN')}</td>
                              <td className="p-2">{row.dropshipperEmail === 'all' ? 'All' : row.dropshipperEmail}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </CardContent>
                </Card>

                {/* Detailed Rows Preview (first 5 rows) */}
                {reportData.detailedRows && reportData.detailedRows.length > 0 && (
                  <Card>
                    <CardHeader>
                      <CardTitle>Sample Per-Order Details (Export में complete data मिलेगा)</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="overflow-x-auto">
                        <table className="w-full text-xs">
                          <thead>
                            <tr className="border-b">
                              <th className="text-left p-1">Order ID</th>
                              <th className="text-left p-1">Product</th>
                              <th className="text-right p-1">Shipped</th>
                              <th className="text-right p-1">Delivered</th>
                              <th className="text-right p-1">COD Rate</th>
                              <th className="text-right p-1">COD Received</th>
                              <th className="text-right p-1">Product Cost</th>
                              <th className="text-right p-1">Payable</th>
                            </tr>
                          </thead>
                          <tbody>
                            {reportData.detailedRows.slice(0, 5).map((row, index) => (
                              <tr key={index} className="border-b hover:bg-gray-50">
                                <td className="p-1">{row.orderId}</td>
                                <td className="p-1">{row.product}</td>
                                <td className="p-1 text-right">{row.shippedQty}</td>
                                <td className="p-1 text-right">{row.deliveredQty}</td>
                                <td className="p-1 text-right">₹{row.codRate.toFixed(2)}</td>
                                <td className="p-1 text-right">₹{row.codReceived.toLocaleString('en-IN')}</td>
                                <td className="p-1 text-right">₹{row.productCost.toLocaleString('en-IN')}</td>
                                <td className="p-1 text-right font-semibold">₹{row.payableAmount.toLocaleString('en-IN')}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                        {reportData.detailedRows.length > 5 && (
                          <p className="text-xs text-gray-500 mt-2">
                            Showing 5 of {reportData.detailedRows.length} detailed rows. Download Excel for complete data.
                          </p>
                        )}
                      </div>
                    </CardContent>
                  </Card>
                )}

                {/* Warnings */}
                {(reportData.missingPrices > 0 || reportData.missingProviders > 0) && (
                  <Card className="border-orange-200 bg-orange-50">
                    <CardContent className="p-4">
                      <h3 className="font-semibold text-orange-800">Data Completeness Warning</h3>
                      <p className="text-sm text-orange-700">
                        {reportData.missingPrices} products / {reportData.missingProviders} providers need rates. 
                        Fill in Settings or upload template to include amounts.
                      </p>
                    </CardContent>
                  </Card>
                )}
              </>
            ) : (
              <Card>
                <CardContent className="p-8 text-center">
                  <FileSpreadsheet className="h-12 w-12 mx-auto text-gray-400 mb-4" />
                  <h3 className="text-lg font-semibold text-gray-500">No Report Generated</h3>
                  <p className="text-sm text-gray-400">Generate a report first to view results</p>
                </CardContent>
              </Card>
            )}
          </TabsContent>
        </Tabs>

      </div>
    </div>
  );
}