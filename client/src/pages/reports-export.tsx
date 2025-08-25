import React, { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Badge } from '@/components/ui/badge';
import { Calendar, Download, Settings, History, Zap, Clock, Target, TrendingUp } from 'lucide-react';
import { Link } from 'wouter';
import { useToast } from '@/hooks/use-toast';
import { cn } from '@/lib/utils';

interface PaymentCycle {
  id: string;
  dropshipperEmail: string;
  cycleType: 'daily' | 'weekly' | 'biweekly' | 'monthly' | 'custom';
  cycleParams: any;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
}

interface ExportHistory {
  id: string;
  dropshipperEmail: string;
  exportType: string;
  dateRangeFrom: string | null;
  dateRangeTo: string | null;
  paymentCycleId: string | null;
  totalRecords: number;
  fileSize: number | null;
  exportedAt: string;
  exportParams: any;
}

const cycleTypeLabels = {
  daily: { label: 'Daily (दैनिक)', color: 'bg-blue-500', icon: '📅' },
  weekly: { label: 'Weekly (साप्ताहिक)', color: 'bg-green-500', icon: '📊' },
  biweekly: { label: 'Bi-weekly (पाक्षिक)', color: 'bg-purple-500', icon: '⚡' },
  monthly: { label: 'Monthly (मासिक)', color: 'bg-orange-500', icon: '📈' },
  custom: { label: 'Custom Range (कस्टम रेंज)', color: 'bg-indigo-500', icon: '🎯' }
};

const formatFileSize = (bytes: number | null) => {
  if (!bytes) return 'N/A';
  const mb = bytes / (1024 * 1024);
  return mb < 1 ? `${(bytes / 1024).toFixed(1)}KB` : `${mb.toFixed(1)}MB`;
};

export default function ReportsExport() {
  const [selectedDropshipper, setSelectedDropshipper] = useState<string>('');
  const [selectedCycle, setSelectedCycle] = useState<string>('');
  const [isGeneratingReport, setIsGeneratingReport] = useState(false);
  const [customFromDate, setCustomFromDate] = useState('');
  const [customToDate, setCustomToDate] = useState('');
  const { toast } = useToast();
  const queryClient = useQueryClient();

  // Fetch dropshippers
  const { data: dropshippers = [] } = useQuery({
    queryKey: ['/api/dropshippers'],
  });

  // Fetch payment cycles for selected dropshipper
  const { data: paymentCycles = [] } = useQuery<PaymentCycle[]>({
    queryKey: ['/api/payment-cycles', selectedDropshipper],
    queryFn: async () => {
      const response = await fetch(`/api/payment-cycles?dropshipperEmail=${selectedDropshipper}`);
      if (!response.ok) throw new Error('Failed to fetch payment cycles');
      return response.json();
    },
    enabled: !!selectedDropshipper,
  });

  // Fetch export history for selected dropshipper
  const { data: exportHistory = [] } = useQuery<ExportHistory[]>({
    queryKey: ['/api/export-history', selectedDropshipper],
    queryFn: async () => {
      const response = await fetch(`/api/export-history?dropshipperEmail=${selectedDropshipper}`);
      if (!response.ok) throw new Error('Failed to fetch export history');
      return response.json();
    },
    enabled: !!selectedDropshipper,
  });

  // Mutation to create/update payment cycle
  const createCycleMutation = useMutation({
    mutationFn: async (cycleData: any) => {
      const response = await fetch('/api/payment-cycles', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(cycleData),
      });
      if (!response.ok) throw new Error('Failed to create payment cycle');
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/payment-cycles'] });
      toast({ title: 'Payment cycle updated successfully!' });
    },
  });

  // Generate payment report
  const generateReport = async (format: 'json' | 'excel' = 'excel') => {
    if (!selectedDropshipper) {
      toast({ title: 'Please select a dropshipper first', variant: 'destructive' });
      return;
    }

    setIsGeneratingReport(true);
    try {
      const response = await fetch('/api/generate-payment-report', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dropshipperEmail: selectedDropshipper,
          paymentCycleId: selectedCycle === 'manual' ? undefined : selectedCycle,
          format,
        }),
      });

      if (!response.ok) throw new Error('Failed to generate report');

      if (format === 'excel') {
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `payment_report_${selectedDropshipper}_${new Date().toISOString().split('T')[0]}.xlsx`;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
        
        toast({ title: 'Report downloaded successfully!' });
      } else {
        const data = await response.json();
        console.log('Report data:', data);
        toast({ title: 'Report generated successfully!' });
      }

      // Refresh export history
      queryClient.invalidateQueries({ queryKey: ['/api/export-history'] });
    } catch (error) {
      console.error('Error generating report:', error);
      toast({ title: 'Failed to generate report', variant: 'destructive' });
    } finally {
      setIsGeneratingReport(false);
    }
  };

  const setupPaymentCycle = (cycleType: string) => {
    if (!selectedDropshipper) {
      toast({ title: 'Please select a dropshipper first', variant: 'destructive' });
      return;
    }

    let cycleParams = {};
    switch (cycleType) {
      case 'daily':
        cycleParams = { daysOffset: 2 }; // D+2 settlement
        break;
      case 'weekly':
        cycleParams = { weekOffset: 0, dayOfWeek: 1 }; // Weekly on Monday
        break;
      case 'biweekly':
        cycleParams = { biweekOffset: 0, dayOfWeek: 1 }; // Bi-weekly on Monday
        break;
      case 'monthly':
        cycleParams = { monthOffset: 0, dayOfMonth: 1 }; // Monthly on 1st
        break;
      case 'custom':
        if (!customFromDate || !customToDate) {
          toast({ title: 'Please select both from and to dates for custom range', variant: 'destructive' });
          return;
        }
        cycleParams = { 
          fromDate: customFromDate, 
          toDate: customToDate,
          description: `Custom range from ${customFromDate} to ${customToDate}`
        };
        break;
    }

    createCycleMutation.mutate({
      dropshipperEmail: selectedDropshipper,
      cycleType,
      cycleParams,
    });
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100 dark:from-gray-900 dark:to-gray-800">
      <div className="container mx-auto p-6 space-y-8">
        {/* Header */}
        <div className="text-center space-y-4">
          <div className="flex items-center justify-center gap-3 mb-4">
            <div className="w-12 h-12 bg-gradient-to-r from-blue-500 to-purple-600 rounded-xl flex items-center justify-center">
              <TrendingUp className="w-6 h-6 text-white" />
            </div>
            <div>
              <h1 className="text-3xl font-bold bg-gradient-to-r from-blue-600 to-purple-600 bg-clip-text text-transparent">
                Reports & Export
              </h1>
              <p className="text-gray-600 dark:text-gray-400">
                रिपोर्ट और निर्यात • Payment cycle management और download history
              </p>
            </div>
          </div>
          
          <Link href="/">
            <Button variant="outline" className="mb-6">
              ← Back to Dashboard
            </Button>
          </Link>
        </div>

        {/* Dropshipper Selection */}
        <Card className="bg-white/80 dark:bg-gray-800/80 backdrop-blur border-0 shadow-lg">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Target className="w-5 h-5 text-blue-500" />
              Select Dropshipper • ड्रॉपशिपर चुनें
            </CardTitle>
            <CardDescription>
              Choose a dropshipper to manage payment cycles and generate reports
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Select value={selectedDropshipper} onValueChange={setSelectedDropshipper}>
              <SelectTrigger className="w-full">
                <SelectValue placeholder="Select dropshipper..." />
              </SelectTrigger>
              <SelectContent>
                {dropshippers.map((email: string) => (
                  <SelectItem key={email} value={email}>
                    {email}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </CardContent>
        </Card>

        {selectedDropshipper && (
          <Tabs defaultValue="cycles" className="space-y-6">
            <TabsList className="grid w-full grid-cols-3 bg-white/80 dark:bg-gray-800/80 backdrop-blur">
              <TabsTrigger value="cycles" className="flex items-center gap-2">
                <Settings className="w-4 h-4" />
                Payment Cycles
              </TabsTrigger>
              <TabsTrigger value="generate" className="flex items-center gap-2">
                <Download className="w-4 h-4" />
                Generate Reports
              </TabsTrigger>
              <TabsTrigger value="history" className="flex items-center gap-2">
                <History className="w-4 h-4" />
                Export History
              </TabsTrigger>
            </TabsList>

            {/* Payment Cycles Tab */}
            <TabsContent value="cycles" className="space-y-6">
              <Card className="bg-white/80 dark:bg-gray-800/80 backdrop-blur border-0 shadow-lg">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Clock className="w-5 h-5 text-blue-500" />
                    Payment Cycle Configuration • भुगतान चक्र सेटअप
                  </CardTitle>
                  <CardDescription>
                    Set up automated payment cycles for {selectedDropshipper}
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-6">
                  {/* Current Cycle Display */}
                  {paymentCycles.length > 0 && (
                    <div className="p-4 bg-gradient-to-r from-green-50 to-blue-50 dark:from-green-900/20 dark:to-blue-900/20 rounded-lg">
                      <h4 className="font-semibold text-green-700 dark:text-green-300 mb-2">
                        🎯 Current Active Cycle • वर्तमान सक्रिय चक्र
                      </h4>
                      {paymentCycles.filter(c => c.isActive).map(cycle => (
                        <div key={cycle.id} className="flex items-center gap-3">
                          <Badge className={cn('text-white', cycleTypeLabels[cycle.cycleType].color)}>
                            {cycleTypeLabels[cycle.cycleType].icon} {cycleTypeLabels[cycle.cycleType].label}
                          </Badge>
                          <span className="text-sm text-gray-600 dark:text-gray-400">
                            Parameters: {JSON.stringify(cycle.cycleParams)}
                          </span>
                        </div>
                      ))}
                    </div>
                  )}

                  {/* Payment Cycle Presets */}
                  <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                    {Object.entries(cycleTypeLabels).map(([type, info]) => (
                      <Card 
                        key={type} 
                        className={cn(
                          "cursor-pointer hover:shadow-md transition-all border-2",
                          type === 'custom' && (!customFromDate || !customToDate) 
                            ? "opacity-50 cursor-not-allowed" 
                            : "hover:border-blue-300"
                        )}
                        onClick={() => {
                          if (type === 'custom' && (!customFromDate || !customToDate)) {
                            return; // Don't allow click if custom dates not set
                          }
                          setupPaymentCycle(type);
                        }}
                      >
                        <CardContent className="p-4 text-center space-y-2">
                          <div className="text-2xl">{info.icon}</div>
                          <div className="font-semibold text-sm">{info.label}</div>
                          <Badge className={cn('text-white text-xs', info.color)}>
                            {type === 'daily' && 'D+2 Settlement'}
                            {type === 'weekly' && 'Every Monday'}
                            {type === 'biweekly' && 'Bi-weekly'}
                            {type === 'monthly' && 'Monthly'}
                            {type === 'custom' && 'User Defined'}
                          </Badge>
                        </CardContent>
                      </Card>
                    ))}
                  </div>

                  {/* Custom Date Range Inputs */}
                  <div className="mt-6 p-4 bg-indigo-50 dark:bg-indigo-900/20 rounded-lg border border-indigo-200 dark:border-indigo-700">
                    <h4 className="text-sm font-semibold text-indigo-800 dark:text-indigo-200 mb-3 flex items-center gap-2">
                      🎯 Custom Date Range for Payment Cycle • कस्टम दिनांक सीमा
                    </h4>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <label className="text-sm font-medium text-indigo-700 dark:text-indigo-300">From Date • शुरुआती दिनांक</label>
                        <input 
                          type="date" 
                          value={customFromDate}
                          onChange={(e) => setCustomFromDate(e.target.value)}
                          className="w-full px-3 py-2 text-sm border border-indigo-300 dark:border-indigo-600 rounded-md focus:outline-none focus:ring-2 focus:ring-indigo-500 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
                          data-testid="input-custom-from-date"
                        />
                      </div>
                      <div className="space-y-2">
                        <label className="text-sm font-medium text-indigo-700 dark:text-indigo-300">To Date • अंतिम दिनांक</label>
                        <input 
                          type="date" 
                          value={customToDate}
                          onChange={(e) => setCustomToDate(e.target.value)}
                          className="w-full px-3 py-2 text-sm border border-indigo-300 dark:border-indigo-600 rounded-md focus:outline-none focus:ring-2 focus:ring-indigo-500 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
                          data-testid="input-custom-to-date"
                        />
                      </div>
                    </div>
                    {customFromDate && customToDate && (
                      <div className="mt-3 p-3 bg-indigo-100 dark:bg-indigo-800/50 rounded-lg">
                        <div className="flex items-center gap-2 text-sm text-indigo-700 dark:text-indigo-200">
                          <span className="text-green-600 dark:text-green-400">✓</span>
                          <span className="font-medium">Selected range:</span>
                          <span className="bg-white dark:bg-gray-700 px-2 py-1 rounded font-mono text-xs">
                            {customFromDate} to {customToDate}
                          </span>
                        </div>
                      </div>
                    )}
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* Generate Reports Tab */}
            <TabsContent value="generate" className="space-y-6">
              <Card className="bg-white/80 dark:bg-gray-800/80 backdrop-blur border-0 shadow-lg">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Zap className="w-5 h-5 text-blue-500" />
                    Generate Payment Report • भुगतान रिपोर्ट जेनरेट करें
                  </CardTitle>
                  <CardDescription>
                    Generate comprehensive payment reports based on configured cycles
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-6">
                  {/* Cycle Selection */}
                  <div className="space-y-3">
                    <label className="text-sm font-medium">Select Payment Cycle (Optional)</label>
                    <Select value={selectedCycle} onValueChange={setSelectedCycle}>
                      <SelectTrigger>
                        <SelectValue placeholder="Choose payment cycle or leave blank for manual dates..." />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="manual">Manual Date Selection</SelectItem>
                        {paymentCycles.map(cycle => (
                          <SelectItem key={cycle.id} value={cycle.id}>
                            {cycleTypeLabels[cycle.cycleType].icon} {cycleTypeLabels[cycle.cycleType].label}
                            {cycle.isActive && ' (Active)'}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  {/* Generate Buttons */}
                  <div className="flex gap-4">
                    <Button 
                      onClick={() => generateReport('excel')}
                      disabled={isGeneratingReport}
                      className="flex-1 bg-gradient-to-r from-green-500 to-green-600 hover:from-green-600 hover:to-green-700"
                    >
                      <Download className="w-4 h-4 mr-2" />
                      {isGeneratingReport ? 'Generating...' : 'Download Excel Report'}
                    </Button>
                    <Button 
                      onClick={() => generateReport('json')}
                      disabled={isGeneratingReport}
                      variant="outline"
                      className="flex-1"
                    >
                      <Calendar className="w-4 h-4 mr-2" />
                      View JSON Data
                    </Button>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* Export History Tab */}
            <TabsContent value="history" className="space-y-6">
              <Card className="bg-white/80 dark:bg-gray-800/80 backdrop-blur border-0 shadow-lg">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <History className="w-5 h-5 text-blue-500" />
                    Export History • निर्यात इतिहास
                  </CardTitle>
                  <CardDescription>
                    View all previous exports and downloads for {selectedDropshipper}
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  {exportHistory.length === 0 ? (
                    <div className="text-center py-8">
                      <History className="w-12 h-12 text-gray-400 mx-auto mb-4" />
                      <p className="text-gray-500">No export history found</p>
                      <p className="text-sm text-gray-400">Generate your first report to see history here</p>
                    </div>
                  ) : (
                    <div className="space-y-3">
                      {exportHistory.map(record => (
                        <div 
                          key={record.id} 
                          className="flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-700/50 rounded-lg"
                        >
                          <div className="space-y-1">
                            <div className="flex items-center gap-2">
                              <Badge variant="outline">{record.exportType}</Badge>
                              <span className="font-medium">{record.totalRecords} records</span>
                            </div>
                            <div className="text-sm text-gray-500">
                              {new Date(record.exportedAt).toLocaleString('en-IN')}
                              {record.dateRangeFrom && record.dateRangeTo && (
                                <span className="ml-2">
                                  • {new Date(record.dateRangeFrom).toLocaleDateString()} to {new Date(record.dateRangeTo).toLocaleDateString()}
                                </span>
                              )}
                            </div>
                          </div>
                          <div className="text-right">
                            <div className="text-sm font-medium">{formatFileSize(record.fileSize)}</div>
                            <div className="text-xs text-gray-500">File Size</div>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </CardContent>
              </Card>
            </TabsContent>
          </Tabs>
        )}
      </div>
    </div>
  );
}