import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Calendar, Clock, Download, Settings, Play, Calculator, FileSpreadsheet } from 'lucide-react';
import { Link } from 'wouter';
import { useToast } from '@/hooks/use-toast';
import { cn } from '@/lib/utils';

interface SettlementSettings {
  id?: string;
  frequency: 'monthly' | 'twice_weekly' | 'thrice_weekly';
  lastPaymentDoneOn: string | null;
  lastDeliveredCutoff: string | null;
  dPlus2Enabled: boolean;
  createdAt?: string;
  updatedAt?: string;
}

interface SettlementPreview {
  orderStart: string;
  orderEnd: string;
  delStart: string;
  delEnd: string;
  shippingTotal: number;
  codTotal: number;
  productCostTotal: number;
  adjustmentsTotal: number;
  finalPayable: number;
  ordersCount: number;
  dropshippers: Array<{
    email: string;
    finalPayable: number;
    ordersCount: number;
    codTotal: number;
    shippingTotal: number;
    productCostTotal: number;
  }>;
}

const frequencyLabels = {
  monthly: { label: 'Monthly (मासिक)', icon: '📅', desc: 'Once per month' },
  twice_weekly: { label: 'Twice Weekly (सप्ताह में दो बार)', icon: '📊', desc: 'Tuesday & Friday' },
  thrice_weekly: { label: 'Thrice Weekly (सप्ताह में तीन बार)', icon: '⚡', desc: 'Monday, Wednesday & Friday' }
};

// July 2025 specific dates as per user requirements
const getJuly2025Dates = (frequency: string): string[] => {
  switch (frequency) {
    case 'twice_weekly':
      // Tue: 1, 8, 15, 22, 29; Fri: 4, 11, 18, 25 (total 9)
      return ['2025-07-01', '2025-07-04', '2025-07-08', '2025-07-11', '2025-07-15', 
              '2025-07-18', '2025-07-22', '2025-07-25', '2025-07-29'];
    
    case 'thrice_weekly':
      // Mon: 7, 14, 21, 28; Wed: 2, 9, 16, 23, 30; Fri: 4, 11, 18, 25 (total 13)
      return ['2025-07-02', '2025-07-04', '2025-07-07', '2025-07-09', '2025-07-11',
              '2025-07-14', '2025-07-16', '2025-07-18', '2025-07-21', '2025-07-23',
              '2025-07-25', '2025-07-28', '2025-07-30'];
    
    case 'monthly':
      // Any date in month (example: 31 Jul 2025) - total 1
      return ['2025-07-31'];
    
    default:
      return [];
  }
};

const getNextEligibleDate = (frequency: string): string => {
  const july2025Dates = getJuly2025Dates(frequency);
  const today = new Date();
  const todayStr = today.toISOString().split('T')[0];
  
  // Find next eligible date from July 2025 schedule
  const nextDate = july2025Dates.find(date => date >= todayStr);
  
  if (nextDate) {
    return `${new Date(nextDate).toLocaleDateString('en-IN')} (${july2025Dates.length} total in July)`;
  }
  
  return `${july2025Dates.length} settlement dates in July 2025`;
};

export default function SettlementScheduler() {
  const [frequency, setFrequency] = useState<string>('monthly');
  const [runDate, setRunDate] = useState<string>(new Date().toISOString().split('T')[0]);
  const [lastPaymentDoneOn, setLastPaymentDoneOn] = useState<string>('');
  const [lastDeliveredCutoff, setLastDeliveredCutoff] = useState<string>('');
  const [dPlus2Enabled, setDPlus2Enabled] = useState<boolean>(true);
  const [selectedDropshipper, setSelectedDropshipper] = useState<string>('all');
  const [isGeneratingPreview, setIsGeneratingPreview] = useState(false);
  const [previewData, setPreviewData] = useState<SettlementPreview | null>(null);
  
  const { toast } = useToast();
  const queryClient = useQueryClient();

  // Fetch current settlement settings
  const { data: settings } = useQuery<SettlementSettings>({
    queryKey: ['/api/settlement-settings'],
    queryFn: async () => {
      const response = await fetch('/api/settlement-settings');
      if (!response.ok) throw new Error('Failed to fetch settings');
      return response.json();
    },
  });

  // Load settings when fetched
  useEffect(() => {
    if (settings) {
      setFrequency(settings.frequency);
      setLastPaymentDoneOn(settings.lastPaymentDoneOn || '');
      setLastDeliveredCutoff(settings.lastDeliveredCutoff || '');
      setDPlus2Enabled(settings.dPlus2Enabled);
    }
  }, [settings]);

  // Auto-set next eligible date when frequency changes
  useEffect(() => {
    if (frequency !== 'monthly') {
      setRunDate(getNextEligibleDate(frequency));
    }
  }, [frequency]);

  // Save settings mutation
  const saveSettingsMutation = useMutation({
    mutationFn: async (settingsData: Partial<SettlementSettings>) => {
      const response = await fetch('/api/settlement-settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(settingsData),
      });
      if (!response.ok) throw new Error('Failed to save settings');
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/settlement-settings'] });
      toast({ title: 'Settings saved successfully!' });
    },
  });

  // Fetch dropshippers for dropdown
  const { data: dropshippers = [] } = useQuery<string[]>({
    queryKey: ['/api/dropshippers'],
    queryFn: async () => {
      const response = await fetch('/api/dropshippers');
      if (!response.ok) throw new Error('Failed to fetch dropshippers');
      return response.json();
    },
  });

  // Generate preview mutation
  const generatePreviewMutation = useMutation({
    mutationFn: async (params: any) => {
      const response = await fetch('/api/settlement-preview', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(params),
      });
      if (!response.ok) throw new Error('Failed to generate preview');
      return response.json();
    },
    onSuccess: (data) => {
      setPreviewData(data);
      toast({ title: 'Preview generated successfully!' });
    },
  });

  // Export settlement mutation
  const exportSettlementMutation = useMutation({
    mutationFn: async (params: any) => {
      const response = await fetch('/api/export-settlement', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(params),
      });
      if (!response.ok) throw new Error('Failed to export settlement');
      return response.blob();
    },
    onSuccess: (blob) => {
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `Settlement_${runDate.replace(/-/g, '')}.xlsx`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
      
      // Update anchors after successful export
      const newSettings = {
        frequency: frequency as "monthly" | "twice_weekly" | "thrice_weekly",
        lastPaymentDoneOn: runDate,
        lastDeliveredCutoff: previewData?.delEnd || '',
        dPlus2Enabled,
      };
      saveSettingsMutation.mutate(newSettings);
      
      toast({ title: 'Settlement exported successfully!' });
    },
  });

  const saveSettings = () => {
    saveSettingsMutation.mutate({
      frequency: frequency as any,
      lastPaymentDoneOn: lastPaymentDoneOn || null,
      lastDeliveredCutoff: lastDeliveredCutoff || null,
      dPlus2Enabled,
    });
  };

  const generatePreview = () => {
    setIsGeneratingPreview(true);
    generatePreviewMutation.mutate({
      runDate,
      lastPaymentDoneOn: lastPaymentDoneOn || null,
      lastDeliveredCutoff: lastDeliveredCutoff || null,
      dPlus2Enabled,
      selectedDropshipper: selectedDropshipper !== 'all' ? selectedDropshipper : null,
    });
    setIsGeneratingPreview(false);
  };

  const exportSettlement = () => {
    if (!previewData) {
      toast({ title: 'Please generate preview first', variant: 'destructive' });
      return;
    }
    
    exportSettlementMutation.mutate({
      runDate,
      lastPaymentDoneOn: lastPaymentDoneOn || null,
      lastDeliveredCutoff: lastDeliveredCutoff || null,
      dPlus2Enabled,
      previewData,
    });
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100 dark:from-gray-900 dark:to-gray-800">
      <div className="container mx-auto p-6 space-y-8">
        {/* Header */}
        <div className="text-center space-y-4">
          <div className="flex items-center justify-center gap-3 mb-4">
            <div className="w-12 h-12 bg-gradient-to-r from-green-500 to-blue-600 rounded-xl flex items-center justify-center">
              <Calendar className="w-6 h-6 text-white" />
            </div>
            <div>
              <h1 className="text-3xl font-bold bg-gradient-to-r from-green-600 to-blue-600 bg-clip-text text-transparent">
                Settlement Scheduler
              </h1>
              <p className="text-gray-600 dark:text-gray-400">
                सेटलमेंट शेड्यूलर • Manual D+2, Weekly/Monthly settlements
              </p>
            </div>
          </div>
          
          <Link href="/">
            <Button variant="outline" className="mb-6">
              ← Back to Dashboard
            </Button>
          </Link>
        </div>

        <Tabs defaultValue="settings" className="w-full">
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="settings" className="flex items-center gap-2">
              <Settings className="w-4 h-4" />
              Settings
            </TabsTrigger>
            <TabsTrigger value="preview" className="flex items-center gap-2">
              <Calculator className="w-4 h-4" />
              Preview
            </TabsTrigger>
            <TabsTrigger value="export" className="flex items-center gap-2">
              <FileSpreadsheet className="w-4 h-4" />
              Export
            </TabsTrigger>
          </TabsList>

          {/* Settings Tab */}
          <TabsContent value="settings" className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle>Settlement Configuration • सेटलमेंट कॉन्फ़िगरेशन</CardTitle>
                <CardDescription>
                  Configure frequency, dates, and D+2 mode for automated settlements
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                {/* Frequency Selection */}
                <div className="space-y-3">
                  <label className="text-sm font-medium">Frequency • आवृत्ति</label>
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    {Object.entries(frequencyLabels).map(([key, info]) => (
                      <Card 
                        key={key}
                        className={cn(
                          "cursor-pointer transition-all border-2",
                          frequency === key ? "border-blue-500 bg-blue-50 dark:bg-blue-900/20" : "hover:border-gray-300"
                        )}
                        onClick={() => setFrequency(key)}
                      >
                        <CardContent className="p-4 text-center space-y-2">
                          <div className="text-2xl">{info.icon}</div>
                          <div className="font-semibold text-sm">{info.label}</div>
                          <div className="text-xs text-gray-600">{info.desc}</div>
                        </CardContent>
                      </Card>
                    ))}
                  </div>
                </div>

                {/* Run Date and Dropshipper Selection */}
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Run Settlement Date • सेटलमेंट रन डेट</label>
                    <input 
                      type="date" 
                      value={runDate}
                      onChange={(e) => setRunDate(e.target.value)}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                      data-testid="input-run-date"
                    />
                    {frequency !== 'monthly' && (
                      <p className="text-xs text-gray-500">
                        Next eligible: {getNextEligibleDate(frequency)}
                      </p>
                    )}
                  </div>

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
                  
                  <div className="space-y-2">
                    <label className="text-sm font-medium flex items-center gap-2">
                      D+2 Mode
                      <Button
                        variant={dPlus2Enabled ? "default" : "outline"}
                        size="sm"
                        onClick={() => setDPlus2Enabled(!dPlus2Enabled)}
                        data-testid="toggle-d-plus-2"
                      >
                        {dPlus2Enabled ? 'ON' : 'OFF'}
                      </Button>
                    </label>
                    <p className="text-xs text-gray-500">
                      {dPlus2Enabled ? 'Delivered cutoff = run_date - 2 days' : 'Delivered cutoff = run_date'}
                    </p>
                  </div>
                </div>

                {/* Anchor Dates */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Last Payment Done On</label>
                    <input 
                      type="date" 
                      value={lastPaymentDoneOn}
                      onChange={(e) => setLastPaymentDoneOn(e.target.value)}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                      data-testid="input-last-payment"
                    />
                  </div>
                  
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Last Delivered Cutoff</label>
                    <input 
                      type="date" 
                      value={lastDeliveredCutoff}
                      onChange={(e) => setLastDeliveredCutoff(e.target.value)}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                      data-testid="input-last-cutoff"
                    />
                  </div>
                </div>

                <Button 
                  onClick={saveSettings}
                  disabled={saveSettingsMutation.isPending}
                  className="w-full"
                  data-testid="button-save-settings"
                >
                  {saveSettingsMutation.isPending ? 'Saving...' : 'Save Settings • सेटिंग्स सेव करें'}
                </Button>
              </CardContent>
            </Card>
          </TabsContent>

          {/* Preview Tab */}
          <TabsContent value="preview" className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle>Settlement Preview • सेटलमेंट प्रीव्यू</CardTitle>
                <CardDescription>
                  Generate preview to see calculated windows and amounts
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <Button 
                  onClick={generatePreview}
                  disabled={isGeneratingPreview || generatePreviewMutation.isPending}
                  className="w-full"
                  data-testid="button-generate-preview"
                >
                  <Play className="w-4 h-4 mr-2" />
                  {generatePreviewMutation.isPending ? 'Generating...' : 'Generate Preview • प्रीव्यू जेनरेट करें'}
                </Button>

                {previewData && (
                  <div className="space-y-4">
                    {/* Date Windows */}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <Card className="bg-blue-50 dark:bg-blue-900/20">
                        <CardContent className="p-4">
                          <h4 className="font-semibold text-blue-800 dark:text-blue-200 mb-2">Order Date Window</h4>
                          <p className="text-sm">{previewData.orderStart} → {previewData.orderEnd}</p>
                        </CardContent>
                      </Card>
                      <Card className="bg-green-50 dark:bg-green-900/20">
                        <CardContent className="p-4">
                          <h4 className="font-semibold text-green-800 dark:text-green-200 mb-2">Delivered Date Window</h4>
                          <p className="text-sm">{previewData.delStart} → {previewData.delEnd}</p>
                        </CardContent>
                      </Card>
                    </div>

                    {/* Summary Cards */}
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                      <Card>
                        <CardContent className="p-4 text-center">
                          <div className="text-2xl font-bold text-blue-600">₹{previewData.shippingTotal.toLocaleString()}</div>
                          <div className="text-sm text-gray-600">Shipping Total</div>
                        </CardContent>
                      </Card>
                      <Card>
                        <CardContent className="p-4 text-center">
                          <div className="text-2xl font-bold text-green-600">₹{previewData.codTotal.toLocaleString()}</div>
                          <div className="text-sm text-gray-600">COD Total</div>
                        </CardContent>
                      </Card>
                      <Card>
                        <CardContent className="p-4 text-center">
                          <div className="text-2xl font-bold text-orange-600">₹{previewData.productCostTotal.toLocaleString()}</div>
                          <div className="text-sm text-gray-600">Product Cost</div>
                        </CardContent>
                      </Card>
                      <Card>
                        <CardContent className="p-4 text-center">
                          <div className="text-2xl font-bold text-purple-600">₹{previewData.finalPayable.toLocaleString()}</div>
                          <div className="text-sm text-gray-600">Final Payable</div>
                        </CardContent>
                      </Card>
                    </div>

                    {/* Dropshipper Details */}
                    <Card>
                      <CardHeader>
                        <CardTitle>Dropshipper Details • ड्रॉपशिपर विवरण</CardTitle>
                      </CardHeader>
                      <CardContent>
                        <div className="space-y-3">
                          {previewData.dropshippers.map((ds, index) => (
                            <div 
                              key={index}
                              className="flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-700 rounded-lg"
                            >
                              <div>
                                <div className="font-medium">{ds.email}</div>
                                <div className="text-sm text-gray-600">{ds.ordersCount} orders</div>
                              </div>
                              <div className="text-right">
                                <div className="text-lg font-bold text-purple-600">₹{ds.finalPayable.toLocaleString()}</div>
                                <div className="text-xs text-gray-500">
                                  COD: ₹{ds.codTotal.toLocaleString()} | 
                                  Ship: ₹{ds.shippingTotal.toLocaleString()} | 
                                  Cost: ₹{ds.productCostTotal.toLocaleString()}
                                </div>
                              </div>
                            </div>
                          ))}
                        </div>
                      </CardContent>
                    </Card>
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          {/* Export Tab */}
          <TabsContent value="export" className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle>Export Settlement • सेटलमेंट एक्सपोर्ट</CardTitle>
                <CardDescription>
                  Export comprehensive settlement workbook with all dropshipper data
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                {previewData ? (
                  <div className="space-y-4">
                    <div className="p-4 bg-green-50 dark:bg-green-900/20 rounded-lg">
                      <h4 className="font-semibold text-green-800 dark:text-green-200 mb-2">Ready to Export</h4>
                      <p className="text-sm text-green-700 dark:text-green-300">
                        Preview generated successfully. Final payable: ₹{previewData.finalPayable.toLocaleString()}
                      </p>
                    </div>
                    
                    <Button 
                      onClick={exportSettlement}
                      disabled={exportSettlementMutation.isPending}
                      className="w-full"
                      size="lg"
                      data-testid="button-export-settlement"
                    >
                      <Download className="w-4 h-4 mr-2" />
                      {exportSettlementMutation.isPending ? 'Exporting...' : 'Export Settlement Workbook • सेटलमेंट एक्सपोर्ट करें'}
                    </Button>
                  </div>
                ) : (
                  <div className="text-center py-8">
                    <Calculator className="w-16 h-16 text-gray-400 mx-auto mb-4" />
                    <p className="text-gray-500">Please generate preview first to export settlement</p>
                    <p className="text-sm text-gray-400">पहले प्रीव्यू जेनरेट करें</p>
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}