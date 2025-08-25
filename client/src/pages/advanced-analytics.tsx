import { useState, useRef } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Badge } from '@/components/ui/badge';
import { Download, TrendingUp, TrendingDown, AlertTriangle, CheckCircle, MapPin, User, Calendar, Clock, ArrowRight, Truck, Filter, Play, Loader2, Lightbulb } from 'lucide-react';
import { toast } from '@/hooks/use-toast';
import DateRangeFilters from '@/components/date-range-filters';

interface PincodePerformance {
  pincode_area: string;
  total_orders: number;
  delivered_orders: number;
  rts_rto_orders: number;
  rto_percentage: number;
  delivered_cod_value: number;
  rts_rto_cod_loss: number;
}

interface DropshipperSummary {
  dropshipper_email: string;
  total_orders: number;
  delivered_orders: number;
  rts_rto_orders: number;
  rto_percentage: number;
  total_cod_received: number;
  estimated_product_cost: number;
  estimated_shipping_cost: number;
  estimated_payout: number;
  payout_status: 'POSITIVE' | 'NEGATIVE';
}

export default function AdvancedAnalytics() {
  const [selectedDropshipper, setSelectedDropshipper] = useState<string>('all');
  
  // Dual range date filters like payout section
  const [orderDateFrom, setOrderDateFrom] = useState("2025-07-29");
  const [orderDateTo, setOrderDateTo] = useState("2025-08-12");
  const [deliveredDateFrom, setDeliveredDateFrom] = useState("2025-07-24");
  const [deliveredDateTo, setDeliveredDateTo] = useState("2025-08-09");
  
  // Applied filters state
  const [appliedOrderDateFrom, setAppliedOrderDateFrom] = useState("2025-07-29");
  const [appliedOrderDateTo, setAppliedOrderDateTo] = useState("2025-08-12");
  const [appliedDeliveredDateFrom, setAppliedDeliveredDateFrom] = useState("2025-07-24");
  const [appliedDeliveredDateTo, setAppliedDeliveredDateTo] = useState("2025-08-09");
  const [appliedDropshipper, setAppliedDropshipper] = useState<string>('all');
  const [isApplyingFilters, setIsApplyingFilters] = useState(false);
  
  const exportRef = useRef<HTMLButtonElement>(null);

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
    setIsApplyingFilters(true);
    
    // Apply all current filter values
    setAppliedOrderDateFrom(orderDateFrom);
    setAppliedOrderDateTo(orderDateTo);
    setAppliedDeliveredDateFrom(deliveredDateFrom);
    setAppliedDeliveredDateTo(deliveredDateTo);
    setAppliedDropshipper(selectedDropshipper);
    
    // Small delay to show loading state
    setTimeout(() => {
      setIsApplyingFilters(false);
      toast({
        title: '✅ Filters Applied',
        description: 'Analytics updated with new date ranges',
      });
    }, 500);
  };

  // Fetch dropshippers list
  const { data: dropshippers = [] } = useQuery<string[]>({
    queryKey: ['/api/dropshippers'],
  });

  // Fetch pincode performance
  const { data: pincodeData = [], isLoading: pincodeLoading, refetch: refetchPincode } = useQuery<PincodePerformance[]>({
    queryKey: ['/api/analytics/pincode-performance', selectedDropshipper],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (selectedDropshipper && selectedDropshipper !== 'all') {
        params.append('dropshipperEmail', selectedDropshipper);
      }
      
      const response = await fetch(`/api/analytics/pincode-performance?${params}`);
      if (!response.ok) throw new Error('Failed to fetch pincode data');
      return response.json();
    }
  });

  // Fetch dropshipper summary with applied date filters
  const { data: dropshipperData = [], isLoading: dropshipperLoading, refetch: refetchDropshipper } = useQuery<DropshipperSummary[]>({
    queryKey: ['/api/analytics/dropshipper-summary', appliedDeliveredDateFrom, appliedDeliveredDateTo],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (appliedDeliveredDateFrom) params.append('dateFrom', appliedDeliveredDateFrom);
      if (appliedDeliveredDateTo) params.append('dateTo', appliedDeliveredDateTo);
      
      const response = await fetch(`/api/analytics/dropshipper-summary?${params}`);
      if (!response.ok) throw new Error('Failed to fetch dropshipper data');
      return response.json();
    }
  });

  const handleExport = async () => {
    try {
      const response = await fetch('/api/analytics/export', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dateFrom: appliedDeliveredDateFrom,
          dateTo: appliedDeliveredDateTo,
          includeProducts: true,
          includeShipping: true
        })
      });

      if (!response.ok) throw new Error('Export failed');
      
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `advanced_analytics_${appliedDeliveredDateFrom}_${appliedDeliveredDateTo}.xlsx`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
      
      toast({
        title: '✅ Export Successful',
        description: 'Analytics data exported successfully',
      });
    } catch (error) {
      toast({
        title: '❌ Export Failed',
        description: 'Failed to export analytics data',
        variant: 'destructive',
      });
    }
  };

  const refreshData = () => {
    refetchPincode();
    refetchDropshipper();
    toast({
      title: '🔄 Data Refreshed',
      description: 'Analytics data updated successfully',
    });
  };

  // Export specific data sets
  const handleExportHighRTOPincodes = async () => {
    try {
      // Get ALL high RTO pincodes (>70% RTO rate)
      const highRTOPincodes = pincodeData.filter(p => p.rto_percentage > 70);
      
      const csvContent = "data:text/csv;charset=utf-8," + 
        "Pincode,Total Orders,Delivered,RTS/RTO,RTO %,COD Value,COD Loss\n" +
        highRTOPincodes.map(row => 
          `${row.pincode_area},${row.total_orders},${row.delivered_orders},${row.rts_rto_orders},${row.rto_percentage}%,₹${row.delivered_cod_value},₹${row.rts_rto_cod_loss}`
        ).join("\n");
      
      const encodedUri = encodeURI(csvContent);
      const link = document.createElement("a");
      link.setAttribute("href", encodedUri);
      link.setAttribute("download", `high_rto_pincodes_${appliedDeliveredDateFrom}_${appliedDeliveredDateTo}.csv`);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      
      toast({ title: '✅ High RTO Pincodes Exported', description: `${highRTOPincodes.length} high RTO pincodes exported successfully` });
    } catch (error) {
      toast({ title: '❌ Export Failed', description: 'Failed to export high RTO pincodes' });
    }
  };

  const handleExportGoodPincodes = async () => {
    try {
      // Get ALL good delivery pincodes (>60% delivery ratio and minimum 5 orders)
      const goodPincodes = pincodeData.filter(p => (p.delivered_orders / p.total_orders) * 100 > 60 && p.total_orders >= 5);
      
      const csvContent = "data:text/csv;charset=utf-8," + 
        "Pincode,Total Orders,Delivered,RTS/RTO,RTO %,COD Value,COD Loss\n" +
        goodPincodes.map(row => 
          `${row.pincode_area},${row.total_orders},${row.delivered_orders},${row.rts_rto_orders},${row.rto_percentage}%,₹${row.delivered_cod_value},₹${row.rts_rto_cod_loss}`
        ).join("\n");
      
      const encodedUri = encodeURI(csvContent);
      const link = document.createElement("a");
      link.setAttribute("href", encodedUri);
      link.setAttribute("download", `good_delivery_pincodes_${appliedDeliveredDateFrom}_${appliedDeliveredDateTo}.csv`);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      
      toast({ title: '✅ Good Delivery Pincodes Exported', description: `${goodPincodes.length} good delivery pincodes exported successfully` });
    } catch (error) {
      toast({ title: '❌ Export Failed', description: 'Failed to export good delivery pincodes' });
    }
  };

  const handleExportAllAreas = async () => {
    try {
      const csvContent = "data:text/csv;charset=utf-8," + 
        "Area,Total Orders,Delivered,RTS/RTO,RTO %,COD Value,COD Loss\n" +
        pincodeData.map(row => 
          `${row.pincode_area},${row.total_orders},${row.delivered_orders},${row.rts_rto_orders},${row.rto_percentage}%,₹${row.delivered_cod_value},₹${row.rts_rto_cod_loss}`
        ).join("\n");
      
      const encodedUri = encodeURI(csvContent);
      const link = document.createElement("a");
      link.setAttribute("href", encodedUri);
      link.setAttribute("download", `all_areas_analysis_${appliedDeliveredDateFrom}_${appliedDeliveredDateTo}.csv`);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      
      toast({ title: '✅ All Areas Exported', description: 'CSV file downloaded successfully' });
    } catch (error) {
      toast({ title: '❌ Export Failed', description: 'Failed to export all areas' });
    }
  };

  const handleExportPositivePayouts = async () => {
    try {
      const positiveData = dropshipperData.filter(d => d.payout_status === 'POSITIVE');
      const csvContent = "data:text/csv;charset=utf-8," + 
        "Dropshipper,Total Orders,Delivered,RTO %,COD Received,Estimated Payout,Status\n" +
        positiveData.map(row => 
          `${row.dropshipper_email},${row.total_orders},${row.delivered_orders},${row.rto_percentage}%,₹${row.total_cod_received},₹${row.estimated_payout},${row.payout_status}`
        ).join("\n");
      
      const encodedUri = encodeURI(csvContent);
      const link = document.createElement("a");
      link.setAttribute("href", encodedUri);
      link.setAttribute("download", `positive_payouts_${appliedDeliveredDateFrom}_${appliedDeliveredDateTo}.csv`);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      
      toast({ title: '✅ Positive Payouts Exported', description: 'CSV file downloaded successfully' });
    } catch (error) {
      toast({ title: '❌ Export Failed', description: 'Failed to export positive payouts' });
    }
  };

  const handleExportNegativePayouts = async () => {
    try {
      const negativeData = dropshipperData.filter(d => d.payout_status === 'NEGATIVE');
      const csvContent = "data:text/csv;charset=utf-8," + 
        "Dropshipper,Total Orders,Delivered,RTO %,COD Received,Estimated Payout,Status\n" +
        negativeData.map(row => 
          `${row.dropshipper_email},${row.total_orders},${row.delivered_orders},${row.rto_percentage}%,₹${row.total_cod_received},₹${row.estimated_payout},${row.payout_status}`
        ).join("\n");
      
      const encodedUri = encodeURI(csvContent);
      const link = document.createElement("a");
      link.setAttribute("href", encodedUri);
      link.setAttribute("download", `negative_payouts_${appliedDeliveredDateFrom}_${appliedDeliveredDateTo}.csv`);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      
      toast({ title: '✅ Negative Payouts Exported', description: 'CSV file downloaded successfully' });
    } catch (error) {
      toast({ title: '❌ Export Failed', description: 'Failed to export negative payouts' });
    }
  };

  const handleExportAllDropshippers = async () => {
    try {
      const csvContent = "data:text/csv;charset=utf-8," + 
        "Dropshipper,Total Orders,Delivered,RTO %,COD Received,Product Cost,Shipping Cost,Estimated Payout,Status\n" +
        dropshipperData.map(row => 
          `${row.dropshipper_email},${row.total_orders},${row.delivered_orders},${row.rto_percentage}%,₹${row.total_cod_received},₹${row.estimated_product_cost},₹${row.estimated_shipping_cost},₹${row.estimated_payout},${row.payout_status}`
        ).join("\n");
      
      const encodedUri = encodeURI(csvContent);
      const link = document.createElement("a");
      link.setAttribute("href", encodedUri);
      link.setAttribute("download", `all_dropshippers_summary_${appliedDeliveredDateFrom}_${appliedDeliveredDateTo}.csv`);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      
      toast({ title: '✅ All Dropshippers Exported', description: 'CSV file downloaded successfully' });
    } catch (error) {
      toast({ title: '❌ Export Failed', description: 'Failed to export all dropshippers' });
    }
  };

  // Calculate summary stats
  const topPincodes = pincodeData.slice(0, 10);
  const goodPincodes = pincodeData.filter(p => (p.delivered_orders / p.total_orders) * 100 > 60 && p.total_orders >= 5).slice(0, 10);
  const positivePayout = dropshipperData.filter(d => d.payout_status === 'POSITIVE');
  const negativePayout = dropshipperData.filter(d => d.payout_status === 'NEGATIVE');

  const isHighRTO = (percentage: number) => percentage > 70;
  const isGoodDelivery = (deliveredOrders: number, totalOrders: number) => (deliveredOrders / totalOrders) * 100 > 60;

  return (
    <div className="container mx-auto p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900 dark:text-white">
            📊 Advanced Analytics
          </h1>
          <p className="text-gray-600 dark:text-gray-300 mt-2">
            Pincode performance analysis aur dropshipper payout summary
          </p>
        </div>
        <div className="flex gap-3">
          <Button 
            onClick={refreshData} 
            variant="outline"
            data-testid="button-refresh-analytics"
          >
            🔄 Refresh Data
          </Button>
          <Button 
            onClick={handleExport}
            ref={exportRef}
            data-testid="button-export-analytics"
          >
            <Download className="w-4 h-4 mr-2" />
            Export Analytics
          </Button>
        </div>
      </div>

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
        isCalculating={isApplyingFilters}
      />

      <Tabs defaultValue="pincode" className="w-full">
        <TabsList className="grid w-full grid-cols-2">
          <TabsTrigger value="pincode" data-testid="tab-pincode">
            <MapPin className="w-4 h-4 mr-2" />
            Pincode Performance
          </TabsTrigger>
          <TabsTrigger value="dropshipper" data-testid="tab-dropshipper">
            <User className="w-4 h-4 mr-2" />
            Dropshipper Summary
          </TabsTrigger>
        </TabsList>

        <TabsContent value="pincode" className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-sm text-red-600 dark:text-red-400">
                  <AlertTriangle className="w-4 h-4 inline mr-2" />
                  High RTO Pincodes
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-red-600 dark:text-red-400">
                  {pincodeData.filter(p => p.rto_percentage > 70).length}
                </div>
                <p className="text-xs text-gray-500">High RTO Pincodes (&gt;70%)</p>
                <Button 
                  variant="outline" 
                  size="sm" 
                  className="mt-2 w-full text-xs"
                  onClick={() => handleExportHighRTOPincodes()}
                  data-testid="export-high-rto-pincodes"
                >
                  <Download className="w-3 h-3 mr-1" />
                  Export List
                </Button>
              </CardContent>
            </Card>
            
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-sm text-green-600 dark:text-green-400">
                  <CheckCircle className="w-4 h-4 inline mr-2" />
                  Good Delivery Pincodes
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-green-600 dark:text-green-400">
                  {pincodeData.filter(p => (p.delivered_orders / p.total_orders) * 100 > 60 && p.total_orders >= 5).length}
                </div>
                <p className="text-xs text-gray-500">Good Delivery Pincodes (&gt;60% delivery)</p>
                <Button 
                  variant="outline" 
                  size="sm" 
                  className="mt-2 w-full text-xs"
                  onClick={() => handleExportGoodPincodes()}
                  data-testid="export-good-pincodes"
                >
                  <Download className="w-3 h-3 mr-1" />
                  Export List
                </Button>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-sm">Total Analysis</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">
                  {pincodeData.length}
                </div>
                <p className="text-xs text-gray-500">Areas Analyzed</p>
                <Button 
                  variant="outline" 
                  size="sm" 
                  className="mt-2 w-full text-xs"
                  onClick={() => handleExportAllAreas()}
                  data-testid="export-all-areas"
                >
                  <Download className="w-3 h-3 mr-1" />
                  Export All
                </Button>
              </CardContent>
            </Card>
          </div>

          {pincodeLoading ? (
            <Card>
              <CardContent className="p-8 text-center">
                <div className="animate-spin w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full mx-auto"></div>
                <p className="mt-4 text-gray-600">Loading pincode analysis...</p>
              </CardContent>
            </Card>
          ) : (
            <Card>
              <CardHeader>
                <CardTitle>Pincode Performance Analysis</CardTitle>
                <CardDescription>
                  {selectedDropshipper ? `Analysis for ${selectedDropshipper}` : 'All dropshippers analysis'}
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full border-collapse">
                    <thead>
                      <tr className="border-b">
                        <th className="text-left p-3">Pincode</th>
                        <th className="text-right p-3">Total Orders</th>
                        <th className="text-right p-3">Delivered</th>
                        <th className="text-right p-3">RTS/RTO</th>
                        <th className="text-right p-3">RTO %</th>
                        <th className="text-right p-3">COD Value (₹)</th>
                        <th className="text-right p-3">COD Loss (₹)</th>
                        <th className="text-center p-3">Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {pincodeData.map((pincode, index) => (
                        <tr key={pincode.pincode_area} className="border-b hover:bg-gray-50 dark:hover:bg-gray-800">
                          <td className="p-3 font-medium">{pincode.pincode_area}</td>
                          <td className="p-3 text-right">{pincode.total_orders}</td>
                          <td className="p-3 text-right text-green-600">{pincode.delivered_orders}</td>
                          <td className="p-3 text-right text-red-600">{pincode.rts_rto_orders}</td>
                          <td className="p-3 text-right font-bold">
                            <span className={isHighRTO(pincode.rto_percentage) ? 'text-red-600' : isGoodDelivery(pincode.delivered_orders, pincode.total_orders) ? 'text-green-600' : 'text-yellow-600'}>
                              {pincode.rto_percentage}%
                            </span>
                          </td>
                          <td className="p-3 text-right">₹{pincode.delivered_cod_value.toLocaleString('en-IN')}</td>
                          <td className="p-3 text-right text-red-600">₹{pincode.rts_rto_cod_loss.toLocaleString('en-IN')}</td>
                          <td className="p-3 text-center">
                            {isHighRTO(pincode.rto_percentage) ? (
                              <Badge variant="destructive">High RTO (&gt;70%)</Badge>
                            ) : isGoodDelivery(pincode.delivered_orders, pincode.total_orders) ? (
                              <Badge variant="default" className="bg-green-500">Good Delivery (&gt;60%)</Badge>
                            ) : (
                              <Badge variant="secondary">Average</Badge>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          )}
        </TabsContent>

        <TabsContent value="dropshipper" className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-sm text-green-600 dark:text-green-400">
                  <TrendingUp className="w-4 h-4 inline mr-2" />
                  Positive Payout
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-green-600 dark:text-green-400">
                  {positivePayout.length}
                </div>
                <p className="text-xs text-gray-500">Dropshippers</p>
                <Button 
                  variant="outline" 
                  size="sm" 
                  className="mt-2 w-full text-xs"
                  onClick={() => handleExportPositivePayouts()}
                  data-testid="export-positive-payouts"
                >
                  <Download className="w-3 h-3 mr-1" />
                  Export List
                </Button>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-sm text-red-600 dark:text-red-400">
                  <TrendingDown className="w-4 h-4 inline mr-2" />
                  Negative Payout
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-red-600 dark:text-red-400">
                  {negativePayout.length}
                </div>
                <p className="text-xs text-gray-500">Dropshippers</p>
                <Button 
                  variant="outline" 
                  size="sm" 
                  className="mt-2 w-full text-xs"
                  onClick={() => handleExportNegativePayouts()}
                  data-testid="export-negative-payouts"
                >
                  <Download className="w-3 h-3 mr-1" />
                  Export List
                </Button>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-sm">Total COD</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">
                  ₹{dropshipperData.reduce((sum, d) => sum + d.total_cod_received, 0).toLocaleString('en-IN')}
                </div>
                <p className="text-xs text-gray-500">Revenue</p>
                <Button 
                  variant="outline" 
                  size="sm" 
                  className="mt-2 w-full text-xs"
                  onClick={() => handleExportAllDropshippers()}
                  data-testid="export-all-dropshippers"
                >
                  <Download className="w-3 h-3 mr-1" />
                  Export All
                </Button>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-sm">Net Payout</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">
                  ₹{dropshipperData.reduce((sum, d) => sum + d.estimated_payout, 0).toLocaleString('en-IN')}
                </div>
                <p className="text-xs text-gray-500">Total</p>
              </CardContent>
            </Card>
          </div>

          {dropshipperLoading ? (
            <Card>
              <CardContent className="p-8 text-center">
                <div className="animate-spin w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full mx-auto"></div>
                <p className="mt-4 text-gray-600">Loading dropshipper summary...</p>
              </CardContent>
            </Card>
          ) : (
            <Card>
              <CardHeader>
                <CardTitle>Dropshipper Payout Summary</CardTitle>
                <CardDescription>
                  Date range: {appliedDeliveredDateFrom} to {appliedDeliveredDateTo}
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full border-collapse">
                    <thead>
                      <tr className="border-b">
                        <th className="text-left p-3">Dropshipper Email</th>
                        <th className="text-right p-3">Orders</th>
                        <th className="text-right p-3">Delivered</th>
                        <th className="text-right p-3">RTO %</th>
                        <th className="text-right p-3">COD Revenue (₹)</th>
                        <th className="text-right p-3">Product Cost (₹)</th>
                        <th className="text-right p-3">Shipping Cost (₹)</th>
                        <th className="text-right p-3">Net Payout (₹)</th>
                        <th className="text-center p-3">Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {dropshipperData.map((dropshipper) => (
                        <tr key={dropshipper.dropshipper_email} className="border-b hover:bg-gray-50 dark:hover:bg-gray-800">
                          <td className="p-3 font-medium text-sm">{dropshipper.dropshipper_email}</td>
                          <td className="p-3 text-right">{dropshipper.total_orders}</td>
                          <td className="p-3 text-right text-green-600">{dropshipper.delivered_orders}</td>
                          <td className="p-3 text-right">
                            <span className={dropshipper.rto_percentage > 30 ? 'text-red-600' : 'text-green-600'}>
                              {dropshipper.rto_percentage}%
                            </span>
                          </td>
                          <td className="p-3 text-right">₹{dropshipper.total_cod_received.toLocaleString('en-IN')}</td>
                          <td className="p-3 text-right text-red-600">₹{dropshipper.estimated_product_cost.toLocaleString('en-IN')}</td>
                          <td className="p-3 text-right text-red-600">₹{dropshipper.estimated_shipping_cost.toLocaleString('en-IN')}</td>
                          <td className="p-3 text-right font-bold">
                            <span className={dropshipper.payout_status === 'POSITIVE' ? 'text-green-600' : 'text-red-600'}>
                              ₹{dropshipper.estimated_payout.toLocaleString('en-IN')}
                            </span>
                          </td>
                          <td className="p-3 text-center">
                            {dropshipper.payout_status === 'POSITIVE' ? (
                              <Badge variant="default" className="bg-green-500">POSITIVE</Badge>
                            ) : (
                              <Badge variant="destructive">NEGATIVE</Badge>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}