import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import { Progress } from '@/components/ui/progress';
import { 
  BarChart, 
  Bar, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell
} from 'recharts';
import { 
  Package, 
  Users, 
  TrendingUp, 
  TrendingDown, 
  MapPin, 
  Truck,
  FileText,
  CheckCircle,
  XCircle,
  AlertTriangle
} from 'lucide-react';

interface AnalyticsSummary {
  totalUploads: number;
  totalOrders: number;
  cancelledOrders: number;
  deliveredOrders: number;
  rtsOrders: number;
  totalDropshippers: number;
  activeDropshippers: number;
  dateRange: {
    earliest: string | null;
    latest: string | null;
  };
  statusBreakdown: Array<{
    status: string;
    count: number;
  }>;
  recentActivity: {
    lastUpload: string | null;
    ordersThisMonth: number;
  };
}

interface DropshipperAnalytics {
  email: string;
  totalOrders: number;
  deliveredOrders: number;
  rtsOrders: number;
  cancelledOrders: number;
  deliveryRate: number;
  rtsRate: number;
  topProducts: Array<{
    productName: string;
    orderCount: number;
    deliveryRate: number;
  }>;
  pincodeAnalysis: Array<{
    pincode: string;
    orderCount: number;
    deliveryRate: number;
    status: 'good' | 'average' | 'poor';
  }>;
  monthlyTrend: Array<{
    month: string;
    delivered: number;
    rts: number;
    cancelled: number;
  }>;
}

export default function AnalyticsDashboard() {
  const [selectedDropshipper, setSelectedDropshipper] = useState<string>('all');

  // Fetch overall analytics summary
  const summaryQuery = useQuery({
    queryKey: ['/api/analytics/summary'],
    queryFn: async () => {
      const response = await fetch('/api/analytics/summary');
      if (!response.ok) throw new Error('Failed to fetch analytics summary');
      return response.json() as Promise<AnalyticsSummary>;
    },
    staleTime: 2 * 60 * 1000, // 2 minutes
  });

  // Fetch dropshippers list
  const dropshippersQuery = useQuery({
    queryKey: ['/api/dropshippers'],
    staleTime: 5 * 60 * 1000,
  });

  // Fetch specific dropshipper analytics
  const dropshipperAnalyticsQuery = useQuery({
    queryKey: ['/api/analytics/dropshipper', selectedDropshipper],
    queryFn: async () => {
      if (selectedDropshipper === 'all') return null;
      const response = await fetch(`/api/analytics/dropshipper/${encodeURIComponent(selectedDropshipper)}`);
      if (!response.ok) throw new Error('Failed to fetch dropshipper analytics');
      return response.json() as Promise<DropshipperAnalytics>;
    },
    enabled: selectedDropshipper !== 'all',
    staleTime: 2 * 60 * 1000,
  });

  const isLoading = summaryQuery.isLoading || dropshippersQuery.isLoading;
  const data = summaryQuery.data;
  const dropshipperData = dropshipperAnalyticsQuery.data;

  const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6'];

  if (isLoading) {
    return (
      <div className="p-6 space-y-6 animate-pulse">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {[1, 2, 3, 4].map(i => (
            <div key={i} className="h-32 bg-gray-200 rounded-lg"></div>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Analytics Dashboard</h1>
          <p className="text-gray-600">Complete overview of your dropshipping operations</p>
        </div>
        
        {/* Dropshipper Selector */}
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium">Analyze Dropshipper:</span>
          <Select value={selectedDropshipper} onValueChange={setSelectedDropshipper}>
            <SelectTrigger className="w-64">
              <SelectValue placeholder="Select dropshipper" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Dropshippers</SelectItem>
              {Array.isArray(dropshippersQuery.data) && dropshippersQuery.data.map((email: string) => (
                <SelectItem key={email} value={email}>
                  {email}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>

      {/* Overall Summary Cards */}
      {data && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total Uploads</CardTitle>
              <FileText className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-blue-600">{data.totalUploads}</div>
              <p className="text-xs text-muted-foreground">Excel files processed</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total Orders</CardTitle>
              <Package className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{data.totalOrders}</div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Dropshippers</CardTitle>
              <Users className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-purple-600">{data.totalDropshippers}</div>
              <p className="text-xs text-muted-foreground">
                <span className="text-green-600">{data.activeDropshippers} active</span>
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Delivery Performance</CardTitle>
              <TrendingUp className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="space-y-2">
                <div className="flex justify-between">
                  <span className="text-sm">Delivered:</span>
                  <span className="text-sm font-bold text-green-600">{data.deliveredOrders}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-sm">RTS:</span>
                  <span className="text-sm font-bold text-red-600">{data.rtsOrders}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-sm">Cancelled:</span>
                  <span className="text-sm font-bold text-orange-600">{data.cancelledOrders}</span>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Dropshipper-Specific Analytics */}
      {selectedDropshipper !== 'all' && dropshipperData && (
        <div className="space-y-6">
          <Separator />
          
          <div className="flex items-center gap-2">
            <h2 className="text-2xl font-bold">Individual Analytics</h2>
            <Badge variant="outline" className="text-sm">{selectedDropshipper}</Badge>
          </div>

          {/* Dropshipper Performance Cards */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <Card>
              <CardHeader>
                <CardTitle className="text-lg">Delivery Rate</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div className="text-3xl font-bold text-green-600">
                    {dropshipperData.deliveryRate.toFixed(1)}%
                  </div>
                  <Progress value={dropshipperData.deliveryRate} className="h-2" />
                  <div className="text-sm text-gray-600">
                    {dropshipperData.deliveredOrders} out of {dropshipperData.totalOrders} orders delivered
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="text-lg">RTS Rate</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div className="text-3xl font-bold text-red-600">
                    {dropshipperData.rtsRate.toFixed(1)}%
                  </div>
                  <Progress value={dropshipperData.rtsRate} className="h-2" />
                  <div className="text-sm text-gray-600">
                    {dropshipperData.rtsOrders} RTS orders
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="text-lg">Order Distribution</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  <div className="flex justify-between items-center">
                    <span className="flex items-center gap-2">
                      <CheckCircle className="h-4 w-4 text-green-600" />
                      Delivered
                    </span>
                    <Badge variant="default" className="bg-green-100 text-green-800">
                      {dropshipperData.deliveredOrders}
                    </Badge>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="flex items-center gap-2">
                      <XCircle className="h-4 w-4 text-red-600" />
                      RTS
                    </span>
                    <Badge variant="destructive">
                      {dropshipperData.rtsOrders}
                    </Badge>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="flex items-center gap-2">
                      <AlertTriangle className="h-4 w-4 text-orange-600" />
                      Cancelled
                    </span>
                    <Badge variant="secondary">
                      {dropshipperData.cancelledOrders}
                    </Badge>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Pincode Analysis */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <MapPin className="h-5 w-5" />
                Pincode Performance Analysis
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <div>
                    <h4 className="font-medium text-green-700 mb-2">Good Pincodes (≥80% delivery)</h4>
                    <div className="space-y-2 max-h-40 overflow-y-auto">
                      {dropshipperData.pincodeAnalysis
                        .filter(p => p.status === 'good')
                        .map(pincode => (
                          <div key={pincode.pincode} className="flex justify-between text-sm p-2 bg-green-50 rounded">
                            <span>{pincode.pincode}</span>
                            <span className="text-green-700">{pincode.deliveryRate.toFixed(1)}%</span>
                          </div>
                        ))}
                    </div>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-orange-700 mb-2">Average Pincodes (50-80% delivery)</h4>
                    <div className="space-y-2 max-h-40 overflow-y-auto">
                      {dropshipperData.pincodeAnalysis
                        .filter(p => p.status === 'average')
                        .map(pincode => (
                          <div key={pincode.pincode} className="flex justify-between text-sm p-2 bg-orange-50 rounded">
                            <span>{pincode.pincode}</span>
                            <span className="text-orange-700">{pincode.deliveryRate.toFixed(1)}%</span>
                          </div>
                        ))}
                    </div>
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-red-700 mb-2">Poor Pincodes (&lt;50% delivery)</h4>
                    <div className="space-y-2 max-h-40 overflow-y-auto">
                      {dropshipperData.pincodeAnalysis
                        .filter(p => p.status === 'poor')
                        .map(pincode => (
                          <div key={pincode.pincode} className="flex justify-between text-sm p-2 bg-red-50 rounded">
                            <span>{pincode.pincode}</span>
                            <span className="text-red-700">{pincode.deliveryRate.toFixed(1)}%</span>
                          </div>
                        ))}
                    </div>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Top Products */}
          <Card>
            <CardHeader>
              <CardTitle>Top Products by Orders</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                {dropshipperData.topProducts.slice(0, 5).map((product, index) => (
                  <div key={product.productName} className="flex items-center justify-between p-3 bg-gray-50 rounded">
                    <div className="flex items-center gap-3">
                      <div className="w-6 h-6 rounded-full bg-blue-600 text-white text-xs flex items-center justify-center">
                        {index + 1}
                      </div>
                      <span className="font-medium">{product.productName}</span>
                    </div>
                    <div className="text-right">
                      <div className="font-bold">{product.orderCount} orders</div>
                      <div className="text-sm text-green-600">{product.deliveryRate.toFixed(1)}% delivered</div>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}