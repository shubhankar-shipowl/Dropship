import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { 
  Calculator, 
  Package, 
  Truck, 
  Weight,
  DollarSign,
  BarChart3,
  Info,
  AlertCircle,
  CheckCircle
} from 'lucide-react';

interface ShippingBreakdownData {
  orderId: string;
  dropshipperEmail: string;
  productName: string;
  productUid: string;
  qty: number;
  productWeight: number;
  shippingProvider: string;
  orderDate: string;
  status: string;
  
  // Calculation details
  shippingRatePerKg: number;
  totalWeight: number;
  shippingCost: number;
  rateSource: 'exact' | 'fallback' | 'default';
  rateKey: string;
  
  // COD details
  codAmount: number;
  codPerUnit: number;
  productCostPerUnit: number;
  
  // Final calculation
  netAmount: number;
}

interface ShippingCostSummary {
  totalOrders: number;
  totalShippingCost: number;
  totalCODAmount: number;
  totalProductCost: number;
  netPayout: number;
  
  byProvider: Array<{
    provider: string;
    orderCount: number;
    totalCost: number;
    avgRatePerKg: number;
  }>;
  
  byDropshipper: Array<{
    dropshipper: string;
    orderCount: number;
    shippingCost: number;
    codAmount: number;
    netPayout: number;
  }>;
  
  rateSourceBreakdown: {
    exact: number;
    fallback: number;
    default: number;
  };
}

export default function ShippingCostBreakdown() {
  const [selectedDropshipper, setSelectedDropshipper] = useState<string>('all');
  const [showDetails, setShowDetails] = useState(false);

  // Fetch shipping breakdown data
  const breakdownQuery = useQuery({
    queryKey: ['/api/shipping-breakdown', selectedDropshipper],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (selectedDropshipper !== 'all') {
        params.append('dropshipperEmail', selectedDropshipper);
      }
      
      const response = await fetch(`/api/shipping-breakdown?${params.toString()}`);
      if (!response.ok) throw new Error('Failed to fetch shipping breakdown');
      
      return response.json() as Promise<{
        orders: ShippingBreakdownData[];
        summary: ShippingCostSummary;
      }>;
    },
    staleTime: 2 * 60 * 1000,
  });

  // Fetch dropshippers
  const dropshippersQuery = useQuery({
    queryKey: ['/api/dropshippers'],
    staleTime: 5 * 60 * 1000,
  });

  const data = breakdownQuery.data;
  const isLoading = breakdownQuery.isLoading;

  if (isLoading) {
    return (
      <div className="p-6 space-y-6 animate-pulse">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {[1, 2, 3].map(i => (
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
          <h1 className="text-3xl font-bold text-gray-900 flex items-center gap-3">
            <Calculator className="h-8 w-8 text-blue-600" />
            Shipping Cost Breakdown
          </h1>
          <p className="text-gray-600 mt-2">Detailed analysis of shipping cost calculations</p>
        </div>
        
        {/* Dropshipper Selector */}
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium">Filter by Dropshipper:</span>
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

      {/* Summary Cards */}
      {data && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total Orders</CardTitle>
              <Package className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{data.summary.totalOrders}</div>
              <p className="text-xs text-muted-foreground">Orders with shipping charges</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total Shipping Cost</CardTitle>
              <Truck className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-red-600">₹{data.summary.totalShippingCost.toLocaleString()}</div>
              <p className="text-xs text-muted-foreground">Flat rate calculations</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total COD Amount</CardTitle>
              <DollarSign className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-green-600">₹{data.summary.totalCODAmount.toLocaleString()}</div>
              <p className="text-xs text-muted-foreground">Customer payments</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Net Payout</CardTitle>
              <BarChart3 className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-blue-600">₹{data.summary.netPayout.toLocaleString()}</div>
              <p className="text-xs text-muted-foreground">After shipping deduction</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Calculation Formula */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Info className="h-5 w-5 text-blue-600" />
            Shipping Cost Calculation Formula
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="bg-blue-50 p-4 rounded-lg border border-blue-200">
            <h4 className="font-semibold text-blue-800 mb-2">Step-by-Step Calculation:</h4>
            <div className="space-y-2 text-sm">
              <div className="flex items-center gap-2">
                <span className="w-6 h-6 bg-blue-600 text-white rounded-full flex items-center justify-center text-xs">1</span>
                <span><strong>Flat Rate</strong> = Get fixed shipping rate from rates table</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="w-6 h-6 bg-blue-600 text-white rounded-full flex items-center justify-center text-xs">2</span>
                <span><strong>Shipping Cost</strong> = Quantity × Flat Rate (NOT per kg)</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="w-6 h-6 bg-blue-600 text-white rounded-full flex items-center justify-center text-xs">3</span>
                <span><strong>Net Payout</strong> = COD Amount - Product Cost - Shipping Cost</span>
              </div>
              <div className="bg-yellow-100 p-3 rounded border-l-4 border-yellow-500 mt-3">
                <strong>📋 Example:</strong> If you set ₹75 for 0.5kg product, system uses ₹75 flat. If you set ₹100 for 1kg product, system uses ₹100 flat.
              </div>
            </div>
          </div>

          {/* Rate Matching Logic */}
          <div className="bg-yellow-50 p-4 rounded-lg border border-yellow-200">
            <h4 className="font-semibold text-yellow-800 mb-2">Rate Matching Priority:</h4>
            <div className="space-y-2 text-sm">
              <div className="flex items-center gap-2">
                <CheckCircle className="h-4 w-4 text-green-600" />
                <span><strong>Exact Match:</strong> Dropshipper + Product Weight + Shipping Provider</span>
              </div>
              <div className="flex items-center gap-2">
                <AlertCircle className="h-4 w-4 text-orange-600" />
                <span><strong>Fallback:</strong> Dropshipper + Any Weight + Shipping Provider</span>
              </div>
              <div className="flex items-center gap-2">
                <AlertCircle className="h-4 w-4 text-red-600" />
                <span><strong>Default:</strong> Standard rates (Delhivery: ₹25/kg, Bluedart: ₹30/kg)</span>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Rate Source Breakdown */}
      {data && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <Card>
            <CardHeader>
              <CardTitle>Shipping Providers Breakdown</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                {data.summary.byProvider.map((provider, index) => (
                  <div key={provider.provider} className="flex items-center justify-between p-3 bg-gray-50 rounded">
                    <div className="flex items-center gap-3">
                      <div className="w-6 h-6 rounded-full bg-blue-600 text-white text-xs flex items-center justify-center">
                        {index + 1}
                      </div>
                      <span className="font-medium">{provider.provider}</span>
                    </div>
                    <div className="text-right">
                      <div className="font-bold">₹{provider.totalCost.toLocaleString()}</div>
                      <div className="text-sm text-gray-600">
                        {provider.orderCount} orders • ₹{provider.avgRatePerKg} flat avg
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Rate Source Analysis</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                <div className="flex justify-between items-center p-3 bg-green-50 rounded">
                  <div className="flex items-center gap-2">
                    <CheckCircle className="h-4 w-4 text-green-600" />
                    <span>Exact Match</span>
                  </div>
                  <Badge variant="default" className="bg-green-600">
                    {data.summary.rateSourceBreakdown.exact} orders
                  </Badge>
                </div>
                <div className="flex justify-between items-center p-3 bg-orange-50 rounded">
                  <div className="flex items-center gap-2">
                    <AlertCircle className="h-4 w-4 text-orange-600" />
                    <span>Fallback Rate</span>
                  </div>
                  <Badge variant="default" className="bg-orange-600">
                    {data.summary.rateSourceBreakdown.fallback} orders
                  </Badge>
                </div>
                <div className="flex justify-between items-center p-3 bg-red-50 rounded">
                  <div className="flex items-center gap-2">
                    <AlertCircle className="h-4 w-4 text-red-600" />
                    <span>Default Rate</span>
                  </div>
                  <Badge variant="destructive">
                    {data.summary.rateSourceBreakdown.default} orders
                  </Badge>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Toggle Details Button */}
      <div className="flex justify-center">
        <Button
          onClick={() => setShowDetails(!showDetails)}
          variant="outline"
          size="lg"
          className="gap-2"
        >
          <Package className="h-4 w-4" />
          {showDetails ? 'Hide' : 'Show'} Order-wise Details
        </Button>
      </div>

      {/* Detailed Order Breakdown */}
      {showDetails && data && (
        <Card>
          <CardHeader>
            <CardTitle>Order-wise Shipping Cost Details</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Order ID</TableHead>
                    <TableHead>Product</TableHead>
                    <TableHead>Qty</TableHead>
                    <TableHead>Weight (kg)</TableHead>
                    <TableHead>Provider</TableHead>
                    <TableHead>Flat Rate</TableHead>
                    <TableHead>Source</TableHead>
                    <TableHead>Shipping Cost</TableHead>
                    <TableHead>COD Amount</TableHead>
                    <TableHead>Net Amount</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.orders.slice(0, 100).map((order) => (
                    <TableRow key={`${order.orderId}-${order.productUid}`}>
                      <TableCell className="font-medium">{order.orderId}</TableCell>
                      <TableCell>{order.productName}</TableCell>
                      <TableCell className="text-center">{order.qty}</TableCell>
                      <TableCell className="text-center">{order.totalWeight}</TableCell>
                      <TableCell>{order.shippingProvider}</TableCell>
                      <TableCell className="text-center">₹{order.shippingRatePerKg}</TableCell>
                      <TableCell>
                        <Badge 
                          variant={
                            order.rateSource === 'exact' ? 'default' :
                            order.rateSource === 'fallback' ? 'secondary' : 'destructive'
                          }
                          className={
                            order.rateSource === 'exact' ? 'bg-green-600' :
                            order.rateSource === 'fallback' ? 'bg-orange-600' : ''
                          }
                        >
                          {order.rateSource}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-right font-bold text-red-600">
                        ₹{order.shippingCost.toLocaleString()}
                      </TableCell>
                      <TableCell className="text-right text-green-600">
                        ₹{order.codAmount.toLocaleString()}
                      </TableCell>
                      <TableCell className="text-right font-bold text-blue-600">
                        ₹{order.netAmount.toLocaleString()}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
            {data.orders.length > 100 && (
              <div className="text-center mt-4 text-gray-600">
                Showing first 100 orders. Total: {data.orders.length} orders
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}