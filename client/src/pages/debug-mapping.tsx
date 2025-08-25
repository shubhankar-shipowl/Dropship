import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Link } from "wouter";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Search, Download, Eye, FileText } from "lucide-react";

interface DebugMappingData {
  orderId: string;
  waybill: string | null;
  productName: string;
  productUid: string;
  dropshipperEmail: string;
  codAmountPaise: number;
  codAmountRupees: number;
  productCostPerUnit: number;
  productWeight: number;
  shippingProvider: string;
  shippingRatePerKg: number;
  shippingCostCalculated: number;
  qty: number;
  status: string;
  deliveredDate: string | null;
  orderDate: string;
  mappingStatus: {
    priceFound: boolean;
    rateFound: boolean;
    codValid: boolean;
  };
}

export default function DebugMapping() {
  const [dropshipperEmail, setDropshipperEmail] = useState("thedaazarastore@gmail.com");
  const [orderDateFrom, setOrderDateFrom] = useState("2025-07-24");
  const [orderDateTo, setOrderDateTo] = useState("2025-08-09");
  const [searchTerm, setSearchTerm] = useState("");

  // Get dropshippers list
  const { data: dropshippers } = useQuery<string[]>({
    queryKey: ['/api/dropshippers']
  });

  const { data: debugData, isLoading, refetch } = useQuery<DebugMappingData[]>({
    queryKey: ['/api/debug-mapping'],
    queryFn: async () => {
      const params = new URLSearchParams({
        dropshipperEmail,
        orderDateFrom,
        orderDateTo
      });
      const response = await fetch(`/api/debug-mapping?${params}`);
      if (!response.ok) throw new Error('Failed to fetch debug data');
      return response.json();
    },
    enabled: false
  });

  const handleSearch = () => {
    refetch();
  };

  const filteredData = debugData?.filter(item => 
    searchTerm === "" || 
    item.orderId.toLowerCase().includes(searchTerm.toLowerCase()) ||
    item.productName.toLowerCase().includes(searchTerm.toLowerCase())
  ) || [];

  const totalOrders = filteredData.length;
  const deliveredOrders = filteredData.filter(item => item.status.toLowerCase().includes('delivered')).length;
  const totalCOD = filteredData.reduce((sum, item) => sum + item.codAmountRupees, 0);
  const totalProductCost = filteredData.reduce((sum, item) => sum + (item.productCostPerUnit * item.qty), 0);
  const totalShippingCost = filteredData.reduce((sum, item) => sum + item.shippingCostCalculated, 0);

  return (
    <div className="container mx-auto p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <Link href="/">
            <Button variant="outline" size="sm">← Back to Dashboard</Button>
          </Link>
          <h1 className="text-3xl font-bold">Debug Mapping Analysis</h1>
        </div>
        <Button variant="outline" size="sm">
          <Download className="h-4 w-4 mr-2" />
          Export Debug Data
        </Button>
      </div>

      {/* Search Controls */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Search className="h-5 w-5" />
            Search Parameters
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <Label htmlFor="dropshipper">Dropshipper Email</Label>
              <Select value={dropshipperEmail} onValueChange={setDropshipperEmail}>
                <SelectTrigger>
                  <SelectValue placeholder="Select dropshipper" />
                </SelectTrigger>
                <SelectContent>
                  {dropshippers?.map((email) => (
                    <SelectItem key={email} value={email}>
                      {email}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div>
              <Label htmlFor="dateFrom">From Date</Label>
              <Input
                id="dateFrom"
                type="date"
                value={orderDateFrom}
                onChange={(e) => setOrderDateFrom(e.target.value)}
              />
            </div>
            <div>
              <Label htmlFor="dateTo">To Date</Label>
              <Input
                id="dateTo"
                type="date"
                value={orderDateTo}
                onChange={(e) => setOrderDateTo(e.target.value)}
              />
            </div>
          </div>
          <div className="flex gap-4">
            <Button onClick={handleSearch} disabled={isLoading} data-testid="button-search">
              <Eye className="h-4 w-4 mr-2" />
              {isLoading ? "Loading..." : "Analyze Mapping"}
            </Button>
            <Input
              placeholder="Search orders or products..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="max-w-sm"
            />
          </div>
        </CardContent>
      </Card>

      {/* Summary Cards */}
      {debugData && (
        <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
          <Card>
            <CardContent className="p-4">
              <div className="text-2xl font-bold">{totalOrders}</div>
              <div className="text-sm text-muted-foreground">Total Orders</div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="text-2xl font-bold text-green-600">{deliveredOrders}</div>
              <div className="text-sm text-muted-foreground">Delivered</div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="text-2xl font-bold">₹{totalCOD.toLocaleString('en-IN')}</div>
              <div className="text-sm text-muted-foreground">Total COD</div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="text-2xl font-bold">₹{totalProductCost.toLocaleString('en-IN')}</div>
              <div className="text-sm text-muted-foreground">Product Cost</div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="text-2xl font-bold">₹{totalShippingCost.toLocaleString('en-IN')}</div>
              <div className="text-sm text-muted-foreground">Shipping Cost</div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Detailed Mapping Table */}
      {debugData && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <FileText className="h-5 w-5" />
              Detailed Mapping Analysis ({filteredData.length} orders)
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b">
                    <th className="text-left p-2">Order ID</th>
                    <th className="text-left p-2">Product</th>
                    <th className="text-left p-2">COD Amount</th>
                    <th className="text-left p-2">Product Cost</th>
                    <th className="text-left p-2">Weight</th>
                    <th className="text-left p-2">Shipping</th>
                    <th className="text-left p-2">Status</th>
                    <th className="text-left p-2">Mapping</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredData.map((item, index) => (
                    <tr key={index} className="border-b hover:bg-muted/50">
                      <td className="p-2 font-mono">{item.orderId}</td>
                      <td className="p-2 max-w-[200px] truncate" title={item.productName}>
                        {item.productName}
                      </td>
                      <td className="p-2">
                        <div className="space-y-1">
                          <div className="font-medium">₹{item.codAmountRupees}</div>
                          <div className="text-xs text-muted-foreground">
                            ({item.codAmountPaise} paise)
                          </div>
                        </div>
                      </td>
                      <td className="p-2">
                        <div className="space-y-1">
                          <div className="font-medium">₹{item.productCostPerUnit}</div>
                          <div className="text-xs text-muted-foreground">
                            × {item.qty} = ₹{item.productCostPerUnit * item.qty}
                          </div>
                        </div>
                      </td>
                      <td className="p-2">{item.productWeight}kg</td>
                      <td className="p-2">
                        <div className="space-y-1">
                          <div className="text-xs text-muted-foreground">{item.shippingProvider}</div>
                          <div className="font-medium">₹{item.shippingRatePerKg}/kg</div>
                          <div className="text-xs">= ₹{item.shippingCostCalculated}</div>
                        </div>
                      </td>
                      <td className="p-2">
                        <Badge variant={item.status.toLowerCase().includes('delivered') ? 'default' : 'secondary'}>
                          {item.status}
                        </Badge>
                      </td>
                      <td className="p-2">
                        <div className="space-y-1">
                          <div className="flex gap-1">
                            <Badge variant={item.mappingStatus.priceFound ? 'default' : 'destructive'} className="text-xs">
                              {item.mappingStatus.priceFound ? '✓ Price' : '✗ Price'}
                            </Badge>
                          </div>
                          <div className="flex gap-1">
                            <Badge variant={item.mappingStatus.rateFound ? 'default' : 'destructive'} className="text-xs">
                              {item.mappingStatus.rateFound ? '✓ Rate' : '✗ Rate'}
                            </Badge>
                          </div>
                          <div className="flex gap-1">
                            <Badge variant={item.mappingStatus.codValid ? 'default' : 'destructive'} className="text-xs">
                              {item.mappingStatus.codValid ? '✓ COD' : '✗ COD'}
                            </Badge>
                          </div>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}

      {!debugData && !isLoading && (
        <Card>
          <CardContent className="p-8 text-center">
            <p className="text-muted-foreground">Click "Analyze Mapping" to see detailed mapping information</p>
          </CardContent>
        </Card>
      )}
    </div>
  );
}