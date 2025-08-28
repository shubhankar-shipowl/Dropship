import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Checkbox } from '@/components/ui/checkbox';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Label } from '@/components/ui/label';
import { Trash2, Download, Eye, EyeOff, Database, Search, Edit, Plus, RefreshCw, Save, X, ChevronLeft, ChevronRight } from 'lucide-react';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { apiRequest, queryClient } from '@/lib/queryClient';
import SettingsUpload from '@/components/settings-upload';

export default function DatabaseTransparency() {
  const [activeTab, setActiveTab] = useState('config-summary');
  const [showSensitiveData, setShowSensitiveData] = useState(false);
  
  // Filters for data
  const [selectedDropshipper, setSelectedDropshipper] = useState('all');
  const [orderDateFrom, setOrderDateFrom] = useState('');
  const [orderDateTo, setOrderDateTo] = useState('');
  const [deliveredDateFrom, setDeliveredDateFrom] = useState('');
  const [deliveredDateTo, setDeliveredDateTo] = useState('');
  
  // Table controls
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedItems, setSelectedItems] = useState<Set<string>>(new Set());
  const [selectAll, setSelectAll] = useState(false);
  
  // Quick edit dialog
  const [editDialog, setEditDialog] = useState<{
    open: boolean;
    config: any;
    formData: {
      productWeight: string;
      productCost: string;
      shippingRate: string;
    };
  }>({
    open: false,
    config: null,
    formData: { productWeight: '', productCost: '', shippingRate: '' }
  });

  // Ultra-fast: Only load orders when viewing orders tab
  const ordersQuery = useQuery({
    queryKey: ['/api/transparency/orders', selectedDropshipper, orderDateFrom, orderDateTo, deliveredDateFrom, deliveredDateTo],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (selectedDropshipper !== 'all') params.append('dropshipperEmail', selectedDropshipper);
      if (orderDateFrom) params.append('orderDateFrom', orderDateFrom);
      if (orderDateTo) params.append('orderDateTo', orderDateTo);
      if (deliveredDateFrom) params.append('deliveredDateFrom', deliveredDateFrom);
      if (deliveredDateTo) params.append('deliveredDateTo', deliveredDateTo);
      
      const response = await fetch(`/api/transparency/orders?${params.toString()}`);
      return await response.json();
    },
    staleTime: 5 * 60 * 1000, // 5 minutes cache
    enabled: activeTab === 'orders',
  });
  
  // Fetch dropshippers list for filter
  const dropsQuery = useQuery({
    queryKey: ['/api/dropshippers'],
    queryFn: () => fetch('/api/dropshippers').then(res => res.json())
  });

  const uploadsQuery = useQuery({
    queryKey: ['/api/transparency/uploads'],
    queryFn: () => fetch('/api/transparency/uploads').then(res => res.json()),
    staleTime: 5 * 60 * 1000,
    enabled: activeTab === 'uploads',
  });

  const productPricesQuery = useQuery({
    queryKey: ['/api/transparency/product-prices', selectedDropshipper],
    queryFn: () => {
      const params = new URLSearchParams();
      if (selectedDropshipper !== 'all') params.append('dropshipperEmail', selectedDropshipper);
      return fetch(`/api/transparency/product-prices?${params.toString()}`).then(res => res.json());
    },
    staleTime: 5 * 60 * 1000,
    enabled: activeTab === 'product-prices',
  });

  const shippingRatesQuery = useQuery({
    queryKey: ['/api/transparency/shipping-rates', selectedDropshipper],
    queryFn: () => {
      const params = new URLSearchParams();
      if (selectedDropshipper !== 'all') params.append('dropshipperEmail', selectedDropshipper);
      return fetch(`/api/transparency/shipping-rates?${params.toString()}`).then(res => res.json());
    },
    staleTime: 5 * 60 * 1000,
    enabled: activeTab === 'shipping-rates',
  });

  // Complete configuration data - load all items
  const configSummaryQuery = useQuery({
    queryKey: ['/api/transparency/config-summary', selectedDropshipper],
    queryFn: async () => {
      try {
        console.log('Loading complete config summary...');
        const params = new URLSearchParams();
        if (selectedDropshipper !== 'all') params.append('dropshipperEmail', selectedDropshipper);
        const response = await fetch(`/api/transparency/config-summary?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        console.log(`Config summary loaded: ${data.length} total items`);
        return data;
      } catch (error) {
        console.error('Error fetching config summary:', error);
        throw error;
      }
    },
    staleTime: 30 * 1000, // 30 seconds cache for better refresh
    enabled: activeTab === 'config-summary',
  });

  // Payout logs query - only when needed
  const payoutLogsQuery = useQuery({
    queryKey: ['/api/transparency/payout-logs'],
    staleTime: 5 * 60 * 1000,
    enabled: activeTab === 'payout-logs',
  });

  // Pagination state
  const [currentPage, setCurrentPage] = useState(1);
  const [itemsPerPage] = useState(100); // Show 100 items per page
  
  // Ultra-fast search with debouncing
  const [debouncedSearchTerm, setDebouncedSearchTerm] = useState('');
  
  React.useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearchTerm(searchTerm);
      setCurrentPage(1); // Reset to first page on search
    }, 300); // 300ms debounce
    
    return () => clearTimeout(timer);
  }, [searchTerm]);

  // Filter data based on search term with pagination
  const filteredData = React.useMemo(() => {
    try {
      const data = configSummaryQuery.data || [];
      if (!debouncedSearchTerm) return data;
      
      const searchLower = debouncedSearchTerm.toLowerCase();
      const filtered = data.filter((config: any) => {
        return (
          config.dropshipperEmail?.toLowerCase().includes(searchLower) ||
          config.productName?.toLowerCase().includes(searchLower) ||
          config.productUid?.toLowerCase().includes(searchLower) ||
          config.sku?.toLowerCase().includes(searchLower) ||
          config.shippingProvider?.toLowerCase().includes(searchLower)
        );
      });
      
      return filtered;
    } catch (error) {
      console.error('Error filtering data:', error);
      return [];
    }
  }, [configSummaryQuery.data, debouncedSearchTerm]);

  // Paginated data
  const paginatedData = React.useMemo(() => {
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    return filteredData.slice(startIndex, endIndex);
  }, [filteredData, currentPage, itemsPerPage]);

  // Pagination info
  const totalPages = Math.ceil(filteredData.length / itemsPerPage);
  const startItem = (currentPage - 1) * itemsPerPage + 1;
  const endItem = Math.min(currentPage * itemsPerPage, filteredData.length);

  // Handle checkbox functions
  const handleSelectAll = (checked: boolean | "indeterminate") => {
    const isChecked = checked === true;
    setSelectAll(isChecked);
    if (isChecked) {
      const allIds = paginatedData.map((config: any, index: number) => 
        `${config.dropshipperEmail}-${config.productUid}-${config.shippingProvider}-${index}`
      );
      setSelectedItems(new Set(allIds));
    } else {
      setSelectedItems(new Set());
    }
  };

  const handleItemSelect = (itemId: string, checked: boolean | "indeterminate") => {
    const newSelected = new Set(selectedItems);
    if (checked === true) {
      newSelected.add(itemId);
    } else {
      newSelected.delete(itemId);
    }
    setSelectedItems(newSelected);
    setSelectAll(newSelected.size === paginatedData.length);
  };

  // Pagination handlers
  const handlePageChange = (newPage: number) => {
    if (newPage >= 1 && newPage <= totalPages) {
      setCurrentPage(newPage);
      setSelectedItems(new Set()); // Clear selections when changing pages
      setSelectAll(false);
    }
  };

  // Action handlers
  const handleBulkUpdate = () => {
    console.log('Bulk update for:', Array.from(selectedItems));
    // TODO: Implement bulk update functionality
    alert(`Bulk update for ${selectedItems.size} items - Feature coming soon!`);
  };

  const handleAddMissing = () => {
    console.log('Add missing data for:', Array.from(selectedItems));
    // TODO: Implement add missing data functionality
    alert(`Add missing data for ${selectedItems.size} items - Feature coming soon!`);
  };

  const handleQuickEdit = (config: any) => {
    console.log('Quick edit for:', config);
    setEditDialog({
      open: true,
      config,
      formData: {
        productWeight: config.productWeight || '',
        productCost: config.productCost || '',
        shippingRate: config.shippingRate || ''
      }
    });
  };

  const handleDownloadTemplate = async () => {
    try {
      console.log('Downloading settings template...');
      
      const response = await fetch('/api/export-settings', {
        method: 'GET',
        headers: {
          'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
      });
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const blob = await response.blob();
      console.log('Template downloaded, blob size:', blob.size);
      
      // Create download link for cross-system compatibility
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'settings_template.xlsx';
      a.style.display = 'none';
      
      // Append to body, click, and remove for PM2/cross-system compatibility
      document.body.appendChild(a);
      a.click();
      
      // Clean up
      setTimeout(() => {
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
      }, 100);
      
      console.log('Template download initiated successfully');
      
    } catch (error) {
      console.error('Error downloading template:', error);
      alert(`Error downloading template: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  };

  const handleSaveEdit = async () => {
    try {
      const { config, formData } = editDialog;
      
      // Update product price if weight or cost changed
      if (formData.productWeight || formData.productCost) {
        await apiRequest('POST', '/api/product-prices', {
          dropshipperEmail: config.dropshipperEmail,
          productUid: config.productUid,
          productName: config.productName,
          sku: config.sku || '',
          productWeight: formData.productWeight || '0.5',
          productCostPerUnit: formData.productCost || '0',
          currency: 'INR'
        });
      }

      // Update shipping rate if changed
      if (formData.shippingRate) {
        await apiRequest('POST', '/api/shipping-rates', {
          productUid: config.productUid,
          productWeight: formData.productWeight || '0.5',
          shippingProvider: config.shippingProvider,
          shippingRatePerKg: formData.shippingRate || '0',
          currency: 'INR'
        });
      }

      // Refresh data - invalidate all related queries
      await queryClient.invalidateQueries({ queryKey: ['/api/transparency/config-summary'] });
      await queryClient.invalidateQueries({ queryKey: ['/api/transparency/product-prices'] });
      await queryClient.invalidateQueries({ queryKey: ['/api/transparency/shipping-rates'] });
      await queryClient.invalidateQueries({ queryKey: ['/api/missing-data'] });
      
      setEditDialog({ open: false, config: null, formData: { productWeight: '', productCost: '', shippingRate: '' } });
      
      // Show success message
      alert('Changes saved successfully!');
      
    } catch (error) {
      console.error('Error saving edit:', error);
      alert('Error saving changes. Please try again.');
    }
  };

  // Update select all when filtered data changes
  React.useEffect(() => {
    try {
      if (selectedItems.size > 0 && paginatedData.length > 0) {
        setSelectAll(selectedItems.size === paginatedData.length);
      } else {
        setSelectAll(false);
      }
    } catch (error) {
      console.error('Error in useEffect:', error);
    }
  }, [paginatedData.length, selectedItems.size]);

  // Reset page when search changes
  React.useEffect(() => {
    setCurrentPage(1);
    setSelectedItems(new Set());
    setSelectAll(false);
  }, [debouncedSearchTerm]);



  const handleClearTable = async (tableName: string) => {
    if (!confirm(`Are you sure you want to clear all data from ${tableName}? This cannot be undone.`)) {
      return;
    }
    
    try {
      await fetch(`/api/transparency/clear/${tableName}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });
      
      // Refetch all queries
      ordersQuery.refetch();
      uploadsQuery.refetch();
      productPricesQuery.refetch();
      shippingRatesQuery.refetch();
      payoutLogsQuery.refetch();
    } catch (error) {
      console.error('Error clearing table:', error);
    }
  };

  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return 'N/A';
    return new Date(dateStr).toLocaleString();
  };

  const formatCurrency = (amount: string | number) => {
    return `₹${Math.round(parseFloat(amount?.toString() || '0'))}`;
  };

  const orders = Array.isArray(ordersQuery.data) ? ordersQuery.data : [];
  const uploads = Array.isArray(uploadsQuery.data) ? uploadsQuery.data : [];
  const productPrices = Array.isArray(productPricesQuery.data) ? productPricesQuery.data : [];
  const shippingRates = Array.isArray(shippingRatesQuery.data) ? shippingRatesQuery.data : [];
  const payoutLogs = Array.isArray(payoutLogsQuery.data) ? payoutLogsQuery.data : [];

  console.log('Current orders count:', orders.length);
  console.log('Current uploads count:', uploads.length);

  return (
    <div className="container mx-auto p-6">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-3xl font-bold">Database Transparency</h1>
          <p className="text-muted-foreground mt-2">
            Complete visibility into all stored data in the system
          </p>
        </div>
        
        <div className="flex gap-2">
          <Button
            variant={showSensitiveData ? "destructive" : "outline"}
            onClick={() => setShowSensitiveData(!showSensitiveData)}
            size="sm"
          >
            {showSensitiveData ? <EyeOff className="w-4 h-4 mr-2" /> : <Eye className="w-4 h-4 mr-2" />}
            {showSensitiveData ? 'Hide' : 'Show'} Sensitive Data
          </Button>
        </div>
      </div>

      {/* Filters Section */}
      <Card className="mb-6">
        <CardHeader>
          <CardTitle>Data Filters</CardTitle>
          <CardDescription>Filter data across all tables to find specific records</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
            {/* Dropshipper Filter */}
            <div>
              <label className="text-sm font-medium">Dropshipper</label>
              <select 
                value={selectedDropshipper}
                onChange={(e) => setSelectedDropshipper(e.target.value)}
                className="w-full mt-1 p-2 border rounded-md"
              >
                <option value="all">All Dropshippers</option>
                {(dropsQuery.data || []).map((email: string) => (
                  <option key={email} value={email}>{email}</option>
                ))}
              </select>
            </div>
            
            {/* Order Date Range */}
            <div>
              <label className="text-sm font-medium">Order Date From</label>
              <input
                type="date"
                value={orderDateFrom}
                onChange={(e) => setOrderDateFrom(e.target.value)}
                className="w-full mt-1 p-2 border rounded-md"
              />
            </div>
            <div>
              <label className="text-sm font-medium">Order Date To</label>
              <input
                type="date"
                value={orderDateTo}
                onChange={(e) => setOrderDateTo(e.target.value)}
                className="w-full mt-1 p-2 border rounded-md"
              />
            </div>
            
            {/* Delivered Date Range */}
            <div>
              <label className="text-sm font-medium">Delivered From</label>
              <input
                type="date"
                value={deliveredDateFrom}
                onChange={(e) => setDeliveredDateFrom(e.target.value)}
                className="w-full mt-1 p-2 border rounded-md"
              />
            </div>
            <div>
              <label className="text-sm font-medium">Delivered To</label>
              <input
                type="date"
                value={deliveredDateTo}
                onChange={(e) => setDeliveredDateTo(e.target.value)}
                className="w-full mt-1 p-2 border rounded-md"
              />
            </div>
          </div>
          
          <div className="flex gap-2 mt-4">
            <Button
              onClick={() => {
                setSelectedDropshipper('all');
                setOrderDateFrom('');
                setOrderDateTo('');
                setDeliveredDateFrom('');
                setDeliveredDateTo('');
              }}
              variant="outline"
              size="sm"
            >
              Clear Filters
            </Button>
            <Button
              onClick={() => ordersQuery.refetch()}
              size="sm"
            >
              Apply Filters
            </Button>
          </div>
        </CardContent>
      </Card>

      <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-4">
        <TabsList className="grid w-full grid-cols-6">
          <TabsTrigger value="config-summary" className="text-xs">
            📊 Configuration Summary
          </TabsTrigger>
          <TabsTrigger value="orders" className="text-xs">
            Orders ({orders.length})
          </TabsTrigger>
          <TabsTrigger value="uploads" className="text-xs">
            Upload Sessions ({uploads.length})
          </TabsTrigger>
          <TabsTrigger value="product-prices" className="text-xs">
            Product Prices ({productPrices.length})
          </TabsTrigger>
          <TabsTrigger value="shipping-rates" className="text-xs">
            Shipping Rates ({shippingRates.length})
          </TabsTrigger>
          <TabsTrigger value="payout-logs" className="text-xs">
            Payout Logs ({payoutLogs.length})
          </TabsTrigger>
        </TabsList>

        {/* Configuration Summary Tab - This is what you specifically requested */}
        <TabsContent value="config-summary">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Database className="h-5 w-5" />
                Configuration Summary - Complete Transparency
              </CardTitle>
              <CardDescription>
                See exactly which dropshipper has which product with what weight, cost, and shipping rates
              </CardDescription>
            </CardHeader>
            <CardContent>
              {/* Bulk Upload Section */}
              <div className="mb-6">
                <SettingsUpload />
                <div className="mt-4 p-4 bg-blue-50 border border-blue-200 rounded-lg">
                  <h4 className="font-medium text-blue-800 mb-2">📁 Bulk Upload Instructions:</h4>
                  <ul className="text-sm text-blue-700 space-y-1">
                    <li>• Download template using "Download Template" button below</li>
                    <li>• Fill Excel file with Product Prices and Shipping Rates sheets</li>
                    <li>• Upload completed file above to bulk update all settings</li>
                  </ul>
                </div>
              </div>

              {configSummaryQuery.isLoading ? (
                <div className="text-center py-8">Loading configuration data...</div>
              ) : (
                <div className="space-y-4">
                  {/* Search and Action Controls */}
                  <div className="flex items-center justify-between gap-4">
                    <div className="flex items-center gap-4 flex-1">
                      <div className="relative flex-1 max-w-md">
                        <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
                        <Input
                          placeholder="Search products, dropshippers, UIDs..."
                          value={searchTerm}
                          onChange={(e) => setSearchTerm(e.target.value)}
                          className="pl-10"
                        />
                      </div>
                      <div className="flex items-center gap-2">
                        <Checkbox
                          checked={selectAll}
                          onCheckedChange={handleSelectAll}
                          id="select-all"
                        />
                        <label htmlFor="select-all" className="text-sm font-medium cursor-pointer">
                          Select All ({filteredData.length})
                        </label>
                      </div>
                    </div>
                    
                    {/* Action Buttons */}
                    <div className="flex items-center gap-2">
                      <Button
                        variant="default"
                        size="sm"
                        onClick={handleDownloadTemplate}
                        className="bg-green-600 hover:bg-green-700 text-white"
                      >
                        <Download className="w-4 h-4 mr-2" />
                        Download Template
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => configSummaryQuery.refetch()}
                        className="gap-2"
                      >
                        <RefreshCw className="h-4 w-4" />
                        Refresh
                      </Button>
                      {selectedItems.size > 0 && (
                        <>
                          <Button
                            variant="default"
                            size="sm"
                            onClick={handleBulkUpdate}
                            className="gap-2 bg-blue-600 hover:bg-blue-700"
                          >
                            <Edit className="h-4 w-4" />
                            Update Selected ({selectedItems.size})
                          </Button>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={handleAddMissing}
                            className="gap-2 border-green-600 text-green-600 hover:bg-green-50"
                          >
                            <Plus className="h-4 w-4" />
                            Add Missing Data
                          </Button>
                        </>
                      )}
                    </div>
                  </div>

                  {/* Enhanced Table */}
                  <div className="border rounded-lg overflow-hidden">
                    <div className="max-h-[600px] overflow-auto">
                      <Table>
                        <TableHeader className="sticky top-0 bg-white z-10">
                          <TableRow>
                            <TableHead className="w-12">
                              <Checkbox
                                checked={selectAll}
                                onCheckedChange={handleSelectAll}
                              />
                            </TableHead>
                            <TableHead className="min-w-[200px]">Dropshipper Email</TableHead>
                            <TableHead className="min-w-[150px]">Product Name</TableHead>
                            <TableHead className="min-w-[120px]">Product UID</TableHead>
                            <TableHead className="min-w-[100px]">SKU</TableHead>
                            <TableHead className="text-center min-w-[120px]">Weight (kg)</TableHead>
                            <TableHead className="text-center min-w-[120px]">Cost (Rs.)</TableHead>
                            <TableHead className="min-w-[120px]">Shipping Provider</TableHead>
                            <TableHead className="text-center min-w-[120px]">Shipping Cost (Rs.)</TableHead>
                            <TableHead className="text-center min-w-[100px]">Status</TableHead>
                            <TableHead className="text-center min-w-[100px]">Actions</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {paginatedData.map((config: any, index: number) => {
                            const itemId = `${config.dropshipperEmail}-${config.productUid}-${config.shippingProvider}-${index}`;
                            return (
                              <TableRow key={itemId} className={selectedItems.has(itemId) ? "bg-blue-50" : ""}>
                                <TableCell>
                                  <Checkbox
                                    checked={selectedItems.has(itemId)}
                                    onCheckedChange={(checked) => handleItemSelect(itemId, checked)}
                                  />
                                </TableCell>
                                <TableCell className="font-medium">{config.dropshipperEmail}</TableCell>
                                <TableCell>{config.productName}</TableCell>
                                <TableCell className="font-mono text-xs">{config.productUid}</TableCell>
                                <TableCell>{config.sku || 'N/A'}</TableCell>
                                <TableCell className="text-center">
                                  <Badge variant={config.productWeight ? "default" : "destructive"} className="cursor-pointer">
                                    {config.productWeight || 'Missing'}
                                  </Badge>
                                </TableCell>
                                <TableCell className="text-center">
                                  <Badge variant={config.productCost ? "default" : "destructive"} className="cursor-pointer">
                                    {config.productCost || 'Missing'}
                                  </Badge>
                                </TableCell>
                                <TableCell>{config.shippingProvider}</TableCell>
                                <TableCell className="text-center">
                                  <Badge variant={config.shippingRate ? "default" : "destructive"} className="cursor-pointer">
                                    {config.shippingRate ? `₹${config.shippingRate}/kg` : 'Missing'}
                                  </Badge>
                                </TableCell>
                                <TableCell className="text-center">
                                  {config.productWeight && config.productCost && config.shippingRate ? (
                                    <Badge variant="default" className="bg-green-100 text-green-800">Complete</Badge>
                                  ) : (
                                    <Badge variant="destructive">Incomplete</Badge>
                                  )}
                                </TableCell>
                                <TableCell className="text-center">
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    onClick={() => handleQuickEdit(config)}
                                    className="h-8 w-8 p-0"
                                  >
                                    <Edit className="h-4 w-4" />
                                  </Button>
                                </TableCell>
                              </TableRow>
                            );
                          })}
                        </TableBody>
                      </Table>
                    </div>
                  </div>
                  
                  {/* Results Summary */}
                  {/* Pagination Controls */}
                  <div className="flex flex-col gap-4 px-4 py-3 border-t bg-gray-50">
                    <div className="flex items-center justify-between text-sm text-gray-600">
                      <div className="flex items-center gap-4">
                        <span className="font-medium text-lg">
                          📊 Total: <span className="text-blue-600">{configSummaryQuery.data?.length || 0}</span> configuration items
                          {debouncedSearchTerm && ` → Filtered: ${filteredData.length} matches`}
                        </span>
                        {selectedItems.size > 0 && (
                          <span className="font-medium text-green-600">
                            ✓ {selectedItems.size} selected
                          </span>
                        )}
                      </div>
                      <div className="text-sm font-medium text-gray-700">
                        Page {currentPage} of {totalPages} • Showing {startItem}-{endItem}
                      </div>
                    </div>

                    {/* Pagination Navigation */}
                    {totalPages > 1 && (
                      <div className="flex items-center justify-center gap-2">
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => handlePageChange(currentPage - 1)}
                          disabled={currentPage === 1}
                        >
                          <ChevronLeft className="h-4 w-4" />
                          Previous
                        </Button>
                        
                        <div className="flex items-center gap-2">
                          {/* First page */}
                          {currentPage > 3 && (
                            <>
                              <Button
                                variant={1 === currentPage ? "default" : "outline"}
                                size="sm"
                                onClick={() => handlePageChange(1)}
                                className="w-8 h-8 p-0"
                              >
                                1
                              </Button>
                              {currentPage > 4 && <span className="text-gray-400">...</span>}
                            </>
                          )}
                          
                          {/* Current page range */}
                          {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                            const pageNum = Math.max(1, Math.min(totalPages - 4, currentPage - 2)) + i;
                            if (pageNum > totalPages) return null;
                            return (
                              <Button
                                key={pageNum}
                                variant={pageNum === currentPage ? "default" : "outline"}
                                size="sm"
                                onClick={() => handlePageChange(pageNum)}
                                className="w-8 h-8 p-0"
                              >
                                {pageNum}
                              </Button>
                            );
                          })}
                          
                          {/* Last page */}
                          {currentPage < totalPages - 2 && (
                            <>
                              {currentPage < totalPages - 3 && <span className="text-gray-400">...</span>}
                              <Button
                                variant={totalPages === currentPage ? "default" : "outline"}
                                size="sm"
                                onClick={() => handlePageChange(totalPages)}
                                className="w-8 h-8 p-0"
                              >
                                {totalPages}
                              </Button>
                            </>
                          )}
                        </div>

                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => handlePageChange(currentPage + 1)}
                          disabled={currentPage === totalPages}
                        >
                          Next
                          <ChevronRight className="h-4 w-4" />
                        </Button>
                      </div>
                    )}
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Orders Tab */}
        <TabsContent value="orders">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between">
              <div>
                <CardTitle>Order Data</CardTitle>
                <CardDescription>
                  All order records with complete details
                </CardDescription>
              </div>
              <Button 
                variant="destructive" 
                size="sm"
                onClick={() => handleClearTable('orders')}
              >
                <Trash2 className="w-4 h-4 mr-2" />
                Clear All Orders
              </Button>
            </CardHeader>
            <CardContent>
              <ScrollArea className="h-[600px]">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Upload ID</TableHead>
                      <TableHead>Dropshipper Email</TableHead>
                      <TableHead>Order ID</TableHead>
                      <TableHead>Order Date</TableHead>
                      <TableHead>Product Name</TableHead>
                      <TableHead>SKU</TableHead>
                      <TableHead>Qty</TableHead>
                      <TableHead>COD Amount</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Waybill</TableHead>
                      <TableHead>Delivered Date</TableHead>
                      <TableHead>RTS Date</TableHead>
                      <TableHead>Shipping Provider</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {ordersQuery.isLoading ? (
                      <TableRow>
                        <TableCell colSpan={13} className="text-center py-4">
                          Loading orders...
                        </TableCell>
                      </TableRow>
                    ) : orders.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={13} className="text-center py-4">
                          No orders found
                        </TableCell>
                      </TableRow>
                    ) : orders.map((order: any, index: number) => (
                      <TableRow key={index}>
                        <TableCell>{order.uploadSessionId}</TableCell>
                        <TableCell>
                          {showSensitiveData ? order.dropshipperEmail : '***@***.com'}
                        </TableCell>
                        <TableCell>{order.orderId}</TableCell>
                        <TableCell>{formatDate(order.orderDate)}</TableCell>
                        <TableCell className="max-w-[200px] truncate">
                          {order.productName}
                        </TableCell>
                        <TableCell>{order.sku || 'N/A'}</TableCell>
                        <TableCell>{order.qty}</TableCell>
                        <TableCell>
                          <Badge variant="secondary">
                            {formatCurrency(order.codAmount)}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <Badge variant={order.status === 'Delivered' ? 'default' : 'secondary'}>
                            {order.status}
                          </Badge>
                        </TableCell>
                        <TableCell>{order.waybill || 'N/A'}</TableCell>
                        <TableCell>{formatDate(order.deliveredDate)}</TableCell>
                        <TableCell>{formatDate(order.rtsDate)}</TableCell>
                        <TableCell>{order.shippingProvider}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </ScrollArea>
              
              {orders.length === 0 && (
                <Alert className="mt-4">
                  <AlertDescription>
                    No order data found. Upload some Excel/CSV files to see data here.
                  </AlertDescription>
                </Alert>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Upload Sessions Tab */}
        <TabsContent value="uploads">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between">
              <div>
                <CardTitle>Upload Sessions</CardTitle>
                <CardDescription>
                  History of all file uploads and processing results
                </CardDescription>
              </div>
              <Button 
                variant="destructive" 
                size="sm"
                onClick={() => handleClearTable('upload-sessions')}
              >
                <Trash2 className="w-4 h-4 mr-2" />
                Clear All Sessions
              </Button>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Session ID</TableHead>
                    <TableHead>Filename</TableHead>
                    <TableHead>Upload Date</TableHead>
                    <TableHead>Total Rows</TableHead>
                    <TableHead>Processed Rows</TableHead>
                    <TableHead>Cancelled Rows</TableHead>
                    <TableHead>Status</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {uploads.map((upload: any) => (
                    <TableRow key={upload.id}>
                      <TableCell>{upload.id}</TableCell>
                      <TableCell>{upload.filename}</TableCell>
                      <TableCell>{formatDate(upload.uploadedAt)}</TableCell>
                      <TableCell>{upload.totalRows || 'N/A'}</TableCell>
                      <TableCell>{upload.processedRows || 'N/A'}</TableCell>
                      <TableCell>{upload.cancelledRows || 'N/A'}</TableCell>
                      <TableCell>
                        <Badge variant="default">Completed</Badge>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              
              {uploads.length === 0 && (
                <Alert className="mt-4">
                  <AlertDescription>
                    No upload sessions found. Upload some files to see history here.
                  </AlertDescription>
                </Alert>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Product Prices Tab */}
        <TabsContent value="product-prices">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between">
              <div>
                <CardTitle>Product Prices</CardTitle>
                <CardDescription>
                  All configured product prices for payout calculations
                </CardDescription>
              </div>
              <Button 
                variant="destructive" 
                size="sm"
                onClick={() => handleClearTable('product-prices')}
              >
                <Trash2 className="w-4 h-4 mr-2" />
                Clear All Prices
              </Button>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Dropshipper Email</TableHead>
                    <TableHead>Product UID</TableHead>
                    <TableHead>Product Name</TableHead>
                    <TableHead>SKU</TableHead>
                    <TableHead>Price</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {productPrices.map((price: any, index: number) => (
                    <TableRow key={index}>
                      <TableCell>
                        {showSensitiveData ? price.dropshipperEmail : '***@***.com'}
                      </TableCell>
                      <TableCell>{price.productUid}</TableCell>
                      <TableCell className="max-w-[200px] truncate">
                        {price.productName}
                      </TableCell>
                      <TableCell>{price.sku || 'N/A'}</TableCell>
                      <TableCell>
                        <Badge variant="secondary">
                          {formatCurrency(price.price)}
                        </Badge>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              
              {productPrices.length === 0 && (
                <Alert className="mt-4">
                  <AlertDescription>
                    No product prices configured. Go to Settings to add product prices.
                  </AlertDescription>
                </Alert>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Shipping Rates Tab */}
        <TabsContent value="shipping-rates">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between">
              <div>
                <CardTitle>Shipping Rates</CardTitle>
                <CardDescription>
                  All configured shipping rates by provider
                </CardDescription>
              </div>
              <Button 
                variant="destructive" 
                size="sm"
                onClick={() => handleClearTable('shipping-rates')}
              >
                <Trash2 className="w-4 h-4 mr-2" />
                Clear All Rates
              </Button>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Shipping Provider</TableHead>
                    <TableHead>Rate</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {shippingRates.map((rate: any, index: number) => (
                    <TableRow key={index}>
                      <TableCell>{rate.shippingProvider}</TableCell>
                      <TableCell>
                        <Badge variant="secondary">
                          {formatCurrency(rate.rate)}
                        </Badge>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              
              {shippingRates.length === 0 && (
                <Alert className="mt-4">
                  <AlertDescription>
                    No shipping rates configured. Go to Settings to add shipping rates.
                  </AlertDescription>
                </Alert>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Payout Logs Tab */}
        <TabsContent value="payout-logs">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between">
              <div>
                <CardTitle>Payout Logs</CardTitle>
                <CardDescription>
                  History of all payout calculations and reconciliations
                </CardDescription>
              </div>
              <Button 
                variant="destructive" 
                size="sm"
                onClick={() => handleClearTable('payout-logs')}
              >
                <Trash2 className="w-4 h-4 mr-2" />
                Clear All Logs
              </Button>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Order ID</TableHead>
                    <TableHead>Waybill</TableHead>
                    <TableHead>Dropshipper Email</TableHead>
                    <TableHead>Product UID</TableHead>
                    <TableHead>Paid Amount</TableHead>
                    <TableHead>Paid On</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {payoutLogs.map((log: any, index: number) => (
                    <TableRow key={index}>
                      <TableCell>{log.orderId}</TableCell>
                      <TableCell>{log.waybill || 'N/A'}</TableCell>
                      <TableCell>
                        {showSensitiveData ? log.dropshipperEmail : '***@***.com'}
                      </TableCell>
                      <TableCell>{log.productUid}</TableCell>
                      <TableCell>
                        <Badge variant="secondary">
                          {formatCurrency(log.paidAmount)}
                        </Badge>
                      </TableCell>
                      <TableCell>{formatDate(log.paidOn)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              
              {payoutLogs.length === 0 && (
                <Alert className="mt-4">
                  <AlertDescription>
                    No payout logs found. Process some payouts to see history here.
                  </AlertDescription>
                </Alert>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      {/* Quick Edit Dialog */}
      <Dialog open={editDialog.open} onOpenChange={(open) => !open && setEditDialog({ ...editDialog, open: false })}>
        <DialogContent className="sm:max-w-[425px]">
          <DialogHeader>
            <DialogTitle>Quick Edit</DialogTitle>
            <DialogDescription>
              Update product weight, cost, and shipping rate for{' '}
              <span className="font-medium">{editDialog.config?.productName}</span>
            </DialogDescription>
          </DialogHeader>
          
          <div className="grid gap-4 py-4">
            <div className="grid grid-cols-4 items-center gap-4">
              <Label htmlFor="weight" className="text-right">
                Weight (kg)
              </Label>
              <Input
                id="weight"
                type="number"
                step="0.1"
                placeholder="0.5"
                value={editDialog.formData.productWeight}
                onChange={(e) => setEditDialog({
                  ...editDialog,
                  formData: { ...editDialog.formData, productWeight: e.target.value }
                })}
                className="col-span-3"
              />
            </div>
            
            <div className="grid grid-cols-4 items-center gap-4">
              <Label htmlFor="cost" className="text-right">
                Cost (Rs.)
              </Label>
              <Input
                id="cost"
                type="number"
                step="0.01"
                placeholder="0"
                value={editDialog.formData.productCost}
                onChange={(e) => setEditDialog({
                  ...editDialog,
                  formData: { ...editDialog.formData, productCost: e.target.value }
                })}
                className="col-span-3"
              />
            </div>
            
            <div className="grid grid-cols-4 items-center gap-4">
              <Label htmlFor="shipping" className="text-right">
                Shipping (Rs./kg)
              </Label>
              <Input
                id="shipping"
                type="number"
                step="0.01"
                placeholder="25"
                value={editDialog.formData.shippingRate}
                onChange={(e) => setEditDialog({
                  ...editDialog,
                  formData: { ...editDialog.formData, shippingRate: e.target.value }
                })}
                className="col-span-3"
              />
            </div>
            
            <div className="text-sm text-gray-600 bg-blue-50 p-3 rounded">
              <strong>Dropshipper:</strong> {editDialog.config?.dropshipperEmail}<br/>
              <strong>Provider:</strong> {editDialog.config?.shippingProvider}
            </div>
          </div>
          
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => setEditDialog({ ...editDialog, open: false })}
            >
              <X className="h-4 w-4 mr-2" />
              Cancel
            </Button>
            <Button type="button" onClick={handleSaveEdit}>
              <Save className="h-4 w-4 mr-2" />
              Save Changes
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}