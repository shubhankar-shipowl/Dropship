import { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Input } from "@/components/ui/input";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { Download, FileText, Calendar, User, ChevronDown, Package, Truck, DollarSign, Search, Filter, MoreVertical, Database, Settings } from 'lucide-react';
import { format } from 'date-fns';
import { apiRequest } from "@/lib/queryClient";

interface UploadSession {
  id: string;
  filename: string;
  uploadedAt: string;
  orderCount: number;
  dropshipperEmail?: string;
  productPricesCount?: number;
  shippingRatesCount?: number;
  hasProductPrices?: boolean;
  hasShippingRates?: boolean;
}

export function UploadHistory() {
  const { data: uploads = [], isLoading } = useQuery<UploadSession[]>({
    queryKey: ['/api/transparency/uploads'],
    enabled: true
  });

  const [expandedFiles, setExpandedFiles] = useState<Set<string>>(new Set());
  const [searchTerm, setSearchTerm] = useState('');

  const toggleExpanded = (fileId: string) => {
    const newExpanded = new Set(expandedFiles);
    if (newExpanded.has(fileId)) {
      newExpanded.delete(fileId);
    } else {
      newExpanded.add(fileId);
    }
    setExpandedFiles(newExpanded);
  };

  // Filter uploads based on search term
  const filteredUploads = uploads.filter(upload => 
    upload.filename.toLowerCase().includes(searchTerm.toLowerCase()) ||
    upload.dropshipperEmail?.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const handleDownload = async (sessionId: string, filename: string, type = 'orders') => {
    try {
      const response = await fetch(`/api/download-original/${sessionId}?type=${type}`);
      
      if (!response.ok) {
        throw new Error('Download failed');
      }

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.style.display = 'none';
      a.href = url;
      
      // Set appropriate filename based on type
      const baseFilename = filename.replace(/\.[^/.]+$/, '');
      let downloadFilename = filename;
      if (type === 'prices') {
        downloadFilename = `${baseFilename}_product_prices.xlsx`;
      } else if (type === 'shipping') {
        downloadFilename = `${baseFilename}_shipping_rates.xlsx`;
      } else if (type === 'all') {
        downloadFilename = `${baseFilename}_complete_data.xlsx`;
      }
      
      a.download = downloadFilename;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
    } catch (error) {
      console.error('Download error:', error);
    }
  };

  if (isLoading) {
    return (
      <div className="p-4">
        <div className="animate-pulse">
          <div className="h-4 bg-gray-200 rounded w-1/4 mb-4"></div>
          <div className="space-y-2">
            {[...Array(3)].map((_, i) => (
              <div key={i} className="h-16 bg-gray-100 rounded"></div>
            ))}
          </div>
        </div>
      </div>
    );
  }

  return (
    <Card className="m-4">
      <CardHeader className="pb-4">
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <FileText className="h-5 w-5 text-blue-600" />
            Upload History
          </CardTitle>
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Package className="h-4 w-4" />
            {uploads.length} files
          </div>
        </div>
        
        {/* Search Bar */}
        <div className="relative">
          <Search className="absolute left-3 top-3 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search files by name or email..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="pl-10 bg-muted/30"
            data-testid="input-search-uploads"
          />
        </div>
      </CardHeader>
      <CardContent>
        <div className="space-y-3">
          {filteredUploads.length === 0 ? (
            <div className="text-center text-muted-foreground py-12">
              {searchTerm ? (
                <div>
                  <Search className="h-12 w-12 mx-auto mb-4 text-muted-foreground/50" />
                  <p className="text-lg font-medium">कोई files नहीं मिलीं</p>
                  <p className="text-sm">"{searchTerm}" के लिए search results नहीं मिले</p>
                </div>
              ) : uploads.length === 0 ? (
                <div>
                  <FileText className="h-12 w-12 mx-auto mb-4 text-muted-foreground/50" />
                  <p className="text-lg font-medium">कोई uploaded files नहीं हैं</p>
                  <p className="text-sm">Upload Data tab से files upload करें</p>
                </div>
              ) : null}
            </div>
          ) : (
            filteredUploads.map((upload) => (
              <Collapsible 
                key={upload.id}
                open={expandedFiles.has(upload.id)}
                onOpenChange={() => toggleExpanded(upload.id)}
              >
                <div className="border rounded-lg hover:bg-muted/50 transition-all duration-200 hover:shadow-sm">
                  <div className="flex items-center justify-between p-4">
                    <div className="flex items-center gap-4 flex-1">
                      <CollapsibleTrigger asChild>
                        <Button variant="ghost" size="sm" className="p-1 hover:bg-muted/70 transition-colors">
                          <ChevronDown className={`h-4 w-4 transition-transform duration-200 ${expandedFiles.has(upload.id) ? 'rotate-180' : ''}`} />
                        </Button>
                      </CollapsibleTrigger>
                      
                      <div className="flex-shrink-0">
                        <FileText className="h-8 w-8 text-blue-500" />
                      </div>
                      
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2 mb-1 flex-wrap">
                          <span className="font-medium truncate">
                            {upload.filename}
                          </span>
                          <Badge variant="secondary" className="font-medium">
                            {(upload.orderCount || 0).toLocaleString()} orders
                          </Badge>
                          
                          {upload.hasProductPrices && (
                            <Badge variant="outline" className="bg-green-50 text-green-700 border-green-200 font-medium">
                              <Package className="h-3 w-3 mr-1" />
                              {(upload.productPricesCount || 0).toLocaleString()} prices
                            </Badge>
                          )}
                          
                          {upload.hasShippingRates && (
                            <Badge variant="outline" className="bg-blue-50 text-blue-700 border-blue-200 font-medium">
                              <Truck className="h-3 w-3 mr-1" />
                              {(upload.shippingRatesCount || 0).toLocaleString()} rates
                            </Badge>
                          )}
                        </div>
                        
                        <div className="flex items-center gap-4 text-sm text-muted-foreground">
                          <div className="flex items-center gap-1">
                            <Calendar className="h-3 w-3" />
                            {format(new Date(upload.uploadedAt), 'dd MMM yyyy, hh:mm a')}
                          </div>
                          {upload.dropshipperEmail && (
                            <div className="flex items-center gap-1">
                              <User className="h-3 w-3" />
                              {upload.dropshipperEmail}
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                    
                    <div className="flex gap-2">
                      <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                          <Button
                            variant="outline"
                            size="sm"
                            data-testid={`button-download-${upload.id}`}
                            className="flex items-center gap-2 hover:bg-blue-50 hover:text-blue-600 hover:border-blue-200 transition-colors"
                          >
                            <Download className="h-4 w-4" />
                            Download
                            <ChevronDown className="h-3 w-3" />
                          </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end" className="w-48">
                          <DropdownMenuItem onClick={() => handleDownload(upload.id, upload.filename, 'orders')}>
                            <FileText className="h-4 w-4 mr-2" />
                            Order Data Only
                          </DropdownMenuItem>
                          
                          <DropdownMenuItem onClick={() => handleDownload(upload.id, upload.filename, 'prices')}>
                            <Package className="h-4 w-4 mr-2 text-green-600" />
                            Product Prices Only
                          </DropdownMenuItem>
                          
                          <DropdownMenuItem onClick={() => handleDownload(upload.id, upload.filename, 'shipping')}>
                            <Truck className="h-4 w-4 mr-2 text-blue-600" />
                            Shipping Rates Only
                          </DropdownMenuItem>
                          
                          <DropdownMenuItem onClick={() => handleDownload(upload.id, upload.filename, 'all')}>
                            <Database className="h-4 w-4 mr-2 text-purple-600" />
                            Complete Data
                          </DropdownMenuItem>
                        </DropdownMenuContent>
                      </DropdownMenu>
                    </div>
                  </div>
                  
                  <CollapsibleContent>
                    <div className="px-4 pb-4 border-t bg-gradient-to-r from-muted/20 to-muted/10">
                      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mt-4">
                        <div className="flex items-start gap-3 p-3 rounded-lg bg-green-50/50 border border-green-100">
                          <Package className="h-5 w-5 text-green-600 mt-0.5" />
                          <div>
                            <div className="font-medium text-green-800">Product Prices</div>
                            <div className="text-sm text-green-600">
                              {upload.hasProductPrices ? 
                                `${(upload.productPricesCount || 0).toLocaleString()} items uploaded` : 
                                'Not included in this file'
                              }
                            </div>
                          </div>
                        </div>
                        
                        <div className="flex items-start gap-3 p-3 rounded-lg bg-blue-50/50 border border-blue-100">
                          <Truck className="h-5 w-5 text-blue-600 mt-0.5" />
                          <div>
                            <div className="font-medium text-blue-800">Shipping Rates</div>
                            <div className="text-sm text-blue-600">
                              {upload.hasShippingRates ? 
                                `${(upload.shippingRatesCount || 0).toLocaleString()} rates uploaded` : 
                                'Not included in this file'
                              }
                            </div>
                          </div>
                        </div>
                        
                        <div className="flex items-start gap-3 p-3 rounded-lg bg-purple-50/50 border border-purple-100">
                          <DollarSign className="h-5 w-5 text-purple-600 mt-0.5" />
                          <div>
                            <div className="font-medium text-purple-800">Order Data</div>
                            <div className="text-sm text-purple-600">
                              {(upload.orderCount || 0).toLocaleString()} orders processed
                            </div>
                          </div>
                        </div>
                      </div>
                      
                      {upload.dropshipperEmail && (
                        <div className="mt-3 pt-3 border-t">
                          <span className="text-sm text-muted-foreground">
                            <span className="font-medium">Dropshipper:</span> {upload.dropshipperEmail}
                          </span>
                        </div>
                      )}
                    </div>
                  </CollapsibleContent>
                </div>
              </Collapsible>
            ))
          )}
        </div>
        
        {uploads.length > 0 && (
          <div className="mt-6 pt-4 border-t text-sm text-muted-foreground">
            <div className="flex items-center justify-between">
              <span>
                Total {uploads.length} files uploaded
                {searchTerm && ` • Showing ${filteredUploads.length} results`}
              </span>
              <span className="text-xs">
                {uploads.reduce((sum, upload) => sum + (upload.orderCount || 0), 0).toLocaleString()} orders total
              </span>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}