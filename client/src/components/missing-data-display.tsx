import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, Download, Settings } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ScrollArea } from "@/components/ui/scroll-area";

interface MissingDataDisplayProps {
  onConfigureClick: () => void;
}

interface MissingData {
  missingPrices: Array<{ 
    dropshipperEmail: string; 
    productUid: string; 
    productName: string; 
    sku: string | null 
  }>;
  missingRates: string[];
}

export default function MissingDataDisplay({ onConfigureClick }: MissingDataDisplayProps) {
  const { data: missingData, isLoading } = useQuery<MissingData>({
    queryKey: ['/api/missing-data'],
    refetchInterval: 30000, // Refresh every 30 seconds
  });

  const handleDownloadTemplate = async () => {
    try {
      const response = await fetch('/api/export-settings');
      if (response.ok) {
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'settings-template.xlsx';
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
      }
    } catch (error) {
      console.error('Error downloading template:', error);
    }
  };

  if (isLoading) {
    return (
      <Card className="mb-6 border-orange-200 bg-orange-50">
        <CardContent className="pt-6">
          <div className="text-center text-orange-700">Loading missing data...</div>
        </CardContent>
      </Card>
    );
  }

  if (!missingData || (missingData.missingPrices.length === 0 && missingData.missingRates.length === 0)) {
    return (
      <Card className="mb-6 border-green-200 bg-green-50">
        <CardContent className="pt-6">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <div className="w-2 h-2 rounded-full bg-green-400 mt-2"></div>
            </div>
            <div className="ml-3">
              <h3 className="text-sm font-medium text-green-800">
                All Settings Configured
              </h3>
              <p className="text-sm text-green-700 mt-1">
                All product prices and shipping rates are properly configured.
              </p>
            </div>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="mb-6 border-orange-200 bg-orange-50">
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-orange-800">
          <AlertTriangle className="h-5 w-5" />
          Missing Product Prices & Shipping Rates
        </CardTitle>
        <CardDescription className="text-orange-700">
          {missingData.missingPrices.length} products और {missingData.missingRates.length} shipping providers को configure करना है - एक ही Excel में
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex gap-2 mb-4">
          <Button 
            onClick={handleDownloadTemplate}
            variant="outline"
            size="sm"
            className="border-orange-300 text-orange-800 hover:bg-orange-100"
            data-testid="button-download-template"
          >
            <Download className="h-4 w-4 mr-2" />
            Download Excel Template
          </Button>
          <Button 
            onClick={onConfigureClick}
            variant="outline"
            size="sm"
            className="border-orange-300 text-orange-800 hover:bg-orange-100"
            data-testid="button-configure-manual"
          >
            <Settings className="h-4 w-4 mr-2" />
            Manual Configuration
          </Button>
        </div>

        <div className="space-y-6">
          {/* Product Prices & NDR Shipping Rates Combined Section */}
          <div>
            <h3 className="text-lg font-medium text-orange-800 mb-3 flex items-center gap-2">
              <AlertTriangle className="h-5 w-5" />
              Missing Product Prices & Shipping Rates
            </h3>
            
            <ScrollArea className="h-80 w-full rounded-md border bg-white">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Dropshipper Email</TableHead>
                    <TableHead>Product Name</TableHead>
                    <TableHead>SKU</TableHead>
                    <TableHead>Product UID</TableHead>
                    <TableHead>Product Cost (Rs.)</TableHead>
                    <TableHead>Shipping Rate (Rs.)</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {missingData.missingPrices.map((price, index) => (
                    <TableRow key={index}>
                      <TableCell className="font-medium">{price.dropshipperEmail}</TableCell>
                      <TableCell>{price.productName}</TableCell>
                      <TableCell>{price.sku || 'N/A'}</TableCell>
                      <TableCell className="text-sm text-gray-600">{price.productUid}</TableCell>
                      <TableCell className="text-orange-600 font-medium">Missing</TableCell>
                      <TableCell className="text-orange-600 font-medium">Missing</TableCell>
                    </TableRow>
                  ))}
                  {/* Show missing shipping rates too */}
                  {missingData.missingRates.map((provider, index) => (
                    <TableRow key={`rate-${index}`}>
                      <TableCell colSpan={4} className="font-medium text-gray-600">
                        Shipping Provider: {provider}
                      </TableCell>
                      <TableCell>-</TableCell>
                      <TableCell className="text-orange-600 font-medium">Missing Rate</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </ScrollArea>
          </div>
        </div>

        <div className="p-4 bg-orange-100 rounded-lg">
          <h4 className="font-medium text-orange-800 mb-2">Combined Configuration Instructions:</h4>
          <ul className="text-sm text-orange-700 space-y-1">
            <li>• <strong>Excel Template:</strong> Download one Excel with both product prices and shipping rates</li>
            <li>• <strong>Product Costs:</strong> Set cost per unit for each dropshipper-product combination</li>
            <li>• <strong>Shipping Rates:</strong> Configure shipping rates alongside product data</li>
            <li>• <strong>Single Upload:</strong> Upload completed Excel to configure all settings together</li>
          </ul>
          <div className="mt-3 p-4 bg-blue-50 border border-blue-200 rounded text-sm text-blue-800">
            <strong>🎯 Product Weight Configuration Process:</strong>
            <div className="mt-2 grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <strong className="text-blue-900">Step 1: Product Prices Sheet</strong>
                <ul className="mt-1 space-y-1 text-xs">
                  <li>• Set "Product Weight (kg)" column</li>
                  <li>• Example: Product A = 0.5kg</li>
                  <li>• Example: Product B = 1.0kg</li>
                </ul>
              </div>
              <div>
                <strong className="text-blue-900">Step 2: Shipping Rates Sheet</strong>
                <ul className="mt-1 space-y-1 text-xs">
                  <li>• 0.5kg weight → Rs.25 rate</li>
                  <li>• 1.0kg weight → Rs.20 rate</li>
                  <li>• System auto-matches by weight</li>
                </ul>
              </div>
            </div>
            <div className="mt-3 p-2 bg-blue-100 rounded text-xs">
              <strong>Important:</strong> Product weight को Product Prices sheet में define करना जरूरी है. फिर system automatically उस weight के basis पर correct shipping rate apply करेगा.
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}