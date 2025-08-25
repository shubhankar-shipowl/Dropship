import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { Settings, Download, FileSpreadsheet } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { useToast } from "@/hooks/use-toast";
import { apiRequest, queryClient } from "@/lib/queryClient";

export default function QuickSetup() {
  const [isSettingUp, setIsSettingUp] = useState(false);
  const { toast } = useToast();

  const setupDefaultsMutation = useMutation({
    mutationFn: () => apiRequest('POST', '/api/setup-defaults'),
    onSuccess: () => {
      toast({
        title: "Setup completed",
        description: "Default shipping rates and sample product prices have been added.",
      });
      queryClient.invalidateQueries({ queryKey: ['/api/shipping-rates'] });
      queryClient.invalidateQueries({ queryKey: ['/api/product-prices'] });
    },
    onError: () => {
      toast({
        title: "Setup failed",
        description: "Could not create default settings. Please try again.",
        variant: "destructive",
      });
    }
  });

  const handleSetupDefaults = async () => {
    setIsSettingUp(true);
    await setupDefaultsMutation.mutateAsync();
    setIsSettingUp(false);
  };

  const downloadSampleExcel = () => {
    // Create sample Excel data
    const sampleData = [
      ['Dropshipper Email', 'Order ID', 'Order Date', 'Waybill', 'Product Name', 'SKU', 'Qty', 'COD Amount', 'Status', 'Delivered Date', 'RTS Date', 'Shipping Provider'],
      ['dropshipper1@example.com', 'ORDER001', '2024-01-15', 'WAY123456', 'Product A', 'SKU001', '2', '500', 'delivered', '2024-01-20', '', 'Delhivery'],
      ['dropshipper1@example.com', 'ORDER002', '2024-01-16', 'WAY123457', 'Product B', 'SKU002', '1', '300', 'delivered', '2024-01-21', '', 'BlueDart'],
      ['dropshipper2@example.com', 'ORDER003', '2024-01-17', 'WAY123458', 'Product C', 'SKU003', '3', '750', 'shipped', '', '', 'DTDC'],
      ['dropshipper2@example.com', 'ORDER004', '2024-01-18', 'WAY123459', 'Product D', 'SKU004', '1', '200', 'rts', '', '2024-01-25', 'Ecom Express'],
    ];

    // Convert to CSV and download
    const csvContent = sampleData.map(row => row.join(',')).join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'sample_orders_template.csv';
    a.click();
    window.URL.revokeObjectURL(url);
  };

  const downloadProductPricesTemplate = () => {
    const priceData = [
      ['Dropshipper Email', 'Product UID', 'Product Name', 'SKU', 'Product Cost Per Unit', 'Currency'],
      ['dropshipper1@example.com', 'SKU001', 'Product A', 'SKU001', '100', 'INR'],
      ['dropshipper1@example.com', 'SKU002', 'Product B', 'SKU002', '150', 'INR'],
      ['dropshipper2@example.com', 'SKU003', 'Product C', 'SKU003', '200', 'INR'],
    ];

    const csvContent = priceData.map(row => row.join(',')).join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'product_prices_template.csv';
    a.click();
    window.URL.revokeObjectURL(url);
  };

  const downloadAllSettings = async () => {
    try {
      const response = await fetch('/api/export-settings', {
        method: 'GET',
      });
      
      if (!response.ok) throw new Error('Failed to download settings');
      
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'all_settings.xlsx';
      a.click();
      window.URL.revokeObjectURL(url);
      
      toast({
        title: "Download completed",
        description: "All settings exported to Excel file.",
      });
    } catch (error) {
      toast({
        title: "Download failed",
        description: "Could not download settings file.",
        variant: "destructive",
      });
    }
  };

  const downloadShippingRatesTemplate = () => {
    const rateData = [
      ['Shipping Provider', 'Shipping Rate Per Order', 'Currency'],
      ['Delhivery', '45', 'INR'],
      ['BlueDart', '50', 'INR'],
      ['DTDC', '40', 'INR'],
      ['Ecom Express', '42', 'INR'],
    ];

    const csvContent = rateData.map(row => row.join(',')).join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'shipping_rates_template.csv';
    a.click();
    window.URL.revokeObjectURL(url);
  };

  return (
    <Card className="mb-6 border-blue-200 bg-blue-50">
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-blue-800">
          <Settings className="h-5 w-5" />
          Quick Setup Guide
        </CardTitle>
        <CardDescription className="text-blue-700">
          Get started quickly with ready-made templates and default settings
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="space-y-3">
            <h3 className="font-medium text-blue-800">1. Setup Default Settings</h3>
            <p className="text-sm text-blue-700">
              Add common shipping providers and sample product prices to get started
            </p>
            <Button 
              onClick={handleSetupDefaults} 
              disabled={isSettingUp || setupDefaultsMutation.isPending}
              className="w-full"
              data-testid="button-setup-defaults"
            >
              {isSettingUp ? 'Setting up...' : 'Create Default Settings'}
            </Button>
          </div>

          <div className="space-y-3">
            <h3 className="font-medium text-blue-800">2. Download Templates</h3>
            <p className="text-sm text-blue-700">
              Download ready-made Excel/CSV templates with sample data
            </p>
            <div className="space-y-2">
              <Button 
                onClick={downloadAllSettings}
                className="w-full bg-green-600 hover:bg-green-700"
                data-testid="button-download-all-settings"
              >
                <Download className="h-4 w-4 mr-2" />
                Download All Settings Excel
              </Button>
              <Button 
                variant="outline" 
                onClick={downloadSampleExcel}
                className="w-full"
                data-testid="button-download-orders-template"
              >
                <FileSpreadsheet className="h-4 w-4 mr-2" />
                Orders Template
              </Button>
              <Button 
                variant="outline" 
                onClick={downloadProductPricesTemplate}
                className="w-full"
                data-testid="button-download-prices-template"
              >
                <Download className="h-4 w-4 mr-2" />
                Product Prices Template
              </Button>
            </div>
          </div>
        </div>

        <div className="mt-4 p-3 bg-blue-100 rounded-lg">
          <h4 className="font-medium text-blue-800 mb-2">How to use:</h4>
          <ol className="text-sm text-blue-700 space-y-1">
            <li>1. Click "Create Default Settings" to add common shipping providers</li>
            <li>2. Download the templates and fill them with your data</li>
            <li>3. Upload the completed Excel/CSV files using the upload section</li>
            <li>4. Use the Settings dialog to add/edit product prices and shipping rates</li>
          </ol>
        </div>
      </CardContent>
    </Card>
  );
}