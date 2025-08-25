import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { Upload, FileSpreadsheet } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useToast } from "@/hooks/use-toast";
import { queryClient } from "@/lib/queryClient";

export default function SettingsUpload() {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const { toast } = useToast();

  const uploadMutation = useMutation({
    mutationFn: async (file: File) => {
      const formData = new FormData();
      formData.append('file', file);

      const response = await fetch('/api/import-settings', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        throw new Error('Upload failed');
      }

      return response.json();
    },
    onSuccess: (data) => {
      toast({
        title: "Settings imported successfully",
        description: `Imported ${data.importedPrices} product prices and ${data.importedRates} shipping rates.`,
      });
      queryClient.invalidateQueries({ queryKey: ['/api/product-prices'] });
      queryClient.invalidateQueries({ queryKey: ['/api/shipping-rates'] });
      queryClient.invalidateQueries({ queryKey: ['/api/transparency/config-summary'] });
      queryClient.invalidateQueries({ queryKey: ['/api/transparency/product-prices'] });
      queryClient.invalidateQueries({ queryKey: ['/api/transparency/shipping-rates'] });
      queryClient.invalidateQueries({ queryKey: ['/api/missing-data'] });
      setSelectedFile(null);
    },
    onError: () => {
      toast({
        title: "Import failed",
        description: "Could not import settings. Please check the file format.",
        variant: "destructive",
      });
    }
  });

  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      setSelectedFile(file);
    }
  };

  const handleUpload = () => {
    if (selectedFile) {
      uploadMutation.mutate(selectedFile);
    }
  };

  return (
    <Card className="mb-6 border-green-200 bg-green-50">
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-green-800">
          <Upload className="h-5 w-5" />
          Settings Upload
        </CardTitle>
        <CardDescription className="text-green-700">
          Upload Excel file with product prices and shipping rates
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="space-y-3">
          <div className="flex items-center space-x-2">
            <Input
              type="file"
              accept=".xlsx,.xls"
              onChange={handleFileSelect}
              className="flex-1"
              data-testid="input-settings-file"
            />
            <Button 
              onClick={handleUpload}
              disabled={!selectedFile || uploadMutation.isPending}
              data-testid="button-upload-settings"
            >
              {uploadMutation.isPending ? 'Uploading...' : 'Upload'}
            </Button>
          </div>
          
          {selectedFile && (
            <div className="text-sm text-green-700">
              Selected: {selectedFile.name} ({Math.round(selectedFile.size / 1024)} KB)
            </div>
          )}
        </div>

        <div className="p-3 bg-green-100 rounded-lg">
          <h4 className="font-medium text-green-800 mb-2">Excel Format Required:</h4>
          <ul className="text-sm text-green-700 space-y-1">
            <li>• <strong>Sheet 1:</strong> "Product Prices" - Dropshipper Email, Product UID, Product Name, SKU, Product Weight (kg), Product Cost Per Unit, Currency</li>
            <li>• <strong>Sheet 2:</strong> "Shipping Rates" - Shipping UID, Product UID, Product Weight (kg), Shipping Provider, Shipping Rate Per Kg, Currency</li>
            <li>• <strong>Shipping UID Format:</strong> DropshipperEmail+ProductName+Weight+Provider (no separators)</li>
            <li>• Download template to get proper Excel format with column widths and examples</li>
            <li>• Delete instruction/example rows before upload - system handles duplicates automatically</li>
          </ul>
        </div>
      </CardContent>
    </Card>
  );
}