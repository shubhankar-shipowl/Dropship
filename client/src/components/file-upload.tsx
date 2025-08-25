import { useState, useRef } from "react";
import { Upload, File, CheckCircle, FileText, Sparkles, Database } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { useToast } from "@/hooks/use-toast";
import UploadProgress from "./upload-progress";
import ManualMapping from "./manual-mapping";
import * as XLSX from 'xlsx';

interface FileUploadProps {
  onUploadSuccess: (data: { processedRows: number; cancelledRows: number }) => void;
}

interface PreviewData {
  filename: string;
  headers: string[];
  sampleRows: any[][];
  autoMapping: Record<string, number>;
  totalRows: number;
  requiredFields: string[];
}

export default function FileUpload({ onUploadSuccess }: FileUploadProps) {
  const [isUploading, setIsUploading] = useState(false);
  const [dragActive, setDragActive] = useState(false);
  const [uploadSuccess, setUploadSuccess] = useState<{ processedRows: number; cancelledRows: number } | null>(null);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [currentFile, setCurrentFile] = useState<File | null>(null);
  const [previewData, setPreviewData] = useState<PreviewData | null>(null);
  const [showManualMapping, setShowManualMapping] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const { toast } = useToast();

  const handleFiles = async (files: FileList | null) => {
    console.log('handleFiles called with:', files?.length, 'files');
    if (!files || files.length === 0) return;

    const file = files[0];
    console.log('Processing file:', file.name, 'Type:', file.type);
    const allowedTypes = [
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'application/vnd.ms-excel',
      'text/csv'
    ];

    if (!allowedTypes.includes(file.type) && !file.name.match(/\.(xlsx|xls|csv)$/i)) {
      toast({
        title: "Invalid file type",
        description: "Please upload an Excel (.xlsx, .xls) or CSV file.",
        variant: "destructive",
      });
      return;
    }

    if (file.size > 200 * 1024 * 1024) { // 200MB limit
      toast({
        title: "File too large",
        description: "Please upload a file smaller than 200MB.",
        variant: "destructive",
      });
      return;
    }

    setCurrentFile(file);
    setUploadSuccess(null);
    setUploadError(null);

    // Read Excel file locally to get actual column headers
    try {
      console.log('Reading Excel file locally for headers...');
      
      const arrayBuffer = await file.arrayBuffer();
      const workbook = XLSX.read(arrayBuffer);
      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      const data = XLSX.utils.sheet_to_json(worksheet, { header: 1 });
      
      if (data.length < 1) {
        throw new Error('Excel file appears to be empty');
      }

      const headers = data[0].map((h: any) => String(h || ''));
      const sampleRows = data.slice(1, 4); // First 3 data rows for preview
      
      console.log('Excel headers found:', headers);
      
      // Auto-detect mappings based on column names
      const autoMapping: Record<string, number> = {};
      headers.forEach((header, index) => {
        const lowerHeader = header.toLowerCase().trim();
        
        // Map common variations
        if (lowerHeader.includes('order') && (lowerHeader.includes('account') || lowerHeader.includes('email'))) {
          autoMapping.dropshipperEmail = index;
        } else if (lowerHeader === 'orderid' || lowerHeader === 'order id') {
          autoMapping.orderId = index;
        } else if (lowerHeader.includes('order date') || lowerHeader.includes('orderdate')) {
          autoMapping.orderDate = index;
        } else if (lowerHeader.includes('product name') || lowerHeader.includes('productname')) {
          autoMapping.productName = index;
        } else if (lowerHeader.includes('product qty') || lowerHeader.includes('quantity') || lowerHeader === 'qty') {
          autoMapping.qty = index;
        } else if (lowerHeader.includes('product value') || lowerHeader.includes('productvalue')) {
          autoMapping.productValue = index;
        } else if (lowerHeader.includes('cod amount') || lowerHeader.includes('codamount')) {
          autoMapping.productValue = index;
        } else if (lowerHeader === 'mode' || lowerHeader.includes('payment mode') || lowerHeader.includes('payment type')) {
          autoMapping.mode = index;
        } else if (lowerHeader === 'status') {
          autoMapping.status = index;
        } else if (lowerHeader.includes('waybill') || lowerHeader.includes('way bill')) {
          autoMapping.waybill = index;
        } else if (lowerHeader === 'sku') {
          autoMapping.sku = index;
        } else if (lowerHeader.includes('delivered date') || lowerHeader.includes('delivereddate')) {
          autoMapping.deliveredDate = index;
        } else if (lowerHeader.includes('rts date') || lowerHeader.includes('rtsdate')) {
          autoMapping.rtsDate = index;
        }
        
        // Try to detect shipping provider from various fields
        if (lowerHeader.includes('express') || lowerHeader.includes('courier') || lowerHeader.includes('fulfil')) {
          autoMapping.shippingProvider = index;
        }
      });
      
      console.log('Auto-detected mappings:', autoMapping);
      
      const previewData: PreviewData = {
        filename: file.name,
        headers,
        sampleRows,
        autoMapping,
        totalRows: data.length - 1,
        requiredFields: ['dropshipperEmail', 'orderId', 'orderDate', 'productName', 'qty', 'productValue', 'status', 'shippingProvider']
      };

      setPreviewData(previewData);
      setShowManualMapping(true);
      
    } catch (error) {
      console.error('Local file reading error:', error);
      toast({
        title: "File Reading Failed",
        description: error instanceof Error ? error.message : "Could not read Excel file",
        variant: "destructive",
      });
      setUploadError(error instanceof Error ? error.message : "Could not read Excel file");
    }
  };

  const handleManualMapping = async (mapping: Record<string, number>) => {
    if (!currentFile || !previewData) return;

    setIsUploading(true);
    setShowManualMapping(false);

    try {
      const formData = new FormData();
      formData.append('file', currentFile);
      formData.append('columnMapping', JSON.stringify(mapping));

      const response = await fetch('/api/upload', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.message || 'Upload failed');
      }

      const result = await response.json();
      
      const successData = {
        processedRows: result.processedRows,
        cancelledRows: result.cancelledRows
      };

      setUploadSuccess(successData);
      onUploadSuccess(successData);

      toast({
        title: "Upload successful",
        description: `Processed ${result.processedRows} rows, removed ${result.cancelledRows} cancelled orders.`,
      });

    } catch (error) {
      console.error('Upload error:', error);
      const errorMessage = error instanceof Error ? error.message : "An error occurred during upload.";
      setUploadError(errorMessage);
      
      toast({
        title: "Upload failed",
        description: errorMessage,
        variant: "destructive",
      });
    } finally {
      setIsUploading(false);
      if (fileInputRef.current) {
        fileInputRef.current.value = '';
      }
    }
  };

  const handleDrag = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    console.log('Drag event:', e.type);
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    console.log('Files dropped:', e.dataTransfer.files.length);
    handleFiles(e.dataTransfer.files);
  };

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    handleFiles(e.target.files);
  };

  const handleCancelMapping = () => {
    setShowManualMapping(false);
    setPreviewData(null);
    setCurrentFile(null);
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  // Show manual mapping interface
  if (showManualMapping && previewData) {
    return (
      <ManualMapping
        previewData={previewData}
        onMapping={handleManualMapping}
        onCancel={handleCancelMapping}
        isUploading={isUploading}
      />
    );
  }

  return (
    <>
      <UploadProgress 
        isUploading={isUploading} 
        uploadSuccess={uploadSuccess} 
        uploadError={uploadError} 
      />
      <Card className="mb-4 md:mb-6 shadow-lg border-0 mx-2 md:mx-0">
      <CardHeader className="bg-gradient-to-r from-green-50 to-blue-50 rounded-t-xl p-4 md:p-6">
        <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-3 sm:gap-0">
          <div className="flex-1">
            <CardTitle className="flex items-center gap-2 text-lg md:text-xl" data-testid="text-upload-title">
              <FileText className="h-4 w-4 md:h-5 md:w-5 text-green-600" />
              Data Upload
            </CardTitle>
            <CardDescription className="text-sm md:text-base">
              Excel/CSV files upload करें payout processing के लिए
            </CardDescription>
          </div>
          <Button variant="outline" size="sm" data-testid="button-download-template" className="hover:bg-green-50 hover:text-green-600 text-xs md:text-sm w-full sm:w-auto">
            <Database className="mr-1 md:mr-2 h-3 w-3 md:h-4 md:w-4" />
            Template
          </Button>
        </div>
      </CardHeader>
      <CardContent className="p-4 md:p-6">
        <div
          className={`border-2 border-dashed rounded-xl text-center transition-all duration-300 ${
            isUploading 
              ? 'border-green-400 bg-green-50 p-6 md:p-8 shadow-inner' 
              : dragActive 
              ? 'border-green-400 bg-green-50 p-6 shadow-lg scale-105' 
              : 'border-gray-300 hover:border-green-400 hover:bg-green-50/50 p-6'
          }`}
          onDragEnter={handleDrag}
          onDragLeave={handleDrag}
          onDragOver={handleDrag}
          onDrop={handleDrop}
          data-testid="dropzone-upload"
        >
          {isUploading ? (
            <div className="flex flex-col items-center">
              <div className="animate-spin mx-auto mb-4">
                <Sparkles className="w-12 h-12 text-green-500" />
              </div>
              <div className="text-center">
                <div className="text-xl font-semibold text-green-700 mb-2">आपकी file process हो रही है...</div>
                <div className="text-sm text-gray-600 mb-2">बड़ी files के लिए कुछ minutes लग सकते हैं</div>
                <div className="text-xs text-gray-500">कृपया इस page को बंद न करें</div>
              </div>
            </div>
          ) : (
            <div className="flex flex-col items-center space-y-4">
              <Upload className="mx-auto h-16 w-16 text-gray-400" />
              <div className="text-center">
                <div className="text-lg font-semibold text-gray-700 mb-2">Files को यहाँ drop करें</div>
                <div className="text-sm text-gray-500">या click करके select करें</div>
              </div>
            </div>
          )}
          
          {!isUploading && (
            <div className="text-sm text-gray-600">
              <label 
                htmlFor="file-upload" 
                className="relative cursor-pointer font-medium text-primary hover:text-primary/80"
                data-testid="label-file-upload"
              >
                <span>Upload a file</span>
                <input
                  id="file-upload"
                  name="file-upload"
                  type="file"
                  className="sr-only"
                  accept=".xlsx,.xls,.csv"
                  onChange={handleInputChange}
                  disabled={isUploading}
                  ref={fileInputRef}
                  data-testid="input-file-upload"
                />
              </label>
              <p className="pl-1">or drag and drop</p>
            </div>
          )}
          {!isUploading && (
            <p className="text-xs text-gray-500 mt-2">
              Excel or CSV up to 200MB
            </p>
          )}
        </div>
      </CardContent>
    </Card>
    </>
  );
}