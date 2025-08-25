import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { CheckCircle, XCircle, Upload } from "lucide-react";

interface PreviewData {
  filename: string;
  headers: string[];
  sampleRows: any[][];
  autoMapping: Record<string, number>;
  totalRows: number;
  requiredFields: string[];
}

interface ManualMappingProps {
  previewData: PreviewData;
  onMapping: (mapping: Record<string, number>) => void;
  onCancel: () => void;
  isUploading?: boolean;
}

const FIELD_LABELS = {
  dropshipperEmail: 'Dropshipper Email',
  orderId: 'Order ID', 
  orderDate: 'Order Date',
  productName: 'Product Name',
  qty: 'Quantity',
  productValue: 'Product Value',
  mode: 'Payment Mode',
  status: 'Status',
  shippingProvider: 'Shipping Provider',
  waybill: 'Waybill/Tracking',
  sku: 'SKU',
  deliveredDate: 'Delivered Date',
  rtsDate: 'RTS Date'
};

export default function ManualMapping({ previewData, onMapping, onCancel, isUploading }: ManualMappingProps) {
  const [mapping, setMapping] = useState<Record<string, number>>(previewData.autoMapping);

  const handleFieldChange = (field: string, columnIndex: string) => {
    const newMapping = { ...mapping };
    if (columnIndex === "-1") {
      delete newMapping[field];
    } else {
      newMapping[field] = parseInt(columnIndex);
    }
    setMapping(newMapping);
  };

  const isRequired = (field: string) => previewData.requiredFields.includes(field);
  const isMapped = (field: string) => field in mapping && mapping[field] !== -1;
  const canProceed = previewData.requiredFields.every(field => isMapped(field));

  const getColumnPreview = (columnIndex: number) => {
    const samples = previewData.sampleRows.map(row => row[columnIndex] || '').slice(0, 2);
    return samples.filter(s => s).join(', ') || 'No data';
  };

  return (
    <div className="max-w-4xl mx-auto p-6">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Upload className="h-5 w-5" />
            Manual Column Mapping
          </CardTitle>
          <CardDescription>
            File: <strong>{previewData.filename}</strong> ({previewData.totalRows} rows)
            <br />
            Map your Excel columns to the required fields. Red fields are required.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4">
            {Object.entries(FIELD_LABELS).map(([field, label]) => (
              <div key={field} className="grid grid-cols-3 gap-4 items-center">
                <div className="flex items-center gap-2">
                  <span className={`font-medium ${isRequired(field) ? 'text-red-600' : 'text-gray-600'}`}>
                    {label}
                  </span>
                  {isRequired(field) && <Badge variant="destructive" className="text-xs">Required</Badge>}
                  {isMapped(field) && <CheckCircle className="h-4 w-4 text-green-600" />}
                  {isRequired(field) && !isMapped(field) && <XCircle className="h-4 w-4 text-red-600" />}
                </div>
                
                <Select
                  value={isMapped(field) ? mapping[field].toString() : "-1"}
                  onValueChange={(value) => handleFieldChange(field, value)}
                  data-testid={`select-${field}`}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Select column..." />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="-1">-- None --</SelectItem>
                    {previewData.headers.map((header, index) => (
                      <SelectItem key={index} value={index.toString()}>
                        Column {index + 1}: {header}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                
                <div className="text-sm text-gray-500">
                  {isMapped(field) && (
                    <span>Preview: {getColumnPreview(mapping[field])}</span>
                  )}
                </div>
              </div>
            ))}
          </div>

          <div className="mt-6 pt-6 border-t">
            <h4 className="font-medium mb-3">Sample Data Preview</h4>
            <div className="overflow-x-auto">
              <table className="w-full border-collapse border border-gray-200 text-sm">
                <thead>
                  <tr className="bg-gray-50">
                    {previewData.headers.map((header, index) => (
                      <th key={index} className="border border-gray-200 px-2 py-1 text-left">
                        Col {index + 1}: {header}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {previewData.sampleRows.map((row, rowIndex) => (
                    <tr key={rowIndex}>
                      {row.map((cell, cellIndex) => (
                        <td key={cellIndex} className="border border-gray-200 px-2 py-1">
                          {String(cell || '')}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="flex gap-4 mt-6">
            <Button
              onClick={() => onMapping(mapping)}
              disabled={!canProceed || isUploading}
              className="flex-1"
              data-testid="button-proceed-mapping"
            >
              {isUploading ? 'Uploading...' : 'Proceed with Upload'}
            </Button>
            <Button
              variant="outline"
              onClick={onCancel}
              disabled={isUploading}
              data-testid="button-cancel-mapping"
            >
              Cancel
            </Button>
          </div>
          
          {!canProceed && (
            <p className="text-sm text-red-600 mt-2">
              Please map all required fields (marked in red) before proceeding.
            </p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}