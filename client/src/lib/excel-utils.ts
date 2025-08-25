import * as XLSX from 'xlsx';

export interface ExcelColumn {
  header: string;
  key: string;
  width?: number;
  type?: 'text' | 'number' | 'currency' | 'date';
}

export interface ExcelSheet {
  name: string;
  columns: ExcelColumn[];
  data: Record<string, any>[];
}

export function createWorkbook(sheets: ExcelSheet[]): XLSX.WorkBook {
  const workbook = XLSX.utils.book_new();

  sheets.forEach(sheet => {
    const worksheet = XLSX.utils.json_to_sheet(sheet.data, {
      header: sheet.columns.map(col => col.key)
    });

    // Set column headers
    const headerRow = sheet.columns.map(col => col.header);
    XLSX.utils.sheet_add_aoa(worksheet, [headerRow], { origin: "A1" });

    // Set column widths
    const colWidths = sheet.columns.map(col => ({
      width: col.width || 15
    }));
    worksheet['!cols'] = colWidths;

    // Add sheet to workbook
    XLSX.utils.book_append_sheet(workbook, worksheet, sheet.name);
  });

  return workbook;
}

export function downloadWorkbook(workbook: XLSX.WorkBook, filename: string) {
  const buffer = XLSX.write(workbook, { type: 'array', bookType: 'xlsx' });
  const blob = new Blob([buffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
  
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.style.display = 'none';
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  window.URL.revokeObjectURL(url);
  document.body.removeChild(a);
}

export function generateTemplateSheets(): ExcelSheet[] {
  return [
    {
      name: 'Price_Request',
      columns: [
        { header: 'dropshipper_email', key: 'dropshipperEmail', width: 25 },
        { header: 'product_uid', key: 'productUid', width: 20 },
        { header: 'product_name', key: 'productName', width: 30 },
        { header: 'sku', key: 'sku', width: 15 },
        { header: 'product_cost_per_unit', key: 'productCostPerUnit', type: 'currency', width: 20 },
        { header: 'currency', key: 'currency', width: 10 },
      ],
      data: []
    },
    {
      name: 'Shipping_Rate_Request',
      columns: [
        { header: 'shipping_provider', key: 'shippingProvider', width: 25 },
        { header: 'shipping_rate_per_order', key: 'shippingRatePerOrder', type: 'currency', width: 25 },
        { header: 'currency', key: 'currency', width: 10 },
      ],
      data: []
    }
  ];
}

export function formatCurrency(amount: number, currency: string = 'INR'): string {
  return new Intl.NumberFormat('en-IN', {
    style: 'currency',
    currency,
    maximumFractionDigits: 2,
  }).format(amount);
}

export function parseCSVFile(file: File): Promise<any[]> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const text = e.target?.result as string;
        const lines = text.split('\n');
        const headers = lines[0].split(',').map(h => h.trim());
        
        const data = lines.slice(1)
          .filter(line => line.trim() !== '')
          .map(line => {
            const values = line.split(',').map(v => v.trim());
            const row: Record<string, string> = {};
            headers.forEach((header, index) => {
              row[header] = values[index] || '';
            });
            return row;
          });
        
        resolve(data);
      } catch (error) {
        reject(error);
      }
    };
    reader.onerror = () => reject(new Error('Failed to read file'));
    reader.readAsText(file);
  });
}
