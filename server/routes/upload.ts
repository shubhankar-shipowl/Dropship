import type { Express } from "express";
import multer from "multer";
import * as XLSX from "xlsx";
import csv from "csv-parser";
import { Readable } from "stream";
import { storage } from "../storage";
import type { InsertOrderData } from "@shared/schema";

// Enhanced multer configuration for PM2 and cross-device compatibility
const upload = multer({ 
  storage: multer.memoryStorage(),
  limits: { 
    fileSize: 200 * 1024 * 1024, // 200MB limit
    fieldSize: 10 * 1024 * 1024,  // 10MB field size
    files: 1 // Single file only
  },
  fileFilter: (req, file, cb) => {
    // Validate file types for security
    const allowedTypes = [
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'application/vnd.ms-excel',
      'text/csv',
      'application/csv'
    ];
    
    const allowedExtensions = ['.xlsx', '.xls', '.csv'];
    const fileExtension = file.originalname.toLowerCase().slice(file.originalname.lastIndexOf('.'));
    
    if (allowedTypes.includes(file.mimetype) || allowedExtensions.includes(fileExtension)) {
      cb(null, true);
    } else {
      cb(new Error('Invalid file type. Only Excel and CSV files are allowed.'));
    }
  }
});

// Column mapping for case-insensitive auto-detection
const COLUMN_MAPPINGS = {
  dropshipperEmail: ['dropshipper email', 'order account', 'account', 'email'],
  orderId: ['order id', 'orderid', 'channel order number', 'ref', 'invoice #', 'invoice number'],
  orderDate: ['order date', 'channel order date', 'date'],
  waybill: ['waybill', 'wayball number', 'tracking number', 'awb'],
  productName: ['product name', 'product', 'item name'],
  sku: ['sku', 'client order id', 'product code'],
  qty: ['product qty', 'qty', 'quantity'],
  productValue: ['product value', 'productvalue', 'product_value', 'cod amount', 'cod', 'amount', 'total', 'order total', 'order amount', 'cod amt', 'customer amount', 'payment amount', 'final amount', 'bill amount'],
  mode: ['mode', 'payment mode', 'payment type', 'order mode', 'type', 'cod/prepaid'],
  status: ['status', 'order status'],
  deliveredDate: ['delivered date', 'delivery date'],
  rtsDate: ['rts date', 'return date'],
  shippingProvider: ['fulfilled by', 'courier company', 'shipping provider', 'provider']
};

function mapColumns(headers: string[]): Record<string, number> {
  const mapping: Record<string, number> = {};
  
  for (const [field, variations] of Object.entries(COLUMN_MAPPINGS)) {
    // First try exact matches (for better priority)
    for (let i = 0; i < headers.length; i++) {
      const header = headers[i].toLowerCase().trim();
      if (variations.some(variation => header === variation)) {
        mapping[field] = i;
        break;
      }
    }
    
    // If no exact match, try contains match
    if (!(field in mapping)) {
      for (let i = 0; i < headers.length; i++) {
        const header = headers[i].toLowerCase().trim();
        if (variations.some(variation => header.includes(variation))) {
          mapping[field] = i;
          break;
        }
      }
    }
  }
  
  return mapping;
}

function parseDate(dateStr: string): Date | null {
  if (!dateStr || dateStr.trim() === '') return null;
  
  // Handle multiple date formats to avoid parsing errors
  const cleanDateStr = String(dateStr).trim();
  
  // Try different date formats commonly used in Excel
  const formats = [
    cleanDateStr, // Direct parse
    cleanDateStr.replace(/(\d+)-(\d+)-(\d+)/, '$3-$2-$1'), // DD-MM-YYYY to YYYY-MM-DD  
    cleanDateStr.replace(/(\d+)\/(\d+)\/(\d+)/, '$3-$2-$1'), // DD/MM/YYYY to YYYY-MM-DD
  ];
  
  for (const format of formats) {
    const date = new Date(format);
    if (!isNaN(date.getTime()) && date.getFullYear() > 1900) {
      return date;
    }
  }
  
  // If all parsing fails, log for debugging but return null
  console.log(`Failed to parse date: "${dateStr}"`);
  return null;
}

function generateProductUid(sku: string | null, productName: string, dropshipperEmail: string): string {
  return `${dropshipperEmail}${productName.trim()}`;
}

export function registerUploadRoutes(app: Express): void {
  // Enhanced error handling middleware
  const handleUploadError = (err: any, req: any, res: any, next: any) => {
    console.error('Upload error:', err);
    if (err instanceof multer.MulterError) {
      if (err.code === 'LIMIT_FILE_SIZE') {
        return res.status(400).json({ message: 'File too large. Maximum size is 200MB.' });
      }
      if (err.code === 'LIMIT_UNEXPECTED_FILE') {
        return res.status(400).json({ message: 'Unexpected file field. Please upload only one file.' });
      }
    }
    if (err.message.includes('Invalid file type')) {
      return res.status(400).json({ message: err.message });
    }
    return res.status(500).json({ message: 'File upload failed. Please try again.' });
  };

  // Preview file headers for manual mapping
  app.post('/api/preview-file', upload.single('file'), handleUploadError, async (req, res) => {
    try {
      console.log('Preview API called with file:', req.file?.originalname);
      if (!req.file) {
        return res.status(400).json({ message: 'No file uploaded' });
      }

      const { originalname, buffer, mimetype } = req.file;
      console.log('File details - Name:', originalname, 'Type:', mimetype, 'Size:', buffer.length);
      let data: any[][] = [];

      // Parse file based on type (same logic as upload)
      if (mimetype.includes('excel') || originalname.endsWith('.xlsx') || originalname.endsWith('.xls')) {
        console.log('Processing Excel file...');
        const workbook = XLSX.read(buffer, { type: 'buffer' });
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        data = XLSX.utils.sheet_to_json(worksheet, { header: 1 });
        console.log('Excel data rows:', data.length);
      } else if (mimetype.includes('csv') || originalname.endsWith('.csv')) {
        console.log('Processing CSV file...');
        const csvData: any[] = [];
        const stream = Readable.from(buffer.toString());
        
        await new Promise((resolve, reject) => {
          stream
            .pipe(csv())
            .on('data', (row) => csvData.push(Object.values(row)))
            .on('end', resolve)
            .on('error', reject);
        });
        
        data = csvData;
        console.log('CSV data rows:', data.length);
      } else {
        console.log('Unsupported file type:', mimetype, originalname);
        return res.status(400).json({ message: 'Unsupported file type' });
      }

      if (data.length < 1) {
        return res.status(400).json({ message: 'File must contain headers' });
      }

      const headers = data[0].map((h: any) => String(h || ''));
      const sampleRows = data.slice(1, Math.min(4, data.length)); // First 3 data rows as sample

      // Auto-suggest mapping based on existing logic
      const autoMapping = mapColumns(headers);

      res.json({
        filename: originalname,
        headers,
        sampleRows,
        autoMapping,
        totalRows: data.length - 1,
        requiredFields: ['dropshipperEmail', 'orderId', 'orderDate', 'productName', 'qty', 'productValue', 'status', 'shippingProvider']
      });

    } catch (error) {
      console.error('Preview error:', error);
      res.status(500).json({ message: 'Error previewing file' });
    }
  });

  // File upload and processing with manual mapping - Enhanced for PM2 and cross-device
  app.post('/api/upload', upload.single('file'), handleUploadError, async (req, res) => {
    try {
      if (!req.file) {
        return res.status(400).json({ message: 'No file uploaded' });
      }

      const { originalname, buffer, mimetype } = req.file;
      const manualMapping = req.body.columnMapping ? JSON.parse(req.body.columnMapping) : null;
      let data: any[][] = [];

      // Parse file based on type
      if (mimetype.includes('excel') || originalname.endsWith('.xlsx') || originalname.endsWith('.xls')) {
        const workbook = XLSX.read(buffer, { type: 'buffer' });
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        data = XLSX.utils.sheet_to_json(worksheet, { header: 1 });
      } else if (mimetype.includes('csv') || originalname.endsWith('.csv')) {
        const csvData: any[] = [];
        const stream = Readable.from(buffer.toString());
        
        await new Promise((resolve, reject) => {
          stream
            .pipe(csv())
            .on('data', (row) => csvData.push(Object.values(row)))
            .on('end', resolve)
            .on('error', reject);
        });
        
        data = csvData;
      } else {
        return res.status(400).json({ message: 'Unsupported file type' });
      }

      if (data.length < 2) {
        return res.status(400).json({ message: 'File must contain headers and at least one data row' });
      }

      const headers = data[0].map((h: any) => String(h || ''));
      
      // Use manual mapping if provided, otherwise fall back to auto-mapping
      let columnMapping: Record<string, number>;
      if (manualMapping) {
        columnMapping = manualMapping;
        console.log('Using manual column mapping:', columnMapping);
      } else {
        columnMapping = mapColumns(headers);
        console.log('Using auto-detected column mapping:', columnMapping);
      }
      
      // Validate required columns
      const requiredFields = ['dropshipperEmail', 'orderId', 'orderDate', 'productName', 'qty', 'productValue', 'status', 'shippingProvider'];
      const missingFields = requiredFields.filter(field => !(field in columnMapping) || columnMapping[field] === -1);
      
      if (missingFields.length > 0) {
        return res.status(400).json({ 
          message: `Missing required columns: ${missingFields.join(', ')}`,
          availableColumns: headers,
          mappedColumns: columnMapping,
          requiredFields
        });
      }

      // Process data rows with optimization for speed
      const orders: InsertOrderData[] = [];
      let cancelledCount = 0;
      let insertPromises: Promise<void>[] = [];

      // Create upload session
      const uploadSession = await storage.createUploadSession({
        filename: originalname,
        totalRows: data.length - 1,
        processedRows: 0,
        cancelledRows: 0
      });

      console.log(`Processing ${data.length - 1} rows from Excel file...`);
      
      // Process in large chunks for optimization
      const processChunkSize = 25000;
      let processedCount = 0;
      
      for (let chunkStart = 1; chunkStart < data.length; chunkStart += processChunkSize) {
        const chunkEnd = Math.min(chunkStart + processChunkSize, data.length);
        console.log(`Processing chunk ${Math.floor(chunkStart/processChunkSize) + 1}/${Math.ceil((data.length-1)/processChunkSize)}: rows ${chunkStart} to ${chunkEnd-1}`);
        
        for (let i = chunkStart; i < chunkEnd; i++) {
          const row = data[i];
          
          try {
            const status = String(row[columnMapping.status] || '').trim();
            
            // Count cancelled orders but don't skip them - store all data exactly as in Excel
            if (status.toLowerCase().includes('cancelled')) {
              cancelledCount++;
            }

            // Parse all data exactly as in Excel
            const dropshipperEmail = String(row[columnMapping.dropshipperEmail] || '').trim() || '';
            const orderId = String(row[columnMapping.orderId] || '').trim() || '';
            const productName = String(row[columnMapping.productName] || '').trim() || '';
            const sku = row[columnMapping.sku] ? String(row[columnMapping.sku]).trim() : null;
            const qty = parseInt(String(row[columnMapping.qty] || '0')) || 0;
            
            // Parse Product Value exactly as in Excel
            const productValueStr = String(row[columnMapping.productValue] || '').trim();
            let productValue = 0;
            if (productValueStr && productValueStr !== '') {
              productValue = parseFloat(productValueStr) || 0;
            }
            
            const shippingProvider = String(row[columnMapping.shippingProvider] || '').trim() || '';
            const mode = columnMapping.mode ? String(row[columnMapping.mode] || '').trim() || null : null;
            
            // Handle dates
            const orderDate = parseDate(String(row[columnMapping.orderDate] || '')) || new Date();
            const deliveredDate = columnMapping.deliveredDate ? parseDate(String(row[columnMapping.deliveredDate] || '')) : null;
            const rtsDate = columnMapping.rtsDate ? parseDate(String(row[columnMapping.rtsDate] || '')) : null;
            const waybill = columnMapping.waybill ? String(row[columnMapping.waybill] || '').trim() || null : null;

            const productUid = generateProductUid(sku, productName, dropshipperEmail);

            orders.push({
              uploadSessionId: uploadSession.id,
              dropshipperEmail,
              orderId,
              orderDate,
              waybill,
              productName,
              sku,
              productUid,
              qty,
              productValue: productValue.toString(),
              mode,
              status,
              deliveredDate,
              rtsDate,
              shippingProvider
            });
            processedCount++;
          } catch (error) {
            console.error(`Error processing row ${i}:`, error);
            // Store error record to preserve row count
            orders.push({
              uploadSessionId: uploadSession.id,
              dropshipperEmail: 'ERROR_ROW',
              orderId: `ERROR_${i}`,
              orderDate: new Date(),
              waybill: null,
              productName: 'ERROR_PROCESSING',
              sku: null,
              productUid: `ERROR_${i}`,
              qty: 0,
              productValue: '0',
              mode: null,
              status: 'ERROR',
              deliveredDate: null,
              rtsDate: null,
              shippingProvider: 'ERROR'
            });
            processedCount++;
          }
        }
        
        // Insert batch frequently for performance
        if (orders.length >= 1000) {
          console.log(`Inserting intermediate batch of ${orders.length} orders...`);
          
          const batchToInsert = [...orders];
          orders.length = 0;
          
          const insertPromise = storage.insertOrderData(batchToInsert);
          insertPromises.push(insertPromise);
          
          // Limit concurrent insertions
          if (insertPromises.length >= 3) {
            await Promise.all(insertPromises);
            insertPromises.length = 0;
          }
        }
      }

      // Wait for any pending insertions
      if (insertPromises.length > 0) {
        console.log(`Waiting for ${insertPromises.length} parallel insertions to complete...`);
        await Promise.all(insertPromises);
      }
      
      // Insert remaining order data
      if (orders.length > 0) {
        console.log(`Inserting final batch of ${orders.length} orders...`);
        await storage.insertOrderData(orders);
      }
      
      console.log(`Total processed: ${processedCount} orders, cancelled: ${cancelledCount}`);

      // Update upload session with final counts
      await storage.updateUploadSession(uploadSession.id, {
        processedRows: processedCount,
        cancelledRows: cancelledCount
      });

      res.json({
        uploadSessionId: uploadSession.id,
        totalRows: data.length - 1,
        processedRows: processedCount,
        cancelledRows: cancelledCount,
        message: `Successfully processed ${processedCount} orders from Excel (${cancelledCount} cancelled orders included)`
      });

    } catch (error) {
      console.error('Upload error:', error);
      res.status(500).json({ message: 'Error processing file' });
    }
  });

  // Reset data endpoint
  app.post('/api/reset-data', async (req, res) => {
    try {
      await storage.resetAllData();
      res.json({ 
        message: 'Data reset completed',
        cleared: ['orders', 'upload-sessions', 'payout-logs', 'rts-rto-reconciliation']
      });
    } catch (error) {
      console.error('Error resetting data:', error);
      res.status(500).json({ message: 'Error resetting data' });
    }
  });
}