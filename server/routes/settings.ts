import type { Express } from "express";
import multer from "multer";
import * as XLSX from "xlsx";
import { storage } from "../storage";
import { insertProductPriceSchema, insertShippingRateSchema } from "@shared/schema";

const upload = multer({ 
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 } // 10MB limit for settings files
});

export function registerSettingsRoutes(app: Express): void {
  // Product Prices CRUD
  app.get('/api/product-prices', async (req, res) => {
    try {
      const prices = await storage.getProductPrices();
      res.json(prices);
    } catch (error) {
      console.error('Error fetching product prices:', error);
      res.status(500).json({ message: 'Error fetching product prices' });
    }
  });

  app.post('/api/product-prices', async (req, res) => {
    try {
      const priceData = insertProductPriceSchema.parse(req.body);
      const result = await storage.upsertProductPrice(priceData);
      res.json(result);
    } catch (error) {
      console.error('Error creating/updating product price:', error);
      res.status(500).json({ message: 'Error creating/updating product price' });
    }
  });

  app.delete('/api/product-prices/:id', async (req, res) => {
    try {
      const { id } = req.params;
      await storage.deleteProductPrice(id);
      res.json({ message: 'Product price deleted successfully' });
    } catch (error) {
      console.error('Error deleting product price:', error);
      res.status(500).json({ message: 'Error deleting product price' });
    }
  });

  // Bulk upload product prices
  app.post('/api/product-prices/bulk-upload', upload.single('file'), async (req, res) => {
    try {
      if (!req.file) {
        return res.status(400).json({ message: 'No file uploaded' });
      }

      const { buffer, mimetype, originalname } = req.file;
      let data: any[][] = [];

      if (mimetype.includes('excel') || originalname.endsWith('.xlsx') || originalname.endsWith('.xls')) {
        const workbook = XLSX.read(buffer, { type: 'buffer' });
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        data = XLSX.utils.sheet_to_json(worksheet, { header: 1 });
      } else {
        return res.status(400).json({ message: 'Please upload an Excel file' });
      }

      if (data.length < 2) {
        return res.status(400).json({ message: 'File must contain headers and data' });
      }

      const headers = data[0].map((h: any) => String(h || '').toLowerCase());
      const requiredHeaders = ['dropshipper email', 'product uid', 'product name', 'product cost per unit'];
      
      const headerMapping: Record<string, number> = {};
      requiredHeaders.forEach(reqHeader => {
        const index = headers.findIndex(h => h.includes(reqHeader.toLowerCase()) || reqHeader.toLowerCase().includes(h));
        if (index === -1) {
          throw new Error(`Required header "${reqHeader}" not found`);
        }
        headerMapping[reqHeader] = index;
      });

      const prices = [];
      for (let i = 1; i < data.length; i++) {
        const row = data[i];
        if (!row || row.length === 0) continue;

        const priceData = {
          dropshipperEmail: String(row[headerMapping['dropshipper email']] || '').trim(),
          productUid: String(row[headerMapping['product uid']] || '').trim(),
          productName: String(row[headerMapping['product name']] || '').trim(),
          productCostPerUnit: String(parseFloat(String(row[headerMapping['product cost per unit']] || '0'))),
          sku: null,
          productWeight: '0.5', // Default weight
          currency: 'INR'
        };

        if (priceData.dropshipperEmail && priceData.productUid && priceData.productName) {
          prices.push(priceData);
        }
      }

      await storage.bulkUpsertProductPrices(prices);
      res.json({ 
        message: `Successfully uploaded ${prices.length} product prices`,
        count: prices.length 
      });

    } catch (error) {
      console.error('Error bulk uploading product prices:', error);
      res.status(500).json({ message: 'Error uploading product prices' });
    }
  });

  // Shipping Rates CRUD
  app.get('/api/shipping-rates', async (req, res) => {
    try {
      const rates = await storage.getShippingRates();
      res.json(rates);
    } catch (error) {
      console.error('Error fetching shipping rates:', error);
      res.status(500).json({ message: 'Error fetching shipping rates' });
    }
  });

  app.post('/api/shipping-rates', async (req, res) => {
    try {
      const rateData = insertShippingRateSchema.parse(req.body);
      const result = await storage.upsertShippingRate(rateData);
      res.json(result);
    } catch (error) {
      console.error('Error creating/updating shipping rate:', error);
      res.status(500).json({ message: 'Error creating/updating shipping rate' });
    }
  });

  app.delete('/api/shipping-rates/:id', async (req, res) => {
    try {
      const { id } = req.params;
      await storage.deleteShippingRate(id);
      res.json({ message: 'Shipping rate deleted successfully' });
    } catch (error) {
      console.error('Error deleting shipping rate:', error);
      res.status(500).json({ message: 'Error deleting shipping rate' });
    }
  });

  // Bulk upload shipping rates
  app.post('/api/shipping-rates/bulk-upload', upload.single('file'), async (req, res) => {
    try {
      if (!req.file) {
        return res.status(400).json({ message: 'No file uploaded' });
      }

      const { buffer, mimetype, originalname } = req.file;
      let data: any[][] = [];

      if (mimetype.includes('excel') || originalname.endsWith('.xlsx') || originalname.endsWith('.xls')) {
        const workbook = XLSX.read(buffer, { type: 'buffer' });
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        data = XLSX.utils.sheet_to_json(worksheet, { header: 1 });
      } else {
        return res.status(400).json({ message: 'Please upload an Excel file' });
      }

      if (data.length < 2) {
        return res.status(400).json({ message: 'File must contain headers and data' });
      }

      const headers = data[0].map((h: any) => String(h || '').toLowerCase());
      const requiredHeaders = ['product uid', 'product weight', 'shipping provider', 'shipping rate per kg'];
      
      const headerMapping: Record<string, number> = {};
      requiredHeaders.forEach(reqHeader => {
        const index = headers.findIndex(h => h.includes(reqHeader.toLowerCase()) || reqHeader.toLowerCase().includes(h));
        if (index === -1) {
          throw new Error(`Required header "${reqHeader}" not found`);
        }
        headerMapping[reqHeader] = index;
      });

      const rates = [];
      for (let i = 1; i < data.length; i++) {
        const row = data[i];
        if (!row || row.length === 0) continue;

        const rateData = {
          productUid: String(row[headerMapping['product uid']] || '').trim(),
          productWeight: String(parseFloat(String(row[headerMapping['product weight']] || '0.5'))),
          shippingProvider: String(row[headerMapping['shipping provider']] || '').trim(),
          shippingRatePerKg: String(parseFloat(String(row[headerMapping['shipping rate per kg']] || '0'))),
          currency: 'INR'
        };

        if (rateData.productUid && rateData.shippingProvider) {
          rates.push(rateData);
        }
      }

      await storage.bulkUpsertShippingRates(rates);
      res.json({ 
        message: `Successfully uploaded ${rates.length} shipping rates`,
        count: rates.length 
      });

    } catch (error) {
      console.error('Error bulk uploading shipping rates:', error);
      res.status(500).json({ message: 'Error uploading shipping rates' });
    }
  });

  // Import settings (combined product prices and shipping rates)
  app.post('/api/import-settings', upload.single('file'), async (req, res) => {
    try {
      if (!req.file) {
        return res.status(400).json({ message: 'No file uploaded' });
      }

      const { buffer, mimetype, originalname } = req.file;
      
      if (!mimetype.includes('excel') && !originalname.endsWith('.xlsx') && !originalname.endsWith('.xls')) {
        return res.status(400).json({ message: 'Please upload an Excel file' });
      }

      const workbook = XLSX.read(buffer, { type: 'buffer' });
      let importedPrices = 0;
      let importedRates = 0;

      // Process Sheet 1: Product Prices
      const pricesSheetName = workbook.SheetNames.find(name => 
        name.toLowerCase().includes('product') || name.toLowerCase().includes('price') || name === workbook.SheetNames[0]
      );
      
      if (pricesSheetName) {
        try {
          const pricesWorksheet = workbook.Sheets[pricesSheetName];
          const pricesData: any[][] = XLSX.utils.sheet_to_json(pricesWorksheet, { header: 1 });
          
          if (pricesData.length >= 2) {
            const pricesHeaders = (pricesData[0] || []).map((h: any) => String(h || '').toLowerCase());
            const requiredPriceHeaders = ['dropshipper email', 'product uid', 'product name', 'product cost per unit'];
            
            const priceHeaderMapping: Record<string, number> = {};
            let allPriceHeadersFound = true;
            
            requiredPriceHeaders.forEach(reqHeader => {
              const index = pricesHeaders.findIndex((h: string) => h.includes(reqHeader.toLowerCase()) || reqHeader.toLowerCase().includes(h));
              if (index === -1) {
                allPriceHeadersFound = false;
              } else {
                priceHeaderMapping[reqHeader] = index;
              }
            });

            if (allPriceHeadersFound) {
              const prices = [];
              for (let i = 1; i < pricesData.length; i++) {
                const row = pricesData[i] || [];
                if (!row || row.length === 0) continue;

                const priceData = {
                  dropshipperEmail: String(row[priceHeaderMapping['dropshipper email']] || '').trim(),
                  productUid: String(row[priceHeaderMapping['product uid']] || '').trim(),
                  productName: String(row[priceHeaderMapping['product name']] || '').trim(),
                  productCostPerUnit: String(parseFloat(String(row[priceHeaderMapping['product cost per unit']] || '0'))),
                  sku: null,
                  productWeight: '0.5', // Default weight
                  currency: 'INR'
                };

                if (priceData.dropshipperEmail && priceData.productUid && priceData.productName) {
                  prices.push(priceData);
                }
              }

              if (prices.length > 0) {
                await storage.bulkUpsertProductPrices(prices);
                importedPrices = prices.length;
              }
            }
          }
        } catch (error) {
          console.error('Error processing product prices sheet:', error);
        }
      }

      // Process Sheet 2: Shipping Rates
      const ratesSheetName = workbook.SheetNames.find(name => 
        name.toLowerCase().includes('shipping') || name.toLowerCase().includes('rate') || 
        (workbook.SheetNames.length > 1 && name === workbook.SheetNames[1])
      );
      
      if (ratesSheetName) {
        try {
          const ratesWorksheet = workbook.Sheets[ratesSheetName];
          const ratesData: any[][] = XLSX.utils.sheet_to_json(ratesWorksheet, { header: 1 });
          
          if (ratesData.length >= 2) {
            const ratesHeaders = (ratesData[0] || []).map((h: any) => String(h || '').toLowerCase());
            const requiredRateHeaders = ['product uid', 'product weight', 'shipping provider', 'shipping rate per kg'];
            
            const rateHeaderMapping: Record<string, number> = {};
            let allRateHeadersFound = true;
            
            requiredRateHeaders.forEach(reqHeader => {
              const index = ratesHeaders.findIndex((h: string) => h.includes(reqHeader.toLowerCase()) || reqHeader.toLowerCase().includes(h));
              if (index === -1) {
                allRateHeadersFound = false;
              } else {
                rateHeaderMapping[reqHeader] = index;
              }
            });

            if (allRateHeadersFound) {
              const rates = [];
              for (let i = 1; i < ratesData.length; i++) {
                const row = ratesData[i] || [];
                if (!row || row.length === 0) continue;

                const rateData = {
                  productUid: String(row[rateHeaderMapping['product uid']] || '').trim(),
                  productWeight: String(parseFloat(String(row[rateHeaderMapping['product weight']] || '0.5'))),
                  shippingProvider: String(row[rateHeaderMapping['shipping provider']] || '').trim(),
                  shippingRatePerKg: String(parseFloat(String(row[rateHeaderMapping['shipping rate per kg']] || '0'))),
                  currency: 'INR'
                };

                if (rateData.productUid && rateData.shippingProvider) {
                  rates.push(rateData);
                }
              }

              if (rates.length > 0) {
                await storage.bulkUpsertShippingRates(rates);
                importedRates = rates.length;
              }
            }
          }
        } catch (error) {
          console.error('Error processing shipping rates sheet:', error);
        }
      }

      if (importedPrices === 0 && importedRates === 0) {
        return res.status(400).json({ 
          message: 'Could not import any data. Please check the file format and ensure you have the correct headers.' 
        });
      }

      res.json({ 
        message: `Successfully imported ${importedPrices} product prices and ${importedRates} shipping rates`,
        importedPrices,
        importedRates
      });

    } catch (error) {
      console.error('Error importing settings:', error);
      res.status(500).json({ message: 'Error importing settings. Please check the file format.' });
    }
  });

  // Download templates
  app.get('/api/download-product-prices-template', async (req, res) => {
    try {
      const workbook = XLSX.utils.book_new();
      const templateData = [
        ['Dropshipper Email', 'Product UID', 'Product Name', 'SKU', 'Product Weight (kg)', 'Product Cost Per Unit', 'Currency'],
        ['user@example.com', 'PROD001', 'Sample Product', 'SKU001', '0.5', '100', 'INR']
      ];
      
      const worksheet = XLSX.utils.aoa_to_sheet(templateData);
      XLSX.utils.book_append_sheet(workbook, worksheet, 'Product Prices');
      
      const buffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
      
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', 'attachment; filename=product_prices_template.xlsx');
      res.send(buffer);
    } catch (error) {
      console.error('Error creating product prices template:', error);
      res.status(500).json({ message: 'Error creating template' });
    }
  });

  app.get('/api/download-shipping-rates-template', async (req, res) => {
    try {
      const workbook = XLSX.utils.book_new();
      const templateData = [
        ['Product UID', 'Product Weight (kg)', 'Shipping Provider', 'Shipping Rate Per Kg', 'Currency'],
        ['PROD001', '0.5', 'Delhivery', '25', 'INR']
      ];
      
      const worksheet = XLSX.utils.aoa_to_sheet(templateData);
      XLSX.utils.book_append_sheet(workbook, worksheet, 'Shipping Rates');
      
      const buffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
      
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', 'attachment; filename=shipping_rates_template.xlsx');
      res.send(buffer);
    } catch (error) {
      console.error('Error creating shipping rates template:', error);
      res.status(500).json({ message: 'Error creating template' });
    }
  });
}