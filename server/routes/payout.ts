import type { Express } from "express";
import { storage } from "../storage";
import { z } from "zod";
import * as XLSX from 'xlsx';

export function registerPayoutRoutes(app: Express): void {
  // Get all dropshippers
  app.get('/api/dropshippers', async (req, res) => {
    try {
      const dropshippers = await storage.getUniqueDropshippers();
      res.json(dropshippers);
    } catch (error) {
      console.error('Error fetching dropshippers:', error);
      res.status(500).json({ message: 'Error fetching dropshippers' });
    }
  });

  // Get recommended date ranges for a dropshipper
  app.get('/api/dropshipper-date-ranges/:email', async (req, res) => {
    try {
      const { email } = req.params;
      const decodedEmail = decodeURIComponent(email);
      const dateRanges = await storage.getDropshipperDateRanges(decodedEmail);
      res.json(dateRanges);
    } catch (error) {
      console.error('Error fetching dropshipper date ranges:', error);
      res.status(500).json({ message: 'Error fetching dropshipper date ranges' });
    }
  });

  // Calculate payouts
  app.post('/api/calculate-payouts', async (req, res) => {
    try {
      const requestSchema = z.object({
        orderDateFrom: z.string(),
        orderDateTo: z.string(),
        deliveredDateFrom: z.string(),
        deliveredDateTo: z.string(),
        dropshipperEmail: z.string().optional()
      });

      const request = requestSchema.parse(req.body);
      console.log('🔄 CALCULATE PAYOUTS called with:', request);

      const result = await storage.calculatePayouts(request);
      res.json(result);
    } catch (error) {
      console.error('Error calculating payouts:', error);
      res.status(500).json({ message: 'Error calculating payouts' });
    }
  });

  // COD Breakdown for specific date range
  app.post('/api/cod-breakdown-details', async (req, res) => {
    try {
      const { deliveredDateFrom, deliveredDateTo, dropshipperEmail } = req.body;
      console.log(`📊 COD Breakdown requested: ${deliveredDateFrom} to ${deliveredDateTo} for ${dropshipperEmail || 'all'}`);
      
      const result = await storage.getCodBreakdownForRange({
        deliveredDateFrom,
        deliveredDateTo,
        dropshipperEmail
      });
      
      res.json(result);
    } catch (error) {
      console.error('Error getting COD breakdown:', error);
      res.status(500).json({ message: 'Error getting COD breakdown' });
    }
  });

  // Get missing data (prices and rates)
  app.get('/api/missing-data', async (req, res) => {
    try {
      const result = await storage.getMissingPricesAndRates();
      res.json(result);
    } catch (error) {
      console.error('Error fetching missing data:', error);
      res.status(500).json({ message: 'Error fetching missing data' });
    }
  });

  // Export payout data as Excel workbook
  app.post('/api/export-workbook', async (req, res) => {
    try {
      const requestSchema = z.object({
        orderDateFrom: z.string(),
        orderDateTo: z.string(),
        deliveredDateFrom: z.string(),
        deliveredDateTo: z.string(),
        dropshipperEmail: z.string().optional()
      });

      const request = requestSchema.parse(req.body);
      console.log('📊 EXPORT WORKBOOK called with:', request);

      // Get payout calculation data
      const payoutData = await storage.calculatePayouts(request);
      
      // Create workbook
      const workbook = XLSX.utils.book_new();

      // Summary sheet with comprehensive details (matching old format)
      const summaryData = [
        ['PAYOUT CALCULATION REPORT'],
        ['Generated on:', new Date().toLocaleString('en-IN')],
        [''],
        ['DROPSHIPPER DETAILS'],
        ['Dropshipper Email:', request.dropshipperEmail || 'All Dropshippers'],
        [''],
        ['DATE RANGES'],
        ['Order Date Range (for shipping costs):', `${request.orderDateFrom} to ${request.orderDateTo}`],
        ['Delivered Date Range (for COD/product costs):', `${request.deliveredDateFrom} to ${request.deliveredDateTo}`],
        [''],
        ['ORDER COUNTS'],
        ['Orders with Shipping Charges:', payoutData.summary.ordersWithShippingCharges || 0],
        ['Orders with Product Amount:', payoutData.summary.ordersWithProductAmount || 0],
        [''],
        ['FINANCIAL BREAKDOWN'],
        ['Metric', 'Amount (Rs.)', 'Description'],
        ['Total Shipping Charges', payoutData.summary.shippingTotal, 'Based on order date range, cancelled orders excluded'],
        ['Total COD Received', payoutData.summary.codTotal, 'From delivered orders in delivered date range'],
        ['Total Product Cost', payoutData.summary.productCostTotal, 'Product costs for delivered orders'],
        ['RTS/RTO Reversal', payoutData.summary.rtsRtoReversalTotal, 'Deductions for returned orders'],
        [''],
        ['FINAL PAYOUT', payoutData.summary.finalPayable, 'COD - Product Cost - Shipping - RTS/RTO'],
        [''],
        ['CALCULATION FORMULA'],
        ['Final Payout = COD Received - Product Costs - Shipping Charges - RTS/RTO Reversals'],
        [''],
        ['DATA INTEGRITY NOTES'],
        ['• COD amounts preserved exactly from Excel (no rounding)'],
        ['• Shipping costs calculated: Quantity × Weight × Rate per KG'],
        ['• Cancelled orders excluded from shipping calculations'],
        ['• Dual date ranges for accurate cost allocation']
      ];

      const summarySheet = XLSX.utils.aoa_to_sheet(summaryData);
      XLSX.utils.book_append_sheet(workbook, summarySheet, 'Summary');

      // Order Details sheet (matching old format)
      const orderHeaders = [
        'Order ID', 'Waybill', 'Product', 'SKU/UID', 'Dropshipper',
        'Order Date', 'Delivered Date', 'Shipped Qty', 'Delivered Qty',
        'COD Rate', 'COD Received', 'Shipping Cost', 'Product Cost',
        'Net Payable', 'Status', 'Shipping Provider', 'Weight (g)'
      ];

      const orderRows = [orderHeaders];

      // Filter rows that are within date ranges and have relevant data
      payoutData.rows.filter(row => {
        // Include if order has shipping cost (within order date range) OR COD/product cost (within delivered date range)
        return (row.shippingCost && row.shippingCost > 0) || (row.codReceived && row.codReceived > 0) || (row.productCost && row.productCost > 0);
      }).forEach((row) => {
        const orderDate = row.orderDate ? new Date(row.orderDate).toLocaleDateString('en-IN') : '';
        const deliveredDate = row.deliveredDate ? new Date(row.deliveredDate).toLocaleDateString('en-IN') : '';
        
        orderRows.push([
          row.orderId || '',
          row.waybill || '',
          row.productName || '',
          row.sku || row.productUid || '',
          row.dropshipperEmail || '',
          orderDate,
          deliveredDate,
          (row.qty || 0).toString(),
          (row.deliveredQty || 0).toString(),
          (row.codRate || 0).toString(),
          (row.codReceived || 0).toString(),
          (row.shippingCost || 0).toString(),
          (row.productCost || 0).toString(),
          (row.payable || 0).toString(),
          row.status || '',
          row.shippingProvider || '',
          (row.productWeight || 0).toString()
        ]);
      });

      const orderSheet = XLSX.utils.aoa_to_sheet(orderRows);
      XLSX.utils.book_append_sheet(workbook, orderSheet, 'Order Details');

      // Shipping Details sheet (matching old format)
      const shippingHeaders = [
        'Order ID', 'Waybill', 'Product', 'Dropshipper', 'Shipping Provider',
        'Order Date', 'Quantity', 'Total Weight (g)', 'Shipping Cost (Rs.)', 
        'Status', 'Included in Calculation'
      ];

      const shippingRows = [shippingHeaders];

      // Only include orders that have shipping costs (within order date range)
      payoutData.rows.filter(row => row.shippingCost && row.shippingCost > 0).forEach((row) => {
        const orderDate = row.orderDate ? new Date(row.orderDate).toLocaleDateString('en-IN') : '';
        const includedInCalc = !row.status?.toLowerCase().includes('cancelled') ? 'YES' : 'NO (Cancelled)';
        
        shippingRows.push([
          row.orderId || '',
          row.waybill || '',
          row.productName || '',
          row.dropshipperEmail || '',
          row.shippingProvider || '',
          orderDate,
          (row.qty || 0).toString(),
          (row.weight || 0).toString(),
          (row.shippingCost || 0).toString(),
          row.status || '',
          includedInCalc
        ]);
      });

      const shippingDetailsSheet = XLSX.utils.aoa_to_sheet(shippingRows);
      XLSX.utils.book_append_sheet(workbook, shippingDetailsSheet, 'Shipping Details');

      // COD Details sheet (matching old format)
      const codHeaders = [
        'Order ID', 'Waybill', 'Product', 'Dropshipper', 'Delivered Date',
        'Delivered Qty', 'COD Rate (Rs.)', 'Total COD Received (Rs.)', 
        'Status', 'Included in Calculation'
      ];

      const codRows = [codHeaders];

      // Only include orders with COD received (within delivered date range)
      payoutData.rows.filter(row => row.codReceived && row.codReceived > 0).forEach((row) => {
          const deliveredDate = row.deliveredDate ? new Date(row.deliveredDate).toLocaleDateString('en-IN') : '';
          const includedInCalc = row.status?.toLowerCase().includes('delivered') ? 'YES' : 'NO';
          
        codRows.push([
          row.orderId || '',
          row.waybill || '',
          row.productName || '',
          row.dropshipperEmail || '',
          deliveredDate,
          (row.deliveredQty || 0).toString(),
          (row.codRate || 0).toString(),
          (row.codReceived || 0).toString(),
          row.status || '',
          includedInCalc
        ]);
      });

      const codDetailsSheet = XLSX.utils.aoa_to_sheet(codRows);
      XLSX.utils.book_append_sheet(workbook, codDetailsSheet, 'COD Details');

      // Product Cost Details sheet (matching old format)
      const productCostHeaders = [
        'Order ID', 'Waybill', 'Product', 'SKU/UID', 'Dropshipper',
        'Delivered Qty', 'Product Cost per Unit (Rs.)', 'Total Product Cost (Rs.)',
        'Status', 'Cost Source'
      ];

      const productCostRows = [productCostHeaders];

      // Only include orders with product costs (within delivered date range)
      payoutData.rows.filter(row => row.productCost && row.productCost > 0).forEach((row) => {
        const costSource = (row.productCost || 0) > 0 ? 'Found in Database' : 'Default/Missing';
        
        productCostRows.push([
          row.orderId || '',
          row.waybill || '',
          row.productName || '',
          row.sku || row.productUid || '',
          row.dropshipperEmail || '',
          (row.deliveredQty || 0).toString(),
          (row.productCostPerUnit || 0).toString(),
          (row.productCost || 0).toString(),
          row.status || '',
          costSource
        ]);
      });

      const productCostDetailsSheet = XLSX.utils.aoa_to_sheet(productCostRows);
      XLSX.utils.book_append_sheet(workbook, productCostDetailsSheet, 'Product Cost Details');

      // Adjustments sheet (if any)
      if (payoutData.adjustments && payoutData.adjustments.length > 0) {
        const adjustmentHeaders = ['Order ID', 'Reason', 'Amount', 'Reference'];
        const adjustmentData = [adjustmentHeaders];
        
        payoutData.adjustments.forEach(adj => {
          adjustmentData.push([
            adj.orderId,
            adj.reason,
            `₹${adj.amount}`,
            adj.reference
          ]);
        });

        const adjustmentSheet = XLSX.utils.aoa_to_sheet(adjustmentData);
        XLSX.utils.book_append_sheet(workbook, adjustmentSheet, 'Adjustments');
      }

      // Generate buffer with proper options
      const buffer = XLSX.write(workbook, { 
        type: 'buffer', 
        bookType: 'xlsx',
        compression: true
      });
      
      // Generate filename with date range and dropshipper info
      const dropshipperPart = request.dropshipperEmail ? 
        `_${request.dropshipperEmail.split('@')[0]}` : '_all';
      const filename = `payout-report_${request.orderDateFrom}_to_${request.orderDateTo}${dropshipperPart}.xlsx`;
      
      // Set proper headers for Excel file download
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      res.send(buffer);
      
      console.log(`✅ Export completed: ${filename} (${payoutData.rows.length} orders)`);
      
    } catch (error) {
      console.error('Error exporting workbook:', error);
      res.status(500).json({ message: 'Error exporting workbook' });
    }
  });
}