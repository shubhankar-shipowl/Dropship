import type { Express } from "express";
import { storage } from "../storage";
import { z } from "zod";
import * as XLSX from 'xlsx';
import nodemailer from 'nodemailer';
import { google } from 'googleapis';

// Helper function to generate Excel workbook buffer
async function generatePayoutExcelBuffer(request: {
  orderDateFrom: string;
  orderDateTo: string;
  deliveredDateFrom: string;
  deliveredDateTo: string;
  dropshipperEmail?: string;
}): Promise<{ buffer: Buffer; filename: string }> {
  // Get payout calculation data
  const payoutData = await storage.calculatePayouts(request);
  
  // Create workbook
  const workbook = XLSX.utils.book_new();

  // Summary sheet with comprehensive details
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

  // Order Details sheet
  const orderHeaders = [
    'Order ID', 'Waybill', 'Product', 'SKU/UID', 'Dropshipper',
    'Order Date', 'Delivered Date', 'Shipped Qty', 'Delivered Qty',
    'COD Rate', 'COD Received', 'Shipping Cost', 'Product Cost',
    'Net Payable', 'Status', 'Shipping Provider', 'Weight (g)'
  ];

  const orderRows = [orderHeaders];
  payoutData.rows.filter(row => {
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

  // Shipping Details sheet
  const shippingHeaders = [
    'Order ID', 'Waybill', 'Product', 'Dropshipper', 'Shipping Provider',
    'Order Date', 'Quantity', 'Total Weight (g)', 'Shipping Cost (Rs.)', 
    'Status', 'Included in Calculation'
  ];

  const shippingRows = [shippingHeaders];
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

  // COD Details sheet
  const codHeaders = [
    'Order ID', 'Waybill', 'Product', 'Dropshipper', 'Delivered Date',
    'Delivered Qty', 'COD Rate (Rs.)', 'Total COD Received (Rs.)', 
    'Status', 'Included in Calculation'
  ];

  const codRows = [codHeaders];
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

  // Product Cost Details sheet
  const productCostHeaders = [
    'Order ID', 'Waybill', 'Product', 'SKU/UID', 'Dropshipper',
    'Delivered Qty', 'Product Cost per Unit (Rs.)', 'Total Product Cost (Rs.)',
    'Status', 'Cost Source'
  ];

  const productCostRows = [productCostHeaders];
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

  // Generate buffer
  const buffer = XLSX.write(workbook, { 
    type: 'buffer', 
    bookType: 'xlsx',
    compression: true
  });
  
  // Generate filename
  const dropshipperPart = request.dropshipperEmail ? 
    `_${request.dropshipperEmail.split('@')[0]}` : '_all';
  const filename = `payout-report_${request.orderDateFrom}_to_${request.orderDateTo}${dropshipperPart}.xlsx`;

  return { buffer, filename };
}

// Helper function to get or create Gmail label
async function getOrCreateLabel(gmail: any, labelName: string): Promise<string> {
  try {
    const labelNameTrimmed = labelName.trim();
    if (!labelNameTrimmed) {
      throw new Error('Label name cannot be empty');
    }

    // List all labels
    const response = await gmail.users.labels.list({
      userId: 'me',
    });

    // Check if label exists (case-insensitive and trimmed comparison, only user labels)
    const existingLabel = response.data.labels?.find(
      (label: any) => label.type === 'user' && label.name.trim().toLowerCase() === labelNameTrimmed.toLowerCase()
    );

    if (existingLabel) {
      console.log(`✅ Found existing label "${labelNameTrimmed}" with ID: ${existingLabel.id}`);
      return existingLabel.id;
    }

    // Create new label if it doesn't exist
    console.log(`📝 Creating new label "${labelNameTrimmed}"...`);
    const createResponse = await gmail.users.labels.create({
      userId: 'me',
      requestBody: {
        name: labelNameTrimmed,
        labelListVisibility: 'labelShow',
        messageListVisibility: 'show',
      },
    });

    const newLabelId = createResponse.data.id;
    if (!newLabelId) {
      throw new Error('Failed to create label - no ID returned');
    }

    console.log(`✅ Created label "${labelNameTrimmed}" with ID: ${newLabelId}`);
    return newLabelId;
  } catch (error: any) {
    console.error(`❌ Error managing label "${labelName}":`, error);
    console.error('Error details:', {
      message: error.message,
      code: error.code,
      response: error.response?.data,
    });
    throw error;
  }
}

// Helper function to get Gmail API client using OAuth2
async function getGmailClient() {
  // Check if OAuth2 credentials are configured
  if (!process.env.GMAIL_CLIENT_ID || !process.env.GMAIL_CLIENT_SECRET) {
    console.log('ℹ️ Gmail API OAuth2 not configured, using SMTP fallback');
    return null;
  }

  const oauth2Client = new google.auth.OAuth2(
    process.env.GMAIL_CLIENT_ID,
    process.env.GMAIL_CLIENT_SECRET,
    process.env.GMAIL_REDIRECT_URI || 'urn:ietf:wg:oauth:2.0:oob'
  );

  // Refresh token is required for Gmail API to work server-side
  // Without it, we cannot authenticate and must use SMTP fallback
  if (!process.env.GMAIL_REFRESH_TOKEN) {
    console.log('ℹ️ Gmail API refresh token not configured - Gmail API requires refresh token for server-side authentication');
    console.log('ℹ️ Using SMTP fallback (labels will not be applied)');
    return null;
  }

  // Set credentials with refresh token
  oauth2Client.setCredentials({
    refresh_token: process.env.GMAIL_REFRESH_TOKEN,
  });

  // The OAuth2 client will automatically refresh the access token when needed
  // We don't need to manually refresh it here - it will be done on first API call
  
  return google.gmail({ version: 'v1', auth: oauth2Client });
}

// Helper function to find existing thread for the same recipient and subject
async function findExistingThread(gmail: any, recipient: string, subjectPrefix: string): Promise<string | null> {
  try {
    // Search for existing emails to the same recipient with similar subject
    // Gmail search query: find sent emails to this recipient with subject containing "Payout Statement"
    const query = `to:${recipient} subject:"${subjectPrefix}"`;
    console.log(`🔍 Searching for existing thread with query: ${query}`);
    
    const searchResponse = await gmail.users.messages.list({
      userId: 'me',
      q: query,
      maxResults: 1, // We only need the most recent one
    });

    const messages = searchResponse.data.messages || [];
    if (messages.length > 0 && messages[0].threadId) {
      console.log(`✅ Found existing thread: ${messages[0].threadId}`);
      return messages[0].threadId;
    }

    console.log('ℹ️ No existing thread found - will create new thread');
    return null;
  } catch (error: any) {
    console.error('⚠️ Error searching for existing thread:', error.message);
    // Don't throw - just return null and create a new thread
    return null;
  }
}

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

      // Use helper function to generate Excel
      const { buffer, filename } = await generatePayoutExcelBuffer(request);
      
      // Set proper headers for Excel file download
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      res.send(buffer);
      
      console.log(`✅ Export completed: ${filename}`);
      
    } catch (error) {
      console.error('Error exporting workbook:', error);
      res.status(500).json({ message: 'Error exporting workbook' });
    }
  });

  // Helper to parse CC string (comma-separated) into a clean array of emails
  const parseCcAddresses = (cc?: string): string[] => {
    if (!cc) return [];
    return cc
      .split(',')
      .map((addr) => addr.trim())
      .filter((addr) => addr.length > 0);
  };

  // Send payout email
  app.post('/api/send-payout-email', async (req, res) => {
    try {
      const requestSchema = z.object({
        to: z.string().email(),
        // Optional CC field - can contain one or more comma-separated email addresses
        cc: z.string().optional(),
        subject: z.string(),
        content: z.string(),
        summary: z.object({
          shippingTotal: z.number(),
          codTotal: z.number(),
          productCostTotal: z.number(),
          rtsRtoReversalTotal: z.number(),
          finalPayable: z.number(),
        }).optional(),
        orderDateFrom: z.string().optional(),
        orderDateTo: z.string().optional(),
        deliveredDateFrom: z.string().optional(),
        deliveredDateTo: z.string().optional(),
        dropshipperEmail: z.string().optional(), // For filtering Excel data
        labelName: z.string().optional(), // Gmail label name to apply
      });

      const request = requestSchema.parse(req.body);
      console.log('📧 Sending payout email to:', request.to);
      const ccList = parseCcAddresses(request.cc);
      if (ccList.length > 0) {
        console.log('📧 CC recipients:', ccList);
      } else if (request.cc) {
        console.warn('⚠️ CC provided but no valid addresses were parsed from:', request.cc);
      }

      // Generate Excel file for attachment
      let excelAttachment: { buffer: Buffer; filename: string } | null = null;
      if (request.orderDateFrom && request.orderDateTo && request.deliveredDateFrom && request.deliveredDateTo) {
        try {
          console.log('📊 Generating Excel file for email attachment...');
          // Use dropshipper email if provided, otherwise use recipient email
          const dropshipperFilter = request.dropshipperEmail || request.to;
          excelAttachment = await generatePayoutExcelBuffer({
            orderDateFrom: request.orderDateFrom,
            orderDateTo: request.orderDateTo,
            deliveredDateFrom: request.deliveredDateFrom,
            deliveredDateTo: request.deliveredDateTo,
            dropshipperEmail: dropshipperFilter,
          });
          console.log(`✅ Excel file generated: ${excelAttachment.filename} (${excelAttachment.buffer.length} bytes)`);
        } catch (excelError: any) {
          console.error('⚠️ Error generating Excel file, sending email without attachment:', excelError.message);
          // Continue without attachment
        }
      }

      // Try to use Gmail API first (if OAuth2 is configured)
      console.log('🔍 Checking Gmail API configuration...');
      console.log('Gmail API config:', {
        hasClientId: !!process.env.GMAIL_CLIENT_ID,
        hasClientSecret: !!process.env.GMAIL_CLIENT_SECRET,
        hasRefreshToken: !!process.env.GMAIL_REFRESH_TOKEN,
        redirectUri: process.env.GMAIL_REDIRECT_URI || 'urn:ietf:wg:oauth:2.0:oob',
      });
      
      let gmail: any = null;
      try {
        gmail = await getGmailClient();
        if (gmail) {
          console.log('✅ Gmail API client created successfully');
        } else {
          console.log('⚠️ Gmail API client is null - will use SMTP');
        }
      } catch (gmailClientError: any) {
        console.error('❌ Error creating Gmail API client:', gmailClientError.message);
        console.error('Will fall back to SMTP');
        gmail = null;
      }
      
      if (gmail) {
        try {
          console.log('📧 Using Gmail API to send email...');
          console.log('🔍 Gmail API client initialized, checking label configuration...');
          
          // Determine if we should apply a Gmail label
          console.log('='.repeat(60));
          console.log('📧 EMAIL SEND REQUEST RECEIVED');
          console.log('🏷️ Raw request.labelName:', JSON.stringify(request.labelName));
          console.log('🏷️ Type of request.labelName:', typeof request.labelName);
          console.log('🏷️ Is request.labelName undefined?', request.labelName === undefined);
          console.log('🏷️ Is request.labelName null?', request.labelName === null);
          console.log('🏷️ Is request.labelName empty string?', request.labelName === '');
          
          const receivedLabelName = request.labelName ? String(request.labelName).trim() : '';
          const shouldApplyLabel = !!receivedLabelName;
          let labelNameToUse: string | null = null;
          let labelId: string | null = null;
          
          if (shouldApplyLabel) {
            labelNameToUse = receivedLabelName;
            console.log('🏷️ Label name after processing:', JSON.stringify(receivedLabelName));
            console.log('🏷️ Will apply Gmail label:', JSON.stringify(labelNameToUse));
            
            console.log(`🔍 Getting or creating label "${labelNameToUse}"...`);
            labelId = await getOrCreateLabel(gmail, labelNameToUse);
            
            if (!labelId) {
              throw new Error(`Failed to get or create label "${labelNameToUse}" - no label ID returned`);
            }
            
            console.log(`✅ Label ID obtained: ${labelId} for label "${labelNameToUse}"`);
          } else {
            console.log('ℹ️ No labelName provided – email will be sent via Gmail API without applying any user label');
          }
          
          // Convert plain text to HTML with better formatting
          const htmlContent = request.content
            .replace(/\n/g, '<br>')
            .replace(/━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━/g, '<hr style="border: 1px solid #ddd; margin: 10px 0;">')
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/^([A-Z\s]+:)/gm, '<strong>$1</strong>')
            .replace(/^PAYOUT SUMMARY:/gm, '<h2 style="color: #2563eb; margin-top: 20px;">PAYOUT SUMMARY:</h2>')
            .replace(/^FINAL PAYABLE:/gm, '<h2 style="color: #16a34a; margin-top: 20px;">FINAL PAYABLE:</h2>')
            .replace(/^Order Statistics:/gm, '<h3 style="color: #7c3aed; margin-top: 15px;">Order Statistics:</h3>');

          // Create multipart email with attachment if Excel file is available
          let emailBody: string;
          const ccList = parseCcAddresses(request.cc);
          const ccHeader = ccList.length > 0 ? `Cc: ${ccList.join(', ')}` : '';

          if (excelAttachment) {
            // Multipart email with attachment
            const boundary = `----=_Part_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
            const attachmentBase64 = excelAttachment.buffer.toString('base64');
            
            emailBody = [
              `From: "Shipowl Finance Team" <${process.env.SMTP_USER || 'shubhankarhaldar07@gmail.com'}>`,
              `To: ${request.to}`,
              ...(ccHeader ? [ccHeader] : []),
              `Subject: ${request.subject}`,
              `MIME-Version: 1.0`,
              `Content-Type: multipart/mixed; boundary="${boundary}"`,
              '',
              `--${boundary}`,
              `Content-Type: text/html; charset=utf-8`,
              `Content-Transfer-Encoding: 7bit`,
              '',
              `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
    .container { max-width: 600px; margin: 0 auto; padding: 20px; }
    .header { background-color: #2563eb; color: white; padding: 15px; border-radius: 5px; margin-bottom: 20px; }
    .content { background-color: #f9fafb; padding: 20px; border-radius: 5px; }
    .footer { margin-top: 20px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 12px; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1 style="margin: 0;">Payout Statement</h1>
    </div>
    <div class="content">
      ${htmlContent}
    </div>
    <div class="footer">
      <p>This is an automated email from Shipowl Finance Team.</p>
      <p>If you have any questions, please contact the support team.</p>
    </div>
  </div>
</body>
</html>`,
              '',
              `--${boundary}`,
              `Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`,
              `Content-Disposition: attachment; filename="${excelAttachment.filename}"`,
              `Content-Transfer-Encoding: base64`,
              '',
              attachmentBase64,
              '',
              `--${boundary}--`
            ].join('\r\n');
          } else {
            // Simple HTML email without attachment
            emailBody = [
              `From: "Shipowl Finance Team" <${process.env.SMTP_USER || 'shubhankarhaldar07@gmail.com'}>`,
              `To: ${request.to}`,
              ...(ccHeader ? [ccHeader] : []),
              `Subject: ${request.subject}`,
              'Content-Type: text/html; charset=utf-8',
              '',
              `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
    .container { max-width: 600px; margin: 0 auto; padding: 20px; }
    .header { background-color: #2563eb; color: white; padding: 15px; border-radius: 5px; margin-bottom: 20px; }
    .content { background-color: #f9fafb; padding: 20px; border-radius: 5px; }
    .footer { margin-top: 20px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 12px; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1 style="margin: 0;">Payout Statement</h1>
    </div>
    <div class="content">
      ${htmlContent}
    </div>
    <div class="footer">
      <p>This is an automated email from Shipowl Finance Team.</p>
      <p>If you have any questions, please contact the support team.</p>
    </div>
  </div>
</body>
</html>`
            ].join('\r\n');
          }

          const encodedEmail = Buffer.from(emailBody)
            .toString('base64')
            .replace(/\+/g, '-')
            .replace(/\//g, '_')
            .replace(/=+$/, '');

          // Find existing thread for email threading (so emails appear in same conversation)
          const subjectPrefix = request.subject.includes('Payout Statement') 
            ? 'Payout Statement' 
            : request.subject.substring(0, 30);
          const existingThreadId = await findExistingThread(gmail, request.to, subjectPrefix);

          // Send email first (without labelIds - it doesn't work reliably for user labels)
          console.log(`📤 Sending email via Gmail API...`);
          const sendRequest: any = {
            userId: 'me',
            requestBody: {
              raw: encodedEmail,
            },
          };

          // Add threadId if we found an existing thread (this groups emails into same conversation)
          if (existingThreadId) {
            sendRequest.requestBody.threadId = existingThreadId;
            console.log(`🔗 Using existing thread ID: ${existingThreadId} (emails will be grouped)`);
          }

          const sendResponse = await gmail.users.messages.send(sendRequest);

          const messageId = sendResponse.data.id;
          if (!messageId) {
            throw new Error('No message ID returned from Gmail API');
          }

          console.log('✅ Email sent via Gmail API');
          console.log('📧 Message ID:', messageId);

          let labelApplied = false;

          // Apply label AFTER sending (this is the correct way for user-created labels)
          if (shouldApplyLabel && labelId && labelNameToUse) {
            try {
              // Get current labels on the message
              const currentMessage = await gmail.users.messages.get({
                userId: 'me',
                id: messageId,
                format: 'metadata',
              });
              const currentLabelIds = currentMessage.data.labelIds || [];
              
              // Get all user-created labels to identify which ones to remove
              const labelsList = await gmail.users.labels.list({ userId: 'me' });
              const userLabels = labelsList.data.labels?.filter((l: any) => l.type === 'user') || [];
              
              // Find all user-created labels that are currently on the message (except the one we want to add)
              const labelsToRemove: string[] = [];
              for (const userLabel of userLabels) {
                // Skip the label we want to keep
                if (userLabel.id === labelId) {
                  continue;
                }
                // If this user label is on the message, mark it for removal
                if (currentLabelIds.includes(userLabel.id)) {
                  labelsToRemove.push(userLabel.id);
                  console.log(`🗑️ Will remove user label "${userLabel.name}" (ID: ${userLabel.id}) to ensure only "${labelNameToUse}" is applied`);
                }
              }

              console.log(`🏷️ Applying "${labelNameToUse}" label (ID: ${labelId}) to message ${messageId}...`);
              const modifyRequest: any = {
                userId: 'me',
                id: messageId,
                requestBody: {
                  addLabelIds: [labelId],
                },
              };

              // Remove all other user-created labels to ensure only the selected label is applied
              if (labelsToRemove.length > 0) {
                modifyRequest.requestBody.removeLabelIds = labelsToRemove;
                console.log(`🗑️ Removing ${labelsToRemove.length} user label(s) to ensure only "${labelNameToUse}" is applied`);
              }

              const modifyResponse = await gmail.users.messages.modify(modifyRequest);
              console.log(`✅ "${labelNameToUse}" label applied successfully`);
              console.log(`📧 Message labels after modification:`, modifyResponse.data.labelIds);

              // Verify label was applied by fetching the message again
              await new Promise(resolve => setTimeout(resolve, 500)); // Small delay to ensure Gmail processes the change
              
              const messageDetails = await gmail.users.messages.get({
                userId: 'me',
                id: messageId,
                format: 'metadata',
                metadataHeaders: ['Subject', 'To'],
              });
              const appliedLabels = messageDetails.data.labelIds || [];
              console.log('🏷️ Final labels on email:', appliedLabels);
              console.log('🏷️ Expected label ID:', labelId);
              
              if (appliedLabels.includes(labelId)) {
                console.log(`✅ "${labelNameToUse}" label confirmed on sent email`);
                labelApplied = true;
              } else {
                console.error(`❌ "${labelNameToUse}" label NOT found after applying!`);
                console.error('Expected label ID:', labelId);
                console.error('Applied label IDs:', appliedLabels);
                
                // Try to get label name from ID for debugging
                try {
                  const labelsList = await gmail.users.labels.list({ userId: 'me' });
                  const labelInfo = labelsList.data.labels?.find((l: any) => l.id === labelId);
                  console.error('Label info:', labelInfo ? { id: labelInfo.id, name: labelInfo.name } : 'NOT FOUND');
                } catch (e) {
                  console.error('Could not fetch label info:', e);
                }
              }
            } catch (labelError: any) {
              console.error('❌ Error applying label:', labelError.message);
              console.error('Label error details:', {
                code: labelError.code,
                response: labelError.response?.data,
                messageId: messageId,
                labelId: labelId,
                labelName: labelNameToUse,
              });
              // Don't fail the entire operation - email was sent successfully
              console.warn('⚠️ Email sent but label could not be applied');
            }
          } else {
            console.log('ℹ️ Skipping Gmail label application because no labelName was provided');
          }
          
          return res.json({
            success: true,
            messageId: sendResponse.data.id,
            message: labelApplied && labelNameToUse
              ? `Email sent successfully with "${labelNameToUse}" label${excelAttachment ? ' and Excel attachment' : ''}`
              : `Email sent successfully${excelAttachment ? ' with Excel attachment' : ''}`,
            method: 'gmail-api',
            labelApplied,
            labelName: labelApplied ? labelNameToUse : undefined,
            attachmentIncluded: !!excelAttachment,
            attachmentFilename: excelAttachment?.filename,
            details: {
              to: request.to,
              subject: request.subject,
              timestamp: new Date().toISOString(),
            }
          });
        } catch (gmailError: any) {
          console.error('❌ Gmail API error, falling back to SMTP:', gmailError);
          console.error('Gmail API error details:', {
            message: gmailError.message,
            code: gmailError.code,
            response: gmailError.response?.data,
            status: gmailError.response?.status,
            errors: gmailError.errors,
            stack: gmailError.stack,
          });
          
          // If it's an authentication error, provide specific guidance
          if (gmailError.code === 401 || gmailError.code === 403) {
            console.error('⚠️ Gmail API authentication failed. Please check your refresh token.');
            console.error('💡 Try regenerating the refresh token:');
            console.error('   1. Visit: http://localhost:3007/api/gmail-oauth-setup');
            console.error('   2. Get new authorization code');
            console.error('   3. Exchange for new refresh token');
            console.error('   4. Update .env and restart server');
          }
          
          // Don't throw - fall through to SMTP method
          console.log('📧 Falling back to SMTP (labels will NOT be applied)');
        }
      } else {
        console.log('ℹ️ Gmail API not available, using SMTP (labels will not be applied)');
        console.log('💡 To enable labels, configure Gmail API OAuth2 in .env file');
      }

      // Fallback to SMTP if Gmail API is not available
      console.log('📧 Using SMTP to send email...');
      
      // Create transporter with SMTP configuration
      const transporter = nodemailer.createTransport({
        host: process.env.SMTP_HOST || 'smtp.gmail.com',
        port: parseInt(process.env.SMTP_PORT || '587'),
        secure: process.env.SMTP_SECURE === 'true', // true for 465, false for other ports
        requireTLS: true, // Force TLS
        auth: {
          user: process.env.SMTP_USER || 'shubhankarhaldar07@gmail.com',
          pass: process.env.SMTP_PASSWORD || 'qzzhrswnidzswfvz',
        },
        tls: {
          rejectUnauthorized: false, // Accept self-signed certificates if needed
        },
        debug: true, // Enable debug output
        logger: true, // Log to console
      });

      // Verify SMTP connection before sending
      console.log('🔍 Verifying SMTP connection...');
      try {
        await transporter.verify();
        console.log('✅ SMTP server is ready to send emails');
      } catch (verifyError: any) {
        console.error('❌ SMTP verification failed:', verifyError);
        throw new Error(`SMTP connection failed: ${verifyError.message}`);
      }

      // Convert plain text to HTML with better formatting
      const htmlContent = request.content
        .replace(/\n/g, '<br>')
        .replace(/━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━/g, '<hr style="border: 1px solid #ddd; margin: 10px 0;">')
        .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
        .replace(/^([A-Z\s]+:)/gm, '<strong>$1</strong>')
        .replace(/^PAYOUT SUMMARY:/gm, '<h2 style="color: #2563eb; margin-top: 20px;">PAYOUT SUMMARY:</h2>')
        .replace(/^FINAL PAYABLE:/gm, '<h2 style="color: #16a34a; margin-top: 20px;">FINAL PAYABLE:</h2>')
        .replace(/^Order Statistics:/gm, '<h3 style="color: #7c3aed; margin-top: 15px;">Order Statistics:</h3>');

      // For SMTP threading: Try to find existing message ID for this recipient
      // This helps Gmail group emails into the same conversation
      let inReplyTo: string | undefined;
      let references: string | undefined;
      
      if (gmail) {
        try {
          const subjectPrefix = request.subject.includes('Payout Statement') 
            ? 'Payout Statement' 
            : request.subject.substring(0, 30);
          const query = `to:${request.to} subject:"${subjectPrefix}"`;
          const searchResponse = await gmail.users.messages.list({
            userId: 'me',
            q: query,
            maxResults: 1,
          });
          
          if (searchResponse.data.messages && searchResponse.data.messages.length > 0) {
            const messageId = searchResponse.data.messages[0].id;
            const messageDetails = await gmail.users.messages.get({
              userId: 'me',
              id: messageId,
              format: 'metadata',
              metadataHeaders: ['Message-ID', 'References'],
            });
            
            const headers = messageDetails.data.payload?.headers || [];
            const existingMessageId = headers.find((h: any) => h.name === 'Message-ID')?.value;
            const existingReferences = headers.find((h: any) => h.name === 'References')?.value;
            
            if (existingMessageId) {
              inReplyTo = existingMessageId;
              references = existingReferences 
                ? `${existingReferences} ${existingMessageId}` 
                : existingMessageId;
              console.log(`🔗 Found existing email thread - will group with previous email`);
            }
          }
        } catch (threadError: any) {
          console.log('ℹ️ Could not find existing thread for SMTP (will create new conversation):', threadError.message);
        }
      }

      // Send email
      console.log('📤 Attempting to send email...');
      const smtpCcList = parseCcAddresses(request.cc);

      const mailOptions: any = {
        from: `"Shipowl Finance Team" <${process.env.SMTP_USER || 'shubhankarhaldar07@gmail.com'}>`,
        to: request.to,
        // Optional CC for SMTP (array of emails)
        ...(smtpCcList.length > 0 ? { cc: smtpCcList } : {}),
        subject: request.subject,
        text: request.content,
        // Add threading headers for SMTP to group emails in same conversation
        ...(inReplyTo && { inReplyTo }),
        ...(references && { references }),
        html: `
          <!DOCTYPE html>
          <html>
          <head>
            <meta charset="utf-8">
            <style>
              body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
              .container { max-width: 600px; margin: 0 auto; padding: 20px; }
              .header { background-color: #2563eb; color: white; padding: 15px; border-radius: 5px; margin-bottom: 20px; }
              .content { background-color: #f9fafb; padding: 20px; border-radius: 5px; }
              .summary-box { background-color: white; padding: 15px; border-left: 4px solid #2563eb; margin: 15px 0; }
              .footer { margin-top: 20px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 12px; }
            </style>
          </head>
          <body>
            <div class="container">
              <div class="header">
                <h1 style="margin: 0;">Payout Statement</h1>
              </div>
              <div class="content">
                ${htmlContent}
              </div>
              <div class="footer">
                <p>This is an automated email from Shipowl Finance Team.</p>
                <p>If you have any questions, please contact the support team.</p>
                ${excelAttachment ? `<p><strong>Note:</strong> Please find the detailed payout report attached as an Excel file.</p>` : ''}
              </div>
            </div>
          </body>
          </html>
        `,
      };

      // Add Excel attachment if available
      if (excelAttachment) {
        mailOptions.attachments = [
          {
            filename: excelAttachment.filename,
            content: excelAttachment.buffer,
            contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
          }
        ];
        console.log(`📎 Attaching Excel file: ${excelAttachment.filename}`);
      }

      const info = await transporter.sendMail(mailOptions);
      
      console.log('✅ Email sent successfully!');
      console.log('📧 Email details:', {
        messageId: info.messageId,
        accepted: info.accepted,
        rejected: info.rejected,
        pending: info.pending,
        response: info.response,
      });

      // Check if email was actually accepted
      if (info.rejected && info.rejected.length > 0) {
        console.error('❌ Email was rejected:', info.rejected);
        throw new Error(`Email was rejected by server: ${info.rejected.join(', ')}`);
      }

      if (!info.messageId) {
        console.error('❌ No message ID returned - email may not have been sent');
        throw new Error('Email was not accepted by the server');
      }

      res.json({ 
        success: true, 
        messageId: info.messageId,
        accepted: info.accepted,
        rejected: info.rejected,
        message: `Email sent successfully${excelAttachment ? ' with Excel attachment' : ''} (Note: Gmail labels only work when Gmail API is configured and a label is selected)`,
        method: 'smtp',
        labelApplied: false,
        attachmentIncluded: !!excelAttachment,
        attachmentFilename: excelAttachment?.filename,
        details: {
          to: request.to,
          subject: request.subject,
          timestamp: new Date().toISOString(),
        }
      });
    } catch (error: any) {
      console.error('❌ Error sending email:', error);
      console.error('Error details:', {
        name: error.name,
        message: error.message,
        code: error.code,
        command: error.command,
        response: error.response,
        responseCode: error.responseCode,
        stack: error.stack,
      });

      // Provide more helpful error messages
      let errorMessage = error.message || 'Error sending email';
      
      if (error.code === 'EAUTH') {
        errorMessage = 'SMTP authentication failed. Please check your email and password.';
      } else if (error.code === 'ECONNECTION') {
        errorMessage = 'Could not connect to SMTP server. Please check your SMTP settings.';
      } else if (error.code === 'ETIMEDOUT') {
        errorMessage = 'SMTP connection timed out. Please try again.';
      } else if (error.responseCode === 535) {
        errorMessage = 'SMTP authentication failed. For Gmail, make sure you are using an App Password, not your regular password.';
      } else if (error.responseCode === 550) {
        errorMessage = 'Email address rejected. Please check the recipient email address.';
      }

      res.status(500).json({ 
        success: false,
        message: errorMessage,
        error: error.toString(),
        code: error.code,
        responseCode: error.responseCode,
      });
    }
  });

  // Test email endpoint
  app.post('/api/test-email', async (req, res) => {
    try {
      const { to } = req.body;
      if (!to || !z.string().email().safeParse(to).success) {
        return res.status(400).json({ message: 'Valid email address is required' });
      }

      console.log('🧪 Testing email to:', to);

      // Create transporter with SMTP configuration
      const transporter = nodemailer.createTransport({
        host: process.env.SMTP_HOST || 'smtp.gmail.com',
        port: parseInt(process.env.SMTP_PORT || '587'),
        secure: process.env.SMTP_SECURE === 'true',
        requireTLS: true,
        auth: {
          user: process.env.SMTP_USER || 'shubhankarhaldar07@gmail.com',
          pass: process.env.SMTP_PASSWORD || 'qzzhrswnidzswfvz',
        },
        tls: {
          rejectUnauthorized: false,
        },
        debug: true,
        logger: true,
      });

      // Verify SMTP connection
      console.log('🔍 Verifying SMTP connection...');
      await transporter.verify();
      console.log('✅ SMTP server is ready');

      // Send test email
      const info = await transporter.sendMail({
        from: `"Shipowl Finance Team" <${process.env.SMTP_USER || 'shubhankarhaldar07@gmail.com'}>`,
        to: to,
        subject: 'Test Email - Payout System',
        text: 'This is a test email from the Payout System. If you receive this, your SMTP configuration is working correctly.',
        html: `
          <div style="font-family: Arial, sans-serif; padding: 20px;">
            <h2 style="color: #2563eb;">Test Email - Payout System</h2>
            <p>This is a test email from the Payout System.</p>
            <p>If you receive this email, your SMTP configuration is working correctly.</p>
            <p style="color: #666; font-size: 12px; margin-top: 20px;">Sent at: ${new Date().toLocaleString()}</p>
          </div>
        `,
      });

      console.log('✅ Test email sent:', info.messageId);

      res.json({
        success: true,
        message: 'Test email sent successfully',
        messageId: info.messageId,
        accepted: info.accepted,
        rejected: info.rejected,
      });
    } catch (error: any) {
      console.error('❌ Test email failed:', error);
      res.status(500).json({
        success: false,
        message: error.message || 'Failed to send test email',
        error: error.toString(),
        code: error.code,
        responseCode: error.responseCode,
      });
    }
  });

  // Gmail OAuth2 setup endpoint (for getting authorization URL)
  app.get('/api/gmail-oauth-setup', async (req, res) => {
    try {
      if (!process.env.GMAIL_CLIENT_ID || !process.env.GMAIL_CLIENT_SECRET) {
        return res.status(400).json({
          message: 'Gmail OAuth2 credentials not configured',
          instructions: [
            '1. Go to https://console.cloud.google.com/',
            '2. Create a new project or select existing one',
            '3. Enable Gmail API',
            '4. Create OAuth 2.0 credentials',
            '5. Add authorized redirect URI: urn:ietf:wg:oauth:2.0:oob',
            '6. Set GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET in .env file',
            '7. Visit this endpoint again to get authorization URL',
          ]
        });
      }

      const oauth2Client = new google.auth.OAuth2(
        process.env.GMAIL_CLIENT_ID,
        process.env.GMAIL_CLIENT_SECRET,
        process.env.GMAIL_REDIRECT_URI || 'urn:ietf:wg:oauth:2.0:oob'
      );

      const scopes = [
        'https://www.googleapis.com/auth/gmail.send',
        'https://www.googleapis.com/auth/gmail.modify',
        'https://www.googleapis.com/auth/gmail.labels',
      ];

      const authUrl = oauth2Client.generateAuthUrl({
        access_type: 'offline',
        scope: scopes,
        prompt: 'consent', // Force consent to get refresh token
      });

      res.json({
        message: 'Gmail OAuth2 setup',
        authUrl: authUrl,
        instructions: [
          '1. Visit the authUrl above in your browser',
          '2. Authorize the application',
          '3. Copy the authorization code from the redirect page',
          '4. Use POST /api/gmail-oauth-callback with the code to get refresh token',
        ]
      });
    } catch (error: any) {
      console.error('Error setting up Gmail OAuth2:', error);
      res.status(500).json({ message: error.message || 'Error setting up OAuth2' });
    }
  });

  // Gmail OAuth2 callback endpoint (to exchange code for refresh token)
  app.post('/api/gmail-oauth-callback', async (req, res) => {
    try {
      const { code } = req.body;
      if (!code) {
        return res.status(400).json({ message: 'Authorization code is required' });
      }

      if (!process.env.GMAIL_CLIENT_ID || !process.env.GMAIL_CLIENT_SECRET) {
        return res.status(400).json({ message: 'Gmail OAuth2 credentials not configured' });
      }

      const oauth2Client = new google.auth.OAuth2(
        process.env.GMAIL_CLIENT_ID,
        process.env.GMAIL_CLIENT_SECRET,
        process.env.GMAIL_REDIRECT_URI || 'urn:ietf:wg:oauth:2.0:oob'
      );

      const { tokens } = await oauth2Client.getToken(code);
      
      res.json({
        message: 'OAuth2 authorization successful!',
        refreshToken: tokens.refresh_token,
        instructions: [
          'Add this to your .env file:',
          `GMAIL_REFRESH_TOKEN=${tokens.refresh_token}`,
          'Then restart your server to use Gmail API with labels.',
        ]
      });
    } catch (error: any) {
      console.error('Error in OAuth2 callback:', error);
      res.status(500).json({ message: error.message || 'Error processing OAuth2 callback'       });
    }
  });

  // Get all Gmail labels
  app.get('/api/gmail-labels', async (req, res) => {
    try {
      const gmail = await getGmailClient();
      
      if (!gmail) {
        return res.status(400).json({
          success: false,
          message: 'Gmail API not configured',
          labels: [],
        });
      }

      const response = await gmail.users.labels.list({
        userId: 'me',
      });

      // Filter to only show user-created labels (not system labels)
      const userLabels = (response.data.labels || [])
        .filter((label: any) => label.type === 'user')
        .map((label: any) => ({
          id: label.id,
          name: label.name,
          type: label.type,
        }))
        .sort((a: any, b: any) => a.name.localeCompare(b.name));

      res.json({
        success: true,
        labels: userLabels,
      });
    } catch (error: any) {
      console.error('Error fetching Gmail labels:', error);
      res.status(500).json({
        success: false,
        message: 'Error fetching labels',
        error: error.message,
        labels: [],
      });
    }
  });

  // Create a new Gmail label
  app.post('/api/gmail-labels', async (req, res) => {
    try {
      const { name } = req.body;
      
      if (!name || typeof name !== 'string' || name.trim().length === 0) {
        return res.status(400).json({
          success: false,
          message: 'Label name is required',
        });
      }

      const gmail = await getGmailClient();
      
      if (!gmail) {
        return res.status(400).json({
          success: false,
          message: 'Gmail API not configured',
        });
      }

      // Check if label already exists
      const labelsResponse = await gmail.users.labels.list({
        userId: 'me',
      });

      const existingLabel = labelsResponse.data.labels?.find(
        (label: any) => label.name === name.trim() && label.type === 'user'
      );

      if (existingLabel) {
        return res.json({
          success: true,
          message: 'Label already exists',
          label: {
            id: existingLabel.id,
            name: existingLabel.name,
            type: existingLabel.type,
          },
        });
      }

      // Create new label
      const createResponse = await gmail.users.labels.create({
        userId: 'me',
        requestBody: {
          name: name.trim(),
          labelListVisibility: 'labelShow',
          messageListVisibility: 'show',
        },
      });

      res.json({
        success: true,
        message: 'Label created successfully',
        label: {
          id: createResponse.data.id,
          name: createResponse.data.name,
          type: createResponse.data.type,
        },
      });
    } catch (error: any) {
      console.error('Error creating Gmail label:', error);
      res.status(500).json({
        success: false,
        message: 'Error creating label',
        error: error.message,
      });
    }
  });

  // Test Gmail API connection and label
  app.get('/api/test-gmail-api', async (req, res) => {
    try {
      console.log('🧪 Testing Gmail API connection...');
      
      const gmail = await getGmailClient();
      
      if (!gmail) {
        return res.status(400).json({
          success: false,
          message: 'Gmail API not configured. Please check your .env file.',
          configured: {
            clientId: !!process.env.GMAIL_CLIENT_ID,
            clientSecret: !!process.env.GMAIL_CLIENT_SECRET,
            refreshToken: !!process.env.GMAIL_REFRESH_TOKEN,
          }
        });
      }

      // Test getting user profile
      const profile = await gmail.users.getProfile({ userId: 'me' });
      console.log('✅ Gmail API connection successful');
      console.log('📧 Email address:', profile.data.emailAddress);

      // Test getting/creating label
      let labelId: string;
      try {
        labelId = await getOrCreateLabel(gmail, 'Dropshipper');
        console.log('✅ Label "Dropshipper" is available');
      } catch (labelError: any) {
        console.error('❌ Error with label:', labelError);
        return res.status(500).json({
          success: false,
          message: 'Gmail API connected but label operation failed',
          error: labelError.message,
          profile: {
            email: profile.data.emailAddress,
          }
        });
      }

      res.json({
        success: true,
        message: 'Gmail API is working correctly!',
        profile: {
          email: profile.data.emailAddress,
        },
        label: {
          name: 'Shipowl Finance Team',
          id: labelId,
        },
        note: 'Labels will be applied automatically when sending emails via Gmail API'
      });
    } catch (error: any) {
      console.error('❌ Gmail API test failed:', error);
      res.status(500).json({
        success: false,
        message: 'Gmail API test failed',
        error: error.message,
        code: error.code,
        details: error.response?.data || error.toString()
      });
    }
  });
}