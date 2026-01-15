import type { Express } from "express";
import { storage } from "../storage";
import { z } from "zod";
import * as XLSX from 'xlsx';
import nodemailer from 'nodemailer';
import { gmailService } from '../services/gmail';

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

// Helper function uses gmailService from services/gmail.ts
// All Gmail OAuth2 functionality is now handled by the centralized service

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

      // Try to use Gmail API first (if authorized via cred.json + token.json)
      const gmailClient = await gmailService.getGmailClient();
      
      if (gmailClient) {
        try {
          
          // Convert plain text to HTML with better formatting
          const htmlContent = request.content
            .replace(/\n/g, '<br>')
            .replace(/━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━/g, '<hr style="border: 1px solid #ddd; margin: 10px 0;">')
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/^([A-Z\s]+:)/gm, '<strong>$1</strong>')
            .replace(/^PAYOUT SUMMARY:/gm, '<h2 style="color: #2563eb; margin-top: 20px;">PAYOUT SUMMARY:</h2>')
            .replace(/^FINAL PAYABLE:/gm, '<h2 style="color: #16a34a; margin-top: 20px;">FINAL PAYABLE:</h2>')
            .replace(/^Order Statistics:/gm, '<h3 style="color: #7c3aed; margin-top: 15px;">Order Statistics:</h3>');

          // Build styled HTML email body
          const styledHtmlBody = `<!DOCTYPE html>
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
</html>`;

          // Parse CC addresses
          const ccList = parseCcAddresses(request.cc);
          const labelNameToUse = request.labelName ? String(request.labelName).trim() : undefined;

          // Send email via Gmail service
          const result = await gmailService.sendEmail({
            to: request.to,
            cc: ccList.length > 0 ? ccList : undefined,
            subject: request.subject,
            htmlBody: styledHtmlBody,
            textBody: request.content,
            attachment: excelAttachment ? {
              filename: excelAttachment.filename,
              content: excelAttachment.buffer,
              contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            } : undefined,
            labelName: labelNameToUse,
          });

          if (result.success) {
            return res.json({
              success: true,
              messageId: result.messageId,
              message: result.labelApplied && labelNameToUse
                ? `Email sent successfully with "${labelNameToUse}" label${excelAttachment ? ' and Excel attachment' : ''}`
                : `Email sent successfully${excelAttachment ? ' with Excel attachment' : ''}`,
              method: 'gmail-api',
              labelApplied: result.labelApplied,
              labelName: result.labelApplied ? labelNameToUse : undefined,
              attachmentIncluded: !!excelAttachment,
              attachmentFilename: excelAttachment?.filename,
              details: {
                to: request.to,
                subject: request.subject,
                timestamp: new Date().toISOString(),
              }
            });
          } else {
            throw new Error(result.error || 'Failed to send email via Gmail API');
          }
        } catch (gmailError: any) {
          console.error('❌ Gmail API error, falling back to SMTP:', gmailError.message);
          
          // If it's an authentication error, provide specific guidance
          if (gmailError.message?.includes('invalid_grant') || gmailError.message?.includes('Token')) {
            console.error('⚠️ Gmail API authentication failed. Please re-authorize:');
            console.error('   Visit: /api/gmail/authorize');
          }
          
          // Don't throw - fall through to SMTP method
          console.log('📧 Falling back to SMTP (labels will NOT be applied)');
        }
      } else {
        console.log('ℹ️ Gmail API not authorized, using SMTP (labels will not be applied)');
        console.log('💡 To enable Gmail API with labels, visit: /api/gmail/authorize');
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

      // Send email via SMTP
      console.log('📤 Attempting to send email...');
      const smtpCcList = parseCcAddresses(request.cc);

      const mailOptions: any = {
        from: `"Shipowl Finance Team" <${process.env.SMTP_USER || 'shubhankarhaldar07@gmail.com'}>`,
        to: request.to,
        // Optional CC for SMTP (array of emails)
        ...(smtpCcList.length > 0 ? { cc: smtpCcList } : {}),
        subject: request.subject,
        text: request.content,
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

  // Note: Gmail OAuth2 endpoints have been moved to /server/routes/gmail.ts
  // Legacy endpoints (/api/gmail-oauth-setup, /api/gmail-labels, /api/test-gmail-api) 
  // are now handled by the centralized gmail routes with proper redirect support.
}