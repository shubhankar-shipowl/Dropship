import { sql } from "drizzle-orm";
import { pgTable, text, varchar, integer, decimal, timestamp, boolean, jsonb } from "drizzle-orm/pg-core";
import { relations } from "drizzle-orm";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

// Upload Data
export const uploadSessions = pgTable("upload_sessions", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  filename: text("filename").notNull(),
  totalRows: integer("total_rows").notNull(),
  processedRows: integer("processed_rows").notNull(),
  cancelledRows: integer("cancelled_rows").notNull(),
  uploadedAt: timestamp("uploaded_at", { withTimezone: true }).defaultNow().notNull(),
});

export const orderData = pgTable("order_data", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  uploadSessionId: varchar("upload_session_id").references(() => uploadSessions.id).notNull(),
  dropshipperEmail: text("dropshipper_email").notNull(),
  orderId: text("order_id").notNull(),
  orderDate: timestamp("order_date", { withTimezone: true }).notNull(),
  waybill: text("waybill"),
  productName: text("product_name").notNull(),
  sku: text("sku"),
  productUid: text("product_uid").notNull(), // SKU or Product Name as fallback
  qty: integer("qty").notNull(),
  productValue: decimal("product_value", { precision: 10, scale: 2 }).notNull(),
  mode: text("mode"), // Payment mode: COD, Prepaid, etc.
  status: text("status").notNull(),
  deliveredDate: timestamp("delivered_date", { withTimezone: true }),
  rtsDate: timestamp("rts_date", { withTimezone: true }),
  shippingProvider: text("shipping_provider").notNull(),
  pincode: text("pincode"),
  state: text("state"),
  city: text("city"),
});

// Settings Tables
export const productPrices = pgTable("product_prices", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  dropshipperEmail: text("dropshipper_email").notNull(),
  productUid: text("product_uid").notNull(),
  productName: text("product_name").notNull(),
  sku: text("sku"),
  productWeight: decimal("product_weight", { precision: 8, scale: 3 }),
  productCostPerUnit: decimal("product_cost_per_unit", { precision: 10, scale: 2 }).notNull(),
  currency: text("currency").default("INR").notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
});

export const shippingRates = pgTable("shipping_rates", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  productUid: text("product_uid").notNull(),
  productWeight: decimal("product_weight", { precision: 8, scale: 3 }).notNull(),
  shippingProvider: text("shipping_provider").notNull(),
  shippingRatePerKg: decimal("shipping_rate_per_kg", { precision: 10, scale: 2 }).notNull(),
  currency: text("currency").default("INR").notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
});

// Payout Tracking
export const payoutLog = pgTable("payout_log", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  orderId: text("order_id").notNull(),
  waybill: text("waybill"),
  dropshipperEmail: text("dropshipper_email").notNull(),
  productUid: text("product_uid").notNull(),
  paidOn: timestamp("paid_on", { withTimezone: true }).defaultNow().notNull(),
  periodFrom: timestamp("period_from", { withTimezone: true }).notNull(),
  periodTo: timestamp("period_to", { withTimezone: true }).notNull(),
  paidAmount: decimal("paid_amount", { precision: 10, scale: 2 }).notNull(),
  payoutData: jsonb("payout_data"), // Store detailed breakdown
});

// RTS/RTO Reconciliation Tracking
export const rtsRtoReconciliation = pgTable("rts_rto_reconciliation", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  orderId: text("order_id").notNull(),
  waybill: text("waybill"),
  dropshipperEmail: text("dropshipper_email").notNull(),
  productUid: text("product_uid").notNull(),
  originalPayoutId: varchar("original_payout_id").references(() => payoutLog.id),
  originalPaidAmount: decimal("original_paid_amount", { precision: 10, scale: 2 }).notNull(),
  reversalAmount: decimal("reversal_amount", { precision: 10, scale: 2 }).notNull(),
  rtsRtoStatus: text("rts_rto_status").notNull(), // 'RTS', 'RTO', 'RTO-Dispatched'
  rtsRtoDate: timestamp("rts_rto_date", { withTimezone: true }).notNull(),
  reconciledOn: timestamp("reconciled_on", { withTimezone: true }).defaultNow().notNull(),
  reconciledBy: text("reconciled_by"), // Future: user who processed the reconciliation
  notes: text("notes"),
  status: text("status").default("pending").notNull(), // 'pending', 'processed', 'disputed'
});

// Relations
export const uploadSessionsRelations = relations(uploadSessions, ({ many }) => ({
  orderData: many(orderData),
}));

export const orderDataRelations = relations(orderData, ({ one }) => ({
  uploadSession: one(uploadSessions, {
    fields: [orderData.uploadSessionId],
    references: [uploadSessions.id],
  }),
}));

// Insert Schemas
export const insertUploadSessionSchema = createInsertSchema(uploadSessions).omit({
  id: true,
  uploadedAt: true,
});

export const insertOrderDataSchema = createInsertSchema(orderData).omit({
  id: true,
});

export const insertProductPriceSchema = createInsertSchema(productPrices).omit({
  id: true,
  updatedAt: true,
}).extend({
  productWeight: z.union([z.string(), z.number()]).transform((val) => {
    const num = typeof val === 'string' ? parseFloat(val) : val;
    return isNaN(num) ? null : num;
  }).nullable(),
  productCostPerUnit: z.union([z.string(), z.number()]).transform((val) => {
    const num = typeof val === 'string' ? parseFloat(val) : val;
    return isNaN(num) ? 0 : num;
  }),
});

export const insertShippingRateSchema = createInsertSchema(shippingRates).omit({
  id: true,
  updatedAt: true,
}).extend({
  productWeight: z.union([z.string(), z.number()]).transform((val) => {
    const num = typeof val === 'string' ? parseFloat(val) : val;
    return isNaN(num) ? 0.5 : num;
  }),
  shippingRatePerKg: z.union([z.string(), z.number()]).transform((val) => {
    const num = typeof val === 'string' ? parseFloat(val) : val;
    return isNaN(num) ? 0 : num;
  }),
});

export const insertPayoutLogSchema = createInsertSchema(payoutLog).omit({
  id: true,
  paidOn: true,
});

export const insertRtsRtoReconciliationSchema = createInsertSchema(rtsRtoReconciliation).omit({
  id: true,
  reconciledOn: true,
});

// Types
export type UploadSession = typeof uploadSessions.$inferSelect;
export type InsertUploadSession = z.infer<typeof insertUploadSessionSchema>;

export type OrderData = typeof orderData.$inferSelect;
export type InsertOrderData = z.infer<typeof insertOrderDataSchema>;

export type ProductPrice = typeof productPrices.$inferSelect;
export type InsertProductPrice = z.infer<typeof insertProductPriceSchema>;

export type ShippingRate = typeof shippingRates.$inferSelect;
export type InsertShippingRate = z.infer<typeof insertShippingRateSchema>;

export type PayoutLog = typeof payoutLog.$inferSelect;
export type InsertPayoutLog = z.infer<typeof insertPayoutLogSchema>;

export type RtsRtoReconciliation = typeof rtsRtoReconciliation.$inferSelect;
export type InsertRtsRtoReconciliation = z.infer<typeof insertRtsRtoReconciliationSchema>;

// Payout calculation types
export interface PayoutSummary {
  shippingTotal: number;
  codTotal: number;
  productCostTotal: number;
  rtsRtoReversalTotal: number;
  finalPayable: number;
  ordersWithShippingCharges: number;  // Count of orders in shipping date range
  ordersWithProductAmount: number;    // Count of orders in delivered date range (ALL)
  ordersWithCodAmount: number;        // Count of COD orders in delivered date range (COD only)
  totalOrdersProcessed: number;       // Total orders in calculation
}

export interface PayoutRow {
  orderId: string;
  waybill: string | null;
  product: string;
  productUid: string;
  productName: string;
  sku: string | null;
  dropshipperEmail: string;
  orderDate: Date;
  shippingProvider: string;
  qty: number;
  codAmountRupees: number;
  productCostPerUnit: number;
  productWeight: number;
  shippingRatePerKg: number;
  shippingCostCalculated: number;
  weight: number;
  mappingStatus: string;
  status: string;
  deliveredDate: string | null;
  rtsDate: string | null;
  shippingRate: number;
  shippingCost: number;
  productCost: number;
  payable: number;
  shippedQty: number;
  deliveredQty: number;
  codReceived: number;
  skuUid: string;
  courierCompany: string;
  pricePerUnit: number;
  codRate: number;
}

export interface PayoutCalculationRequest {
  orderDateFrom: string;
  orderDateTo: string;
  deliveredDateFrom: string;
  deliveredDateTo: string;
  dropshipperEmail?: string;
}

// Payment Cycles Configuration
export const paymentCycles = pgTable("payment_cycles", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  dropshipperEmail: text("dropshipper_email").notNull(),
  cycleType: text("cycle_type").notNull(), // 'daily', 'weekly', 'biweekly', 'monthly', 'custom'
  cycleParams: jsonb("cycle_params").notNull(), // Store cycle-specific parameters like days offset, weekdays, etc.
  isActive: boolean("is_active").default(true).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
});

// Export History
export const exportHistory = pgTable("export_history", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  dropshipperEmail: text("dropshipper_email").notNull(),
  exportType: text("export_type").notNull(), // 'payout_report', 'payment_history', 'order_data'
  dateRangeFrom: timestamp("date_range_from", { withTimezone: true }),
  dateRangeTo: timestamp("date_range_to", { withTimezone: true }),
  paymentCycleId: varchar("payment_cycle_id").references(() => paymentCycles.id),
  totalRecords: integer("total_records").notNull(),
  fileSize: integer("file_size"), // in bytes
  exportedAt: timestamp("exported_at", { withTimezone: true }).defaultNow().notNull(),
  exportParams: jsonb("export_params"), // Store additional export parameters
});

// Insert schemas for new tables
export const insertPaymentCycleSchema = createInsertSchema(paymentCycles).omit({
  id: true,
  createdAt: true,
  updatedAt: true,
});

export const insertExportHistorySchema = createInsertSchema(exportHistory).omit({
  id: true,
  exportedAt: true,
});

// Settlement Settings
export const settlementSettings = pgTable("settlement_settings", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  frequency: text("frequency").notNull(), // 'monthly', 'twice_weekly', 'thrice_weekly'
  lastPaymentDoneOn: timestamp("last_payment_done_on", { withTimezone: true }),
  lastDeliveredCutoff: timestamp("last_delivered_cutoff", { withTimezone: true }),
  dPlus2Enabled: boolean("d_plus_2_enabled").default(true).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
});

// Settlement Exports Log
export const settlementExports = pgTable("settlement_exports", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  runDate: timestamp("run_date", { withTimezone: true }).notNull(),
  orderStart: timestamp("order_start", { withTimezone: true }).notNull(),
  orderEnd: timestamp("order_end", { withTimezone: true }).notNull(),
  delStart: timestamp("del_start", { withTimezone: true }).notNull(),
  delEnd: timestamp("del_end", { withTimezone: true }).notNull(),
  shippingTotal: integer("shipping_total").notNull(),
  codTotal: integer("cod_total").notNull(),
  productCostTotal: integer("product_cost_total").notNull(),
  adjustmentsTotal: integer("adjustments_total").default(0).notNull(),
  finalPayable: integer("final_payable").notNull(),
  ordersCount: integer("orders_count").notNull(),
  exportedAt: timestamp("exported_at", { withTimezone: true }).defaultNow().notNull(),
});

// Insert schemas for settlement tables
export const insertSettlementSettingsSchema = createInsertSchema(settlementSettings).omit({
  id: true,
  createdAt: true,
  updatedAt: true,
});

export const insertSettlementExportSchema = createInsertSchema(settlementExports).omit({
  id: true,
  exportedAt: true,
});

// Types for new tables
export type PaymentCycle = typeof paymentCycles.$inferSelect;
export type InsertPaymentCycle = z.infer<typeof insertPaymentCycleSchema>;

export type ExportHistory = typeof exportHistory.$inferSelect;
export type InsertExportHistory = z.infer<typeof insertExportHistorySchema>;

export type SettlementSettings = typeof settlementSettings.$inferSelect;
export type InsertSettlementSettings = z.infer<typeof insertSettlementSettingsSchema>;

export type SettlementExport = typeof settlementExports.$inferSelect;
export type InsertSettlementExport = z.infer<typeof insertSettlementExportSchema>;
