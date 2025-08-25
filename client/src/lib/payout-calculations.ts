import type { OrderData, PayoutSummary, PayoutRow } from '@shared/schema';

export interface PayoutCalculationParams {
  orders: OrderData[];
  productPriceMap: Map<string, number>; // key: dropshipperEmail|productUid
  shippingRateMap: Map<string, number>; // key: shippingProvider
  orderDateFrom: string;
  orderDateTo: string;
  deliveredDateFrom: string;
  deliveredDateTo: string;
  payoutHistory?: Array<{
    orderId: string;
    waybill: string | null;
    dropshipperEmail: string;
    productUid: string;
    paidAmount: number;
    paidOn: string;
  }>;
}

export interface PayoutCalculationResult {
  summary: PayoutSummary;
  rows: PayoutRow[];
  adjustments: Array<{
    orderId: string;
    reason: string;
    amount: number;
    reference: string;
  }>;
}

export function calculatePayouts(params: PayoutCalculationParams): PayoutCalculationResult {
  const {
    orders,
    productPriceMap,
    shippingRateMap,
    orderDateFrom,
    orderDateTo,
    deliveredDateFrom,
    deliveredDateTo,
    payoutHistory = []
  } = params;

  const orderDateStart = new Date(orderDateFrom);
  const orderDateEnd = new Date(orderDateTo);
  const deliveredDateStart = new Date(deliveredDateFrom);
  const deliveredDateEnd = new Date(deliveredDateTo);

  let shippingTotal = 0;
  let codTotal = 0;
  let productCostTotal = 0;
  let rtsRtoReversalTotal = 0;
  
  // Order counts for summary
  let ordersWithShippingCharges = 0;
  let ordersWithProductAmount = 0;
  let ordersWithCodAmount = 0;

  const rows: PayoutRow[] = [];
  const adjustments: Array<{
    orderId: string;
    reason: string;
    amount: number;
    reference: string;
  }> = [];

  // Process each order line item
  for (const order of orders) {
    const orderDate = new Date(order.orderDate);
    const deliveredDate = order.deliveredDate ? new Date(order.deliveredDate) : null;
    
    // Check if order is in date ranges
    const inOrderDateRange = orderDate >= orderDateStart && orderDate <= orderDateEnd;
    const inDeliveredDateRange = deliveredDate && 
      deliveredDate >= deliveredDateStart && 
      deliveredDate <= deliveredDateEnd;

    const isDelivered = order.status.toLowerCase() === 'delivered';
    const isRtsRto = order.status.toLowerCase().includes('rts') || 
                    order.status.toLowerCase().includes('rto') || 
                    order.rtsDate;
    const isCancelled = order.status.toLowerCase() === 'cancelled';

    // Initialize calculation variables for this order
    let orderShippingCost = 0;
    let orderCodReceived = 0;
    let orderProductCost = 0;
    let orderDeliveredQty = 0;
    let orderAdjustmentAmount = 0;

    // Business Logic 1: Shipping Cost (Order date range, non-cancelled)
    if (inOrderDateRange && !isCancelled) {
      const shippingRate = shippingRateMap.get(order.shippingProvider) || 0;
      orderShippingCost = order.qty * shippingRate;
      shippingTotal += orderShippingCost;
      ordersWithShippingCharges++;
    }

    // Business Logic 2: COD Received and Product Cost (delivered date range)
    if (isDelivered && inDeliveredDateRange) {
      orderDeliveredQty = order.qty;
      
      // Excel Formula Logic: COD received only for COD orders that are delivered
      const modeUpper = (order.mode || '').toUpperCase();
      const isCodMode = modeUpper === 'COD' || modeUpper.includes('COD');
      
      if (isCodMode) {
        // COD received = Qty × Product Value
        const productValuePerUnit = Number(order.productValue) || 0;
        orderCodReceived = order.qty * productValuePerUnit;
        codTotal += orderCodReceived;
        ordersWithCodAmount++; // Count only COD orders
      }
      
      // Product cost applies to ALL delivered orders (COD + Prepaid)
      const productPrice = productPriceMap.get(`${order.dropshipperEmail}|${order.productUid}`) || 0;
      orderProductCost = order.qty * productPrice;
      productCostTotal += orderProductCost;
      ordersWithProductAmount++; // Count all delivered orders
    }

    // Business Logic 3: RTS/RTO Reversals
    if (isRtsRto && inOrderDateRange) {
      const priorPayout = payoutHistory.find(p => 
        p.orderId === order.orderId &&
        p.dropshipperEmail === order.dropshipperEmail &&
        p.productUid === order.productUid &&
        (p.waybill === order.waybill || (!p.waybill && !order.waybill))
      );

      if (priorPayout && priorPayout.paidAmount > 0) {
        orderAdjustmentAmount = -priorPayout.paidAmount;
        rtsRtoReversalTotal += orderAdjustmentAmount;
        
        adjustments.push({
          orderId: order.orderId,
          reason: `Delivered->RTS/RTO (reversal)`,
          amount: orderAdjustmentAmount,
          reference: `${priorPayout.paidOn} period`
        });
      }
    }

    // Only create row if there's ANY business activity
    const hasBusinessActivity = orderShippingCost > 0 || orderCodReceived > 0 || orderProductCost > 0 || orderAdjustmentAmount !== 0;
    
    if (!hasBusinessActivity) {
      continue;
    }

    const payable = orderCodReceived - orderShippingCost - orderProductCost + orderAdjustmentAmount;
    const productPrice = productPriceMap.get(`${order.dropshipperEmail}|${order.productUid}`) || 0;
    const shippingRate = shippingRateMap.get(order.shippingProvider) || 0;

    rows.push({
      orderId: order.orderId,
      waybill: order.waybill,
      product: order.productName,
      productUid: order.productUid,
      productName: order.productName,
      sku: order.sku,
      dropshipperEmail: order.dropshipperEmail,
      orderDate: orderDate,
      shippingProvider: order.shippingProvider,
      qty: order.qty,
      codAmountRupees: Number(order.productValue) || 0,
      productCostPerUnit: productPrice,
      productWeight: 0.5, // Default weight
      shippingRatePerKg: shippingRate,
      shippingCostCalculated: orderShippingCost,
      weight: 0.5, // Default weight
      mappingStatus: 'processed',
      status: order.status,
      deliveredDate: deliveredDate ? deliveredDate.toISOString().split('T')[0] : null,
      rtsDate: order.rtsDate ? new Date(order.rtsDate).toISOString().split('T')[0] : null,
      shippingRate: shippingRate,
      shippingCost: orderShippingCost,
      productCost: orderProductCost,
      payable,
      shippedQty: inOrderDateRange && !isCancelled ? order.qty : 0,
      deliveredQty: orderDeliveredQty,
      codReceived: orderCodReceived,
      skuUid: order.productUid,
      courierCompany: order.shippingProvider,
      pricePerUnit: productPrice,
      codRate: Number(order.productValue) || 0
    });
  }

  // Calculate final totals
  const finalPayable = codTotal - shippingTotal - productCostTotal + rtsRtoReversalTotal;

  return {
    summary: {
      shippingTotal: Math.round(shippingTotal),
      codTotal: Math.round(codTotal),
      productCostTotal: Math.round(productCostTotal),
      rtsRtoReversalTotal: Math.round(rtsRtoReversalTotal),
      finalPayable: Math.round(finalPayable),
      ordersWithShippingCharges,
      ordersWithProductAmount,
      ordersWithCodAmount,
      totalOrdersProcessed: rows.length
    },
    rows,
    adjustments
  };
}

export function validateDateRanges(
  orderDateFrom: string,
  orderDateTo: string,
  deliveredDateFrom: string,
  deliveredDateTo: string
): { isValid: boolean; errors: string[] } {
  const errors: string[] = [];

  if (!orderDateFrom || !orderDateTo) {
    errors.push('Order date range is required');
  } else if (new Date(orderDateFrom) > new Date(orderDateTo)) {
    errors.push('Order date "from" must be before "to"');
  }

  if (!deliveredDateFrom || !deliveredDateTo) {
    errors.push('Delivered date range is required');
  } else if (new Date(deliveredDateFrom) > new Date(deliveredDateTo)) {
    errors.push('Delivered date "from" must be before "to"');
  }

  return {
    isValid: errors.length === 0,
    errors
  };
}

export function formatCurrencyINR(amount: number): string {
  return new Intl.NumberFormat('en-IN', {
    style: 'currency',
    currency: 'INR',
    maximumFractionDigits: 0,
  }).format(amount);
}

export function formatCurrencyINRWithDecimals(amount: number): string {
  return new Intl.NumberFormat('en-IN', {
    style: 'currency',
    currency: 'INR',
    maximumFractionDigits: 2,
  }).format(amount);
}

export function roundToDecimals(num: number, decimals: number): number {
  return Math.round(num * Math.pow(10, decimals)) / Math.pow(10, decimals);
}

export function generatePayoutId(): string {
  return `PAYOUT_${new Date().toISOString().split('T')[0].replace(/-/g, '')}_${Math.random().toString(36).substr(2, 9).toUpperCase()}`;
}