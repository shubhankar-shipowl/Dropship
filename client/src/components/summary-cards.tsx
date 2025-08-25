import { Truck, Coins, Package, Undo, Wallet } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import type { PayoutSummary } from "@shared/schema";

interface SummaryCardsProps {
  summary: PayoutSummary;
  isLoading?: boolean;
  viewType?: 'tab' | 'webview';
}

export default function SummaryCards({ summary, isLoading, viewType = 'tab' }: SummaryCardsProps) {
  if (isLoading || !summary) {
    return (
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4 md:gap-6">
        {[...Array(5)].map((_, i) => (
          <Card key={i} className="rounded-xl md:rounded-2xl shadow-lg">
            <CardContent className="p-4 md:p-6">
              <Skeleton className="h-16 md:h-20 w-full" />
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  // Provide default values to prevent undefined errors
  const safeData = {
    shippingTotal: summary?.shippingTotal || 0,
    codTotal: summary?.codTotal || 0,
    productCostTotal: summary?.productCostTotal || 0,
    rtsRtoReversalTotal: summary?.rtsRtoReversalTotal || 0,
    finalPayable: summary?.finalPayable || 0,
    ordersWithShippingCharges: summary?.ordersWithShippingCharges || 0,
    ordersWithProductAmount: summary?.ordersWithProductAmount || 0,
    ordersWithCodAmount: summary?.ordersWithCodAmount || 0,
  };

  const formatCurrency = (amount: number) => {
    // Show zero amounts explicitly as ₹0 - never hide them
    if (amount === 0) return '₹0';
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      maximumFractionDigits: 0,
    }).format(amount);
  };

  // Web view design - user friendly centered cards
  if (viewType === 'webview') {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-5 gap-6">
        {/* Shipping Cost Card - Web View */}
        <Card className="bg-white border border-blue-200 hover:border-blue-300 rounded-2xl shadow-sm hover:shadow-lg transition-all duration-300 group">
          <CardContent className="p-6">
            <div className="text-center space-y-4">
              <div className="mx-auto w-16 h-16 bg-blue-500 rounded-full flex items-center justify-center group-hover:scale-110 transition-transform duration-300">
                <Truck className="h-8 w-8 text-white" />
              </div>
              <div>
                <h3 className="text-sm font-semibold text-blue-700 uppercase tracking-wider">
                  Shipping Cost
                </h3>
                <p className="text-xs text-blue-500 mt-1">शिपिंग की लागत</p>
                <div className="mt-3">
                  <span className="text-3xl font-bold text-blue-900" data-testid="text-shipping-total">
                    {formatCurrency(safeData.shippingTotal)}
                  </span>
                </div>
                <p className="text-xs text-blue-600 mt-2">
                  {safeData.ordersWithShippingCharges > 0 
                    ? `${safeData.ordersWithShippingCharges} orders`
                    : 'Apply filters to calculate'
                  }
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
  
        {/* COD Received Card - Web View */}
        <Card className="bg-white border border-green-200 hover:border-green-300 rounded-2xl shadow-sm hover:shadow-lg transition-all duration-300 group">
          <CardContent className="p-6">
            <div className="text-center space-y-4">
              <div className="mx-auto w-16 h-16 bg-green-500 rounded-full flex items-center justify-center group-hover:scale-110 transition-transform duration-300">
                <Coins className="h-8 w-8 text-white" />
              </div>
              <div>
                <h3 className="text-sm font-semibold text-green-700 uppercase tracking-wider">
                  COD Received
                </h3>
                <p className="text-xs text-green-500 mt-1">Product Value (Delivered COD Only)</p>
                <div className="mt-3">
                  <span className="text-3xl font-bold text-green-900" data-testid="text-cod-total">
                    {formatCurrency(safeData.codTotal)}
                  </span>
                </div>
                <p className="text-xs text-green-600 mt-2">
                  {safeData.ordersWithCodAmount > 0 
                    ? `${safeData.ordersWithCodAmount} delivered orders`
                    : 'Apply filters to calculate'
                  }
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
  
        {/* Product Cost Card - Web View */}
        <Card className="bg-white border border-orange-200 hover:border-orange-300 rounded-2xl shadow-sm hover:shadow-lg transition-all duration-300 group">
          <CardContent className="p-6">
            <div className="text-center space-y-4">
              <div className="mx-auto w-16 h-16 bg-orange-500 rounded-full flex items-center justify-center group-hover:scale-110 transition-transform duration-300">
                <Package className="h-8 w-8 text-white" />
              </div>
              <div>
                <h3 className="text-sm font-semibold text-orange-700 uppercase tracking-wider">
                  Product Cost
                </h3>
                <p className="text-xs text-orange-500 mt-1">उत्पाद की लागत</p>
                <div className="mt-3">
                  <span className="text-3xl font-bold text-orange-900" data-testid="text-product-cost">
                    {formatCurrency(safeData.productCostTotal)}
                  </span>
                </div>
                <p className="text-xs text-orange-600 mt-2">
                  {safeData.ordersWithProductAmount > 0 
                    ? `${safeData.ordersWithProductAmount} delivered orders`
                    : 'Apply filters to calculate'
                  }
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
  
        {/* RTS/RTO Reversal Card - Web View */}
        <Card className="bg-white border border-red-200 hover:border-red-300 rounded-2xl shadow-sm hover:shadow-lg transition-all duration-300 group">
          <CardContent className="p-6">
            <div className="text-center space-y-4">
              <div className="mx-auto w-16 h-16 bg-red-500 rounded-full flex items-center justify-center group-hover:scale-110 transition-transform duration-300">
                <Undo className="h-8 w-8 text-white" />
              </div>
              <div>
                <h3 className="text-sm font-semibold text-red-700 uppercase tracking-wider">
                  RTS/RTO Reversal
                </h3>
                <p className="text-xs text-red-500 mt-1">वापसी की कटौती</p>
                <div className="mt-3">
                  <span className="text-3xl font-bold text-red-900" data-testid="text-reversal-total">
                    {formatCurrency(safeData.rtsRtoReversalTotal)}
                  </span>
                </div>
                <p className="text-xs text-red-600 mt-2">
                  Deductions for returns
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
  
        {/* Final Payable Card - Web View Special */}
        <Card className="bg-gradient-to-br from-emerald-50 to-green-100 border-2 border-green-300 rounded-2xl shadow-lg hover:shadow-xl transition-all duration-300 group">
          <CardContent className="p-8">
            <div className="text-center space-y-6">
              <div className="mx-auto w-20 h-20 bg-gradient-to-br from-green-600 to-emerald-700 rounded-full flex items-center justify-center group-hover:scale-110 transition-transform duration-300 shadow-lg">
                <Wallet className="h-10 w-10 text-white" />
              </div>
              <div>
                <h3 className="text-base font-bold text-green-800 uppercase tracking-wider">
                  Final Payable
                </h3>
                <p className="text-sm text-green-600 mt-1">अंतिम भुगतान योग्य</p>
                <div className="mt-4">
                  <span className="text-4xl font-bold text-green-900" data-testid="text-final-payable">
                    {formatCurrency(safeData.finalPayable)}
                  </span>
                </div>
                <p className="text-sm text-green-700 mt-3 font-semibold bg-green-200 px-4 py-2 rounded-full inline-block">
                  Ready for Payout
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }

  // Tab view design - original cards
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4 md:gap-6">
      <Card className="bg-gradient-to-br from-blue-50 to-blue-100 border-2 border-blue-200 rounded-xl md:rounded-2xl shadow-lg hover:shadow-xl transition-all duration-300 hover:scale-105">
        <CardContent className="p-4 md:p-6 relative overflow-hidden">
          <div className="absolute top-0 right-0 w-16 h-16 md:w-20 md:h-20 bg-blue-200/20 rounded-full -translate-y-8 translate-x-8 md:-translate-y-10 md:translate-x-10"></div>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3 md:gap-4">
              <div className="bg-blue-500 p-2 md:p-3 rounded-lg md:rounded-xl shadow-lg">
                <Truck className="h-5 w-5 md:h-6 md:w-6 text-white" />
              </div>
              <div>
                <dt className="text-sm font-bold text-blue-800 uppercase tracking-wide">
                  Shipping Cost
                </dt>
                <dt className="text-xs text-blue-600 font-medium">
                  शिपिंग की लागत
                </dt>
                <dd className="text-2xl font-bold text-blue-900 mt-1" data-testid="text-shipping-total">
                  {formatCurrency(safeData.shippingTotal)}
                </dd>
                <dd className="text-xs text-blue-600 mt-1 font-medium">
                  {safeData.ordersWithShippingCharges > 0 
                    ? `${safeData.ordersWithShippingCharges} orders`
                    : 'Filters लगाएं calculate करने के लिए'
                  }
                </dd>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card className="bg-gradient-to-br from-green-50 to-green-100 border-2 border-green-200 rounded-xl md:rounded-2xl shadow-lg hover:shadow-xl transition-all duration-300 hover:scale-105">
        <CardContent className="p-4 md:p-6 relative overflow-hidden">
          <div className="absolute top-0 right-0 w-16 h-16 md:w-20 md:h-20 bg-green-200/20 rounded-full -translate-y-8 translate-x-8 md:-translate-y-10 md:translate-x-10"></div>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3 md:gap-4">
              <div className="bg-green-500 p-2 md:p-3 rounded-lg md:rounded-xl shadow-lg">
                <Coins className="h-5 w-5 md:h-6 md:w-6 text-white" />
              </div>
              <div>
                <dt className="text-sm font-bold text-green-800 uppercase tracking-wide">
                  COD Received
                </dt>
                <dt className="text-xs text-green-600 font-medium">
                  Product Value (Delivered COD)
                </dt>
                <dd className="text-2xl font-bold text-green-900 mt-1" data-testid="text-cod-total">
                  {formatCurrency(safeData.codTotal)}
                </dd>
                <dd className="text-xs text-green-600 mt-1 font-medium">
                  {safeData.ordersWithCodAmount > 0 
                    ? `${safeData.ordersWithCodAmount} delivered orders`
                    : 'Filters लगाएं'
                  }
                </dd>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card className="bg-gradient-to-br from-orange-50 to-orange-100 border-2 border-orange-200 rounded-xl md:rounded-2xl shadow-lg hover:shadow-xl transition-all duration-300 hover:scale-105">
        <CardContent className="p-4 md:p-6 relative overflow-hidden">
          <div className="absolute top-0 right-0 w-16 h-16 md:w-20 md:h-20 bg-orange-200/20 rounded-full -translate-y-8 translate-x-8 md:-translate-y-10 md:translate-x-10"></div>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3 md:gap-4">
              <div className="bg-orange-500 p-2 md:p-3 rounded-lg md:rounded-xl shadow-lg">
                <Package className="h-5 w-5 md:h-6 md:w-6 text-white" />
              </div>
              <div>
                <dt className="text-sm font-bold text-orange-800 uppercase tracking-wide">
                  Product Cost
                </dt>
                <dt className="text-xs text-orange-600 font-medium">
                  उत्पाद की लागत
                </dt>
                <dd className="text-2xl font-bold text-orange-900 mt-1" data-testid="text-product-cost">
                  {formatCurrency(safeData.productCostTotal)}
                </dd>
                <dd className="text-xs text-orange-600 mt-1 font-medium">
                  {safeData.ordersWithProductAmount > 0 
                    ? `${safeData.ordersWithProductAmount} delivered orders`
                    : 'Apply filters to calculate'
                  }
                </dd>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card className="bg-gradient-to-br from-red-50 to-red-100 border-2 border-red-200 rounded-xl md:rounded-2xl shadow-lg hover:shadow-xl transition-all duration-300 hover:scale-105">
        <CardContent className="p-4 md:p-6 relative overflow-hidden">
          <div className="absolute top-0 right-0 w-16 h-16 md:w-20 md:h-20 bg-red-200/20 rounded-full -translate-y-8 translate-x-8 md:-translate-y-10 md:translate-x-10"></div>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3 md:gap-4">
              <div className="bg-red-500 p-2 md:p-3 rounded-lg md:rounded-xl shadow-lg">
                <Undo className="h-5 w-5 md:h-6 md:w-6 text-white" />
              </div>
              <div>
                <dt className="text-sm font-bold text-red-800 uppercase tracking-wide">
                  RTS/RTO Reversal
                </dt>
                <dt className="text-xs text-red-600 font-medium">
                  वापसी की कटौती
                </dt>
                <dd className="text-2xl font-bold text-red-900 mt-1" data-testid="text-reversal-total">
                  {formatCurrency(safeData.rtsRtoReversalTotal)}
                </dd>
                <dd className="text-xs text-red-600 mt-1 font-medium">
                  Deductions for returns
                </dd>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card className="bg-gradient-to-br from-emerald-100 to-green-200 border-4 border-green-300 rounded-xl md:rounded-2xl shadow-2xl hover:shadow-3xl transition-all transform hover:scale-105">
        <CardContent className="p-6 md:p-8">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4 md:gap-6">
              <div className="bg-gradient-to-br from-green-600 to-emerald-700 p-3 md:p-4 rounded-xl md:rounded-2xl shadow-xl">
                <Wallet className="h-6 w-6 md:h-8 md:w-8 text-white" />
              </div>
              <div>
                <dt className="text-base md:text-lg font-bold text-green-800 uppercase tracking-wide">
                  Final Payable
                </dt>
                <dd className="text-3xl md:text-4xl font-bold text-green-900 mt-1 md:mt-2" data-testid="text-final-payable">
                  {formatCurrency(safeData.finalPayable)}
                </dd>
                <dd className="text-sm text-green-700 mt-1 md:mt-2 font-semibold">
                  Ready for payout
                </dd>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}