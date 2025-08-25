import { Truck, CheckCircle, Filter, Play, Loader2, Calendar, Lightbulb } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useQuery } from "@tanstack/react-query";
import { useState, useEffect } from "react";

interface DateRangeFiltersProps {
  orderDateFrom: string;
  orderDateTo: string;
  deliveredDateFrom: string;
  deliveredDateTo: string;
  selectedDropshipper: string;
  dropshippers: string[];
  onDateRangeChange?: (type: "delivered" | "order", from: string, to: string) => void;
  onDropshipperChange: (email: string) => void;
  onApplyFilters: () => Promise<void>;
  isCalculating: boolean;
}

export default function DateRangeFilters({
  orderDateFrom,
  orderDateTo,
  deliveredDateFrom,
  deliveredDateTo,
  selectedDropshipper,
  dropshippers,
  onDateRangeChange,
  onDropshipperChange,
  onApplyFilters,
  isCalculating,
}: DateRangeFiltersProps) {
  const [showRecommendation, setShowRecommendation] = useState(false);

  // Clean date ranges fetch
  const { data: dateRanges, isLoading: isLoadingRanges } = useQuery<{
    firstOrderDate: string | null;
    lastOrderDate: string | null;
    firstDeliveryDate: string | null;
    lastDeliveryDate: string | null;
    totalOrders: number;
    deliveredOrders: number;
  }>({
    queryKey: ['date-ranges', selectedDropshipper],
    queryFn: async () => {
      const response = await fetch(`/api/dropshipper-date-ranges/${encodeURIComponent(selectedDropshipper)}`);
      if (!response.ok) throw new Error('Failed to fetch date ranges');
      return response.json();
    },
    enabled: !!selectedDropshipper && selectedDropshipper !== 'all',
  });

  // Auto-show recommendation when data is loaded
  useEffect(() => {
    if (dateRanges && selectedDropshipper && selectedDropshipper !== 'all') {
      setShowRecommendation(true);
    } else {
      setShowRecommendation(false);
    }
  }, [dateRanges, selectedDropshipper]);

  const handleApplyRecommendation = () => {
    if (dateRanges && typeof onDateRangeChange === 'function') {
      if (dateRanges.firstOrderDate && dateRanges.lastOrderDate) {
        onDateRangeChange('order', dateRanges.firstOrderDate, dateRanges.lastOrderDate);
      }
      if (dateRanges.firstDeliveryDate && dateRanges.lastDeliveryDate) {
        onDateRangeChange('delivered', dateRanges.firstDeliveryDate, dateRanges.lastDeliveryDate);
      }
      setShowRecommendation(false);
    }
  };
  return (
    <Card className="mb-4 md:mb-6 mx-2 md:mx-0">
      <CardHeader className="p-4 md:p-6">
        <CardTitle className="text-lg md:text-xl" data-testid="text-filters-title">Date Range Filters</CardTitle>
      </CardHeader>
      <CardContent className="p-4 md:p-6 pt-0">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 md:gap-6">
          {/* Order Date Range */}
          <div>
            <h3 className="text-sm md:text-base font-medium text-gray-700 mb-3 flex items-center gap-2">
              <Truck className="h-4 w-4 text-blue-500" />
              <span className="hidden sm:inline">Order Date Range (for Shipping Costs)</span>
              <span className="sm:hidden">Order Dates</span>
            </h3>
            <div className="text-xs md:text-sm text-gray-500 mb-3 bg-blue-50 p-2 md:p-3 rounded border-l-2 border-blue-300">
              📦 <strong>Shipping charges calculated for all orders except cancelled orders</strong>
            </div>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 md:gap-4">
              <div>
                <Label htmlFor="order-date-from" className="text-xs md:text-sm font-medium text-gray-700">
                  From Date
                </Label>
                <Input
                  id="order-date-from"
                  type="date"
                  value={orderDateFrom}
                  onChange={(e) => {
                    if (typeof onDateRangeChange === 'function') {
                      onDateRangeChange('order', e.target.value, orderDateTo);
                    }
                  }}
                  className="mt-1 text-sm"
                  data-testid="input-order-date-from"
                />
              </div>
              <div>
                <Label htmlFor="order-date-to" className="text-xs font-medium text-gray-700">
                  To Date
                </Label>
                <Input
                  id="order-date-to"
                  type="date"
                  value={orderDateTo}
                  onChange={(e) => {
                    if (typeof onDateRangeChange === 'function') {
                      onDateRangeChange('order', orderDateFrom, e.target.value);
                    }
                  }}
                  className="mt-1"
                  data-testid="input-order-date-to"
                />
              </div>
            </div>
          </div>

          {/* Delivered Date Range */}
          <div>
            <h3 className="text-sm md:text-base font-medium text-gray-700 mb-3 flex items-center gap-2">
              <CheckCircle className="h-4 w-4 text-green-500" />
              <span className="hidden sm:inline">Delivered Date Range (for COD & Product Costs)</span>
              <span className="sm:hidden">Delivered Dates</span>
            </h3>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 md:gap-4">
              <div>
                <Label htmlFor="delivered-date-from" className="text-xs font-medium text-gray-700">
                  From Date
                </Label>
                <Input
                  id="delivered-date-from"
                  type="date"
                  value={deliveredDateFrom}
                  onChange={(e) => {
                    if (typeof onDateRangeChange === 'function') {
                      onDateRangeChange('delivered', e.target.value, deliveredDateTo);
                    }
                  }}
                  className="mt-1"
                  data-testid="input-delivered-date-from"
                />
              </div>
              <div>
                <Label htmlFor="delivered-date-to" className="text-xs font-medium text-gray-700">
                  To Date
                </Label>
                <Input
                  id="delivered-date-to"
                  type="date"
                  value={deliveredDateTo}
                  onChange={(e) => {
                    if (typeof onDateRangeChange === 'function') {
                      onDateRangeChange('delivered', deliveredDateFrom, e.target.value);
                    }
                  }}
                  className="mt-1"
                  data-testid="input-delivered-date-to"
                />
              </div>
            </div>
          </div>
        </div>

        {/* Dropshipper Filter */}
        <div className="mt-4 md:mt-6">
          <Label className="text-sm md:text-base font-medium text-gray-700 mb-2 flex items-center gap-2">
            <Filter className="h-4 w-4" />
            <span className="hidden sm:inline">Filter by Dropshipper</span>
            <span className="sm:hidden">Dropshipper</span>
          </Label>
          <div className="flex flex-col sm:flex-row items-start sm:items-center gap-3 sm:gap-4">
            <Select value={selectedDropshipper} onValueChange={onDropshipperChange}>
              <SelectTrigger className="w-full sm:max-w-md text-sm" data-testid="select-dropshipper">
                <SelectValue placeholder="All Dropshippers" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all" data-testid="option-all-dropshippers">All Dropshippers</SelectItem>
                {dropshippers.map((email) => (
                  <SelectItem key={email} value={email} data-testid={`option-dropshipper-${email}`}>
                    {email}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            
            {/* Apply Filters Button */}
            <Button 
              onClick={onApplyFilters}
              disabled={isCalculating}
              className="w-full sm:min-w-[120px] sm:w-auto bg-blue-600 hover:bg-blue-700 text-sm md:text-base"
              data-testid="button-apply-filters"
            >
              {isCalculating ? (
                <>
                  <Loader2 className="mr-2 h-3 w-3 md:h-4 md:w-4 animate-spin" />
                  <span className="hidden sm:inline">Processing...</span>
                  <span className="sm:hidden">Wait...</span>
                </>
              ) : (
                <>
                  <Play className="mr-2 h-3 w-3 md:h-4 md:w-4" />
                  <span className="hidden sm:inline">Apply Filters</span>
                  <span className="sm:hidden">Apply</span>
                </>
              )}
            </Button>
          </div>
        </div>

        {/* Smart Date Range Suggestion */}
        {showRecommendation && dateRanges && !isLoadingRanges && (
          <div className="mt-6 bg-gradient-to-r from-emerald-50 via-blue-50 to-purple-50 border-2 border-blue-300 rounded-xl p-5 shadow-lg">
            <div className="text-center mb-4">
              <div className="inline-flex items-center gap-2 bg-white/80 px-4 py-2 rounded-full shadow-sm">
                <Lightbulb className="h-5 w-5 text-amber-500" />
                <span className="text-lg font-bold text-gray-800">💡 Smart Suggestion</span>
              </div>
            </div>
            
            <div className="bg-white/90 backdrop-blur-sm rounded-lg p-4 mb-4 border border-blue-200">
              <h3 className="text-center text-lg font-semibold text-gray-800 mb-4">
                {selectedDropshipper} के लिए सबसे अच्छी Date Range
              </h3>
              
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-4">
                <div className="bg-blue-50 rounded-lg p-3 border border-blue-200">
                  <div className="flex items-center gap-2 mb-2">
                    <Truck className="h-4 w-4 text-blue-600" />
                    <span className="font-semibold text-blue-800">Order Dates</span>
                  </div>
                  <p className="text-sm text-gray-700 mb-1">
                    <span className="font-medium">{dateRanges.firstOrderDate}</span> से <span className="font-medium">{dateRanges.lastOrderDate}</span>
                  </p>
                  <p className="text-xs text-blue-600">कुल {dateRanges.totalOrders} orders</p>
                </div>
                
                <div className="bg-green-50 rounded-lg p-3 border border-green-200">
                  <div className="flex items-center gap-2 mb-2">
                    <CheckCircle className="h-4 w-4 text-green-600" />
                    <span className="font-semibold text-green-800">Delivery Dates</span>
                  </div>
                  <p className="text-sm text-gray-700 mb-1">
                    <span className="font-medium">{dateRanges.firstDeliveryDate}</span> से <span className="font-medium">{dateRanges.lastDeliveryDate}</span>
                  </p>
                  <p className="text-xs text-green-600">{dateRanges.deliveredOrders} delivered orders</p>
                </div>
              </div>
            </div>
            
            <div className="flex flex-col sm:flex-row gap-3 justify-center">
              <Button
                onClick={handleApplyRecommendation}
                className="bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 text-white font-semibold py-3 px-6 rounded-lg shadow-lg transform hover:scale-105 transition-all duration-200"
                data-testid="button-apply-recommendation"
              >
                <Calendar className="h-4 w-4 mr-2" />
                ✨ Use These Dates
              </Button>
              <Button
                variant="outline"
                onClick={() => setShowRecommendation(false)}
                className="border-2 border-gray-300 text-gray-600 hover:bg-gray-50 font-medium py-3 px-4 rounded-lg"
              >
                Not Now
              </Button>
            </div>
          </div>
        )}

        {isLoadingRanges && selectedDropshipper && selectedDropshipper !== 'all' && (
          <div className="mt-6 p-6 bg-gradient-to-r from-blue-50 to-purple-50 border-2 border-blue-200 rounded-xl">
            <div className="flex flex-col items-center gap-3">
              <div className="relative">
                <Loader2 className="h-8 w-8 animate-spin text-blue-600" />
                <div className="absolute inset-0 rounded-full border-2 border-blue-200 animate-pulse"></div>
              </div>
              <span className="text-base font-medium text-gray-700">📊 Analyzing {selectedDropshipper} data...</span>
              <span className="text-sm text-gray-500">Best date ranges loading कर रहे हैं</span>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}