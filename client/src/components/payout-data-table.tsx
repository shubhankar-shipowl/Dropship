import { useState } from "react";
import { Search, ArrowUpDown, ChevronLeft, ChevronRight } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { PayoutRow } from "@shared/schema";

interface PayoutDataTableProps {
  rows: PayoutRow[];
  adjustments: Array<{
    orderId: string;
    reason: string;
    amount: number;
    reference: string;
  }>;
  isLoading?: boolean;
}

export default function PayoutDataTable({ rows, adjustments, isLoading }: PayoutDataTableProps) {
  const [searchQuery, setSearchQuery] = useState("");
  const [sortField, setSortField] = useState<keyof PayoutRow>("orderId");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc");
  const [currentPage, setCurrentPage] = useState(1);
  const [itemsPerPage] = useState(10);

  const formatCurrency = (amount: number) => {
    // Display zero amounts as ₹0 without hiding them
    if (amount === 0) return '₹0';
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      maximumFractionDigits: 0,
    }).format(amount);
  };

  const getStatusBadge = (status: string) => {
    const statusLower = status.toLowerCase();
    if (statusLower.includes('delivered')) {
      return <Badge variant="default" className="bg-green-100 text-green-800">Delivered</Badge>;
    }
    if (statusLower.includes('rts') || statusLower.includes('rto')) {
      return <Badge variant="destructive" className="bg-red-100 text-red-800">RTS</Badge>;
    }
    return <Badge variant="secondary">{status}</Badge>;
  };

  // Handle undefined data with safe defaults
  const safeRows = rows || [];
  const safeAdjustments = adjustments || [];

  // Filter rows based on search query
  const filteredRows = safeRows.filter((row) => {
    const query = searchQuery.toLowerCase();
    return (
      row.orderId.toLowerCase().includes(query) ||
      row.waybill?.toLowerCase().includes(query) ||
      row.product.toLowerCase().includes(query) ||
      row.skuUid.toLowerCase().includes(query)
    );
  });

  // Sort rows with safe filtering
  const sortedRows = [...filteredRows].sort((a, b) => {
    const aValue = a[sortField];
    const bValue = b[sortField];
    
    if (typeof aValue === 'string' && typeof bValue === 'string') {
      return sortDirection === 'asc' 
        ? aValue.localeCompare(bValue)
        : bValue.localeCompare(aValue);
    }
    
    if (typeof aValue === 'number' && typeof bValue === 'number') {
      return sortDirection === 'asc' ? aValue - bValue : bValue - aValue;
    }
    
    return 0;
  });

  // Pagination
  const totalPages = Math.ceil(sortedRows.length / itemsPerPage);
  const startIndex = (currentPage - 1) * itemsPerPage;
  const paginatedRows = sortedRows.slice(startIndex, startIndex + itemsPerPage);

  const handleSort = (field: keyof PayoutRow) => {
    if (field === sortField) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('asc');
    }
    setCurrentPage(1);
  };

  if (isLoading) {
    return (
      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <div className="relative">
            <Input
              placeholder="Search orders..."
              className="w-64 pr-10"
              disabled
            />
            <Search className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
          </div>
        </div>
        <div className="rounded-md border">
          <div className="p-8 text-center">
            <div className="animate-spin mx-auto mb-4 w-8 h-8 border-4 border-primary border-t-transparent rounded-full"></div>
            <p className="text-sm text-gray-500">Loading payout data...</p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-3 md:space-y-4 mx-2 md:mx-0">
      {/* Search and Controls */}
      <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-3 sm:gap-0">
        <div className="relative w-full sm:w-64">
          <Input
            placeholder="Search orders..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pr-10 text-sm"
            data-testid="input-search-orders"
          />
          <Search className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
        </div>
        <div className="text-xs sm:text-sm text-gray-500">
          Showing {Math.min(startIndex + 1, sortedRows.length)} to {Math.min(startIndex + itemsPerPage, sortedRows.length)} of {sortedRows.length} results
        </div>
      </div>

      {/* Adjustments Summary */}
      {safeAdjustments.length > 0 && (
        <div className="bg-orange-50 border border-orange-200 rounded-md p-4">
          <h4 className="text-sm font-medium text-orange-800 mb-2">RTS/RTO Adjustments</h4>
          <p className="text-sm text-orange-700">
            {safeAdjustments.length} order(s) have been adjusted due to status changes from Delivered to RTS/RTO.
          </p>
        </div>
      )}

      {/* Table */}
      <div className="rounded-md border overflow-x-auto">
        <Table className="min-w-[800px]">
          <TableHeader>
            <TableRow>
              <TableHead>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-auto p-0 font-medium hover:bg-transparent"
                  onClick={() => handleSort('orderId')}
                  data-testid="sort-order-id"
                >
                  <div className="flex items-center space-x-1">
                    <span>Order ID</span>
                    <ArrowUpDown className="h-3 w-3" />
                  </div>
                </Button>
              </TableHead>
              <TableHead>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-auto p-0 font-medium hover:bg-transparent"
                  onClick={() => handleSort('waybill')}
                  data-testid="sort-waybill"
                >
                  <div className="flex items-center space-x-1">
                    <span>Waybill</span>
                    <ArrowUpDown className="h-3 w-3" />
                  </div>
                </Button>
              </TableHead>
              <TableHead>Product</TableHead>
              <TableHead>SKU/UID</TableHead>
              <TableHead className="text-center">Shipped Qty</TableHead>
              <TableHead className="text-center">Delivered Qty</TableHead>
              <TableHead className="text-right">COD Rate</TableHead>
              <TableHead className="text-right">COD Received</TableHead>
              <TableHead className="text-right">Shipping Cost</TableHead>
              <TableHead className="text-right">Product Cost</TableHead>
              <TableHead className="text-right">Payable</TableHead>
              <TableHead>Status</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {paginatedRows.map((row, index) => (
              <TableRow key={`${row.orderId}-${index}`} data-testid={`row-order-${row.orderId}`}>
                <TableCell className="font-medium">{row.orderId}</TableCell>
                <TableCell className="text-gray-500">{row.waybill || "-"}</TableCell>
                <TableCell>{row.product}</TableCell>
                <TableCell className="text-gray-500">{row.skuUid}</TableCell>
                <TableCell className="text-center">{row.shippedQty}</TableCell>
                <TableCell className="text-center">{row.deliveredQty}</TableCell>
                <TableCell className="text-right">{formatCurrency(row.codRate)}</TableCell>
                <TableCell className="text-right font-medium text-green-600">
                  {row.codReceived > 0 ? formatCurrency(row.codReceived) : "-"}
                </TableCell>
                <TableCell className="text-right">{formatCurrency(row.shippingCost)}</TableCell>
                <TableCell className="text-right">{formatCurrency(row.productCost)}</TableCell>
                <TableCell className={`text-right font-semibold ${
                  row.payable > 0 ? 'text-primary' : 'text-red-600'
                }`}>
                  {formatCurrency(row.payable)}
                </TableCell>
                <TableCell>
                  {getStatusBadge(row.status)}
                </TableCell>
              </TableRow>
            ))}
            {paginatedRows.length === 0 && (
              <TableRow>
                <TableCell colSpan={12} className="text-center py-8 text-gray-500">
                  {searchQuery ? 'No orders match your search criteria.' : 'No payout data available.'}
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-3 sm:gap-0">
          <div className="text-xs sm:text-sm text-gray-500">
            Page {currentPage} of {totalPages}
          </div>
          <div className="flex items-center space-x-1 sm:space-x-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setCurrentPage(currentPage - 1)}
              disabled={currentPage === 1}
              data-testid="button-previous-page"
              className="text-xs sm:text-sm px-2 sm:px-3"
            >
              <ChevronLeft className="h-3 w-3 sm:h-4 sm:w-4" />
              <span className="hidden sm:inline ml-1">Previous</span>
            </Button>
            <div className="hidden md:flex items-center space-x-1">
              {[...Array(Math.min(5, totalPages))].map((_, i) => {
                const pageNum = i + 1;
                return (
                  <Button
                    key={pageNum}
                    variant={currentPage === pageNum ? "default" : "outline"}
                    size="sm"
                    onClick={() => setCurrentPage(pageNum)}
                    data-testid={`button-page-${pageNum}`}
                    className="text-xs sm:text-sm px-2 sm:px-3"
                  >
                    {pageNum}
                  </Button>
                );
              })}
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setCurrentPage(currentPage + 1)}
              disabled={currentPage === totalPages}
              data-testid="button-next-page"
              className="text-xs sm:text-sm px-2 sm:px-3"
            >
              <span className="hidden sm:inline mr-1">Next</span>
              <ChevronRight className="h-3 w-3 sm:h-4 sm:w-4" />
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}