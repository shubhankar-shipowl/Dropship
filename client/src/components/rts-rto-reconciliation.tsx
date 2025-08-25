import React, { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { apiRequest } from '@/lib/queryClient';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Textarea } from '@/components/ui/textarea';
import { Badge } from '@/components/ui/badge';
import { useToast } from '@/hooks/use-toast';
import { AlertTriangle, CheckCircle, Clock, Search, RefreshCw, Calendar } from 'lucide-react';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';

interface PendingRtsRtoOrder {
  orderId: string;
  waybill: string | null;
  dropshipperEmail: string;
  productUid: string;
  productName: string;
  status: string;
  rtsRtoDate: Date | null;
  codAmount: string;
  originalPaymentStatus?: string;
}

interface RtsRtoReconciliation {
  id: string;
  orderId: string;
  waybill: string | null;
  dropshipperEmail: string;
  productUid: string;
  originalPayoutId?: string | null;
  originalPaidAmount: string;
  reversalAmount: string;
  rtsRtoStatus: string;
  rtsRtoDate: Date;
  reconciledOn: Date;
  reconciledBy?: string | null;
  notes?: string | null;
  status: string;
}

interface AutoDetectSuggestion {
  orderId: string;
  waybill: string | null;
  dropshipperEmail: string;
  productUid: string;
  suggestedReversalAmount: number;
  originalPaidAmount: number;
  rtsRtoStatus: string;
  confidence: 'high' | 'medium' | 'low';
  reason: string;
  previousStatus?: string;
  statusChangeDetected: boolean;
}

export function RtsRtoReconciliation() {
  const [selectedDropshipper, setSelectedDropshipper] = useState<string>('all');
  const [dateRange, setDateRange] = useState({
    from: '',
    to: ''
  });
  const [reconciliationForm, setReconciliationForm] = useState({
    orderId: '',
    waybill: '',
    dropshipperEmail: '',
    productUid: '',
    originalPaidAmount: 0,
    reversalAmount: 0,
    rtsRtoStatus: 'RTS' as 'RTS' | 'RTO' | 'RTO-Dispatched',
    rtsRtoDate: '',
    notes: '',
    reconciledBy: ''
  });
  
  const { toast } = useToast();
  const queryClient = useQueryClient();

  // Fetch dropshippers for filter
  const { data: dropshippers = [] } = useQuery<string[]>({
    queryKey: ['/api/dropshippers']
  });

  // Fetch pending RTS/RTO orders
  const { data: pendingOrders = [], isLoading: pendingLoading, refetch: refetchPending } = useQuery<PendingRtsRtoOrder[]>({
    queryKey: ['/api/rts-rto/pending', selectedDropshipper],
    queryFn: async () => {
      const url = selectedDropshipper && selectedDropshipper !== 'all' ? `/api/rts-rto/pending?dropshipperEmail=${selectedDropshipper}` : '/api/rts-rto/pending';
      const response = await fetch(url);
      if (!response.ok) throw new Error('Failed to fetch pending orders');
      return response.json();
    }
  });

  // Fetch RTS/RTO reconciliation history
  const { data: reconciliationHistory = [], isLoading: historyLoading, refetch: refetchHistory } = useQuery<RtsRtoReconciliation[]>({
    queryKey: ['/api/rts-rto/history', selectedDropshipper, dateRange.from, dateRange.to],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (selectedDropshipper && selectedDropshipper !== 'all') params.append('dropshipperEmail', selectedDropshipper);
      if (dateRange.from) params.append('from', dateRange.from);
      if (dateRange.to) params.append('to', dateRange.to);
      const response = await fetch(`/api/rts-rto/history?${params.toString()}`);
      if (!response.ok) throw new Error('Failed to fetch history');
      return response.json();
    }
  });

  // Auto-detect reconciliation suggestions
  const { data: suggestions = [], isLoading: suggestionsLoading, refetch: refetchSuggestions } = useQuery<AutoDetectSuggestion[]>({
    queryKey: ['/api/rts-rto/auto-detect', selectedDropshipper, dateRange.from, dateRange.to],
    queryFn: async () => {
      if (!dateRange.from || !dateRange.to) return [];
      const response = await fetch('/api/rts-rto/auto-detect', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          orderDateFrom: dateRange.from,
          orderDateTo: dateRange.to,
          dropshipperEmail: selectedDropshipper && selectedDropshipper !== 'all' ? selectedDropshipper : undefined
        })
      });
      if (!response.ok) throw new Error('Failed to auto-detect');
      return response.json();
    },
    enabled: !!(dateRange.from && dateRange.to)
  });

  // Process reconciliation mutation
  const reconcileMutation = useMutation({
    mutationFn: async (data: any) => {
      const response = await fetch('/api/rts-rto/reconcile', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(data)
      });
      if (!response.ok) throw new Error('Failed to process reconciliation');
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: "Reconciliation Processed",
        description: "RTS/RTO reconciliation has been successfully processed."
      });
      queryClient.invalidateQueries({ queryKey: ['/api/rts-rto/pending'] });
      queryClient.invalidateQueries({ queryKey: ['/api/rts-rto/history'] });
      setReconciliationForm({
        orderId: '',
        waybill: '',
        dropshipperEmail: '',
        productUid: '',
        originalPaidAmount: 0,
        reversalAmount: 0,
        rtsRtoStatus: 'RTS',
        rtsRtoDate: '',
        notes: '',
        reconciledBy: ''
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to process reconciliation",
        variant: "destructive"
      });
    }
  });

  const handleProcessReconciliation = (e: React.FormEvent) => {
    e.preventDefault();
    if (!reconciliationForm.orderId || !reconciliationForm.dropshipperEmail) {
      toast({
        title: "Validation Error",
        description: "Order ID and Dropshipper Email are required",
        variant: "destructive"
      });
      return;
    }

    reconcileMutation.mutate(reconciliationForm);
  };

  const fillFormFromSuggestion = (suggestion: AutoDetectSuggestion) => {
    setReconciliationForm({
      orderId: suggestion.orderId,
      waybill: suggestion.waybill || '',
      dropshipperEmail: suggestion.dropshipperEmail,
      productUid: suggestion.productUid,
      originalPaidAmount: suggestion.originalPaidAmount,
      reversalAmount: suggestion.suggestedReversalAmount,
      rtsRtoStatus: suggestion.rtsRtoStatus as 'RTS' | 'RTO' | 'RTO-Dispatched',
      rtsRtoDate: new Date().toISOString().split('T')[0],
      notes: suggestion.reason,
      reconciledBy: ''
    });
  };

  const fillFormFromPendingOrder = (order: PendingRtsRtoOrder) => {
    setReconciliationForm({
      orderId: order.orderId,
      waybill: order.waybill || '',
      dropshipperEmail: order.dropshipperEmail,
      productUid: order.productUid,
      originalPaidAmount: 0,
      reversalAmount: parseFloat(order.codAmount) || 0,
      rtsRtoStatus: order.status as 'RTS' | 'RTO' | 'RTO-Dispatched',
      rtsRtoDate: order.rtsRtoDate ? new Date(order.rtsRtoDate).toISOString().split('T')[0] : new Date().toISOString().split('T')[0],
      notes: `Auto-filled from ${order.status} order`,
      reconciledBy: ''
    });
  };

  const getConfidenceBadge = (confidence: string) => {
    const colors = {
      high: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200',
      medium: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200',
      low: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
    };
    return (
      <Badge className={colors[confidence as keyof typeof colors] || colors.low}>
        {confidence.toUpperCase()}
      </Badge>
    );
  };

  const getStatusBadge = (status: string) => {
    const colors = {
      RTS: 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200',
      RTO: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200',
      'RTO-Dispatched': 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200',
      pending: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200',
      processed: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
    };
    return (
      <Badge className={colors[status as keyof typeof colors] || 'bg-gray-100 text-gray-800'}>
        {status}
      </Badge>
    );
  };

  return (
    <div className="space-y-6" data-testid="rts-rto-reconciliation">
      {/* Filters */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Search className="h-5 w-5" />
            RTS/RTO Reconciliation Filters
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <Label htmlFor="dropshipper-select">Dropshipper (Optional)</Label>
              <Select value={selectedDropshipper} onValueChange={setSelectedDropshipper}>
                <SelectTrigger data-testid="select-dropshipper">
                  <SelectValue placeholder="All Dropshippers" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Dropshippers</SelectItem>
                  {dropshippers.map((email: string) => (
                    <SelectItem key={email} value={email}>{email}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div>
              <Label htmlFor="date-from">Date From</Label>
              <Input
                id="date-from"
                type="date"
                value={dateRange.from}
                onChange={(e) => setDateRange(prev => ({ ...prev, from: e.target.value }))}
                data-testid="input-date-from"
              />
            </div>
            <div>
              <Label htmlFor="date-to">Date To</Label>
              <Input
                id="date-to"
                type="date"
                value={dateRange.to}
                onChange={(e) => setDateRange(prev => ({ ...prev, to: e.target.value }))}
                data-testid="input-date-to"
              />
            </div>
          </div>
        </CardContent>
      </Card>

      <Tabs defaultValue="pending" className="w-full">
        <TabsList className="grid w-full grid-cols-4">
          <TabsTrigger value="pending" data-testid="tab-pending">
            Pending Orders ({pendingOrders.length})
          </TabsTrigger>
          <TabsTrigger value="suggestions" data-testid="tab-suggestions">
            Auto Suggestions ({suggestions.length})
          </TabsTrigger>
          <TabsTrigger value="process" data-testid="tab-process">
            Manual Process
          </TabsTrigger>
          <TabsTrigger value="history" data-testid="tab-history">
            History
          </TabsTrigger>
        </TabsList>

        {/* Pending Orders */}
        <TabsContent value="pending">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center justify-between">
                <span className="flex items-center gap-2">
                  <Clock className="h-5 w-5" />
                  Pending RTS/RTO Orders
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => refetchPending()}
                  disabled={pendingLoading}
                  data-testid="button-refresh-pending"
                >
                  <RefreshCw className={`h-4 w-4 ${pendingLoading ? 'animate-spin' : ''}`} />
                  Refresh
                </Button>
              </CardTitle>
            </CardHeader>
            <CardContent>
              {pendingLoading ? (
                <div className="text-center py-8">Loading pending orders...</div>
              ) : pendingOrders.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground">
                  No pending RTS/RTO orders found
                </div>
              ) : (
                <div className="space-y-4">
                  {pendingOrders.map((order: PendingRtsRtoOrder) => (
                    <div
                      key={order.orderId}
                      className="border rounded-lg p-4 hover:bg-muted/50 transition-colors"
                      data-testid={`pending-order-${order.orderId}`}
                    >
                      <div className="flex items-center justify-between">
                        <div className="space-y-2">
                          <div className="flex items-center gap-2">
                            <span className="font-medium">Order: {order.orderId}</span>
                            {getStatusBadge(order.status)}
                          </div>
                          <div className="text-sm text-muted-foreground">
                            <div>Product: {order.productName}</div>
                            <div>Dropshipper: {order.dropshipperEmail}</div>
                            <div>COD Amount: ₹{order.codAmount}</div>
                            {order.rtsRtoDate && (
                              <div>RTS/RTO Date: {new Date(order.rtsRtoDate).toLocaleDateString()}</div>
                            )}
                          </div>
                        </div>
                        <Button
                          variant="outline"
                          onClick={() => fillFormFromPendingOrder(order)}
                          data-testid={`button-process-${order.orderId}`}
                        >
                          Process Reconciliation
                        </Button>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Auto Suggestions */}
        <TabsContent value="suggestions">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center justify-between">
                <span className="flex items-center gap-2">
                  <AlertTriangle className="h-5 w-5" />
                  Auto-Detection Suggestions
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => refetchSuggestions()}
                  disabled={suggestionsLoading || !dateRange.from || !dateRange.to}
                  data-testid="button-refresh-suggestions"
                >
                  <RefreshCw className={`h-4 w-4 ${suggestionsLoading ? 'animate-spin' : ''}`} />
                  Auto-Detect
                </Button>
              </CardTitle>
            </CardHeader>
            <CardContent>
              {!dateRange.from || !dateRange.to ? (
                <div className="text-center py-8 text-muted-foreground">
                  Please select date range to auto-detect reconciliations
                </div>
              ) : suggestionsLoading ? (
                <div className="text-center py-8">Auto-detecting reconciliations...</div>
              ) : suggestions.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground">
                  No reconciliation suggestions found for the selected date range
                </div>
              ) : (
                <div className="space-y-4">
                  {suggestions.map((suggestion: AutoDetectSuggestion) => (
                    <div
                      key={suggestion.orderId}
                      className={`border rounded-lg p-4 hover:bg-muted/50 transition-colors ${
                        suggestion.statusChangeDetected ? 'border-orange-200 bg-orange-50 dark:bg-orange-950' : ''
                      }`}
                      data-testid={`suggestion-${suggestion.orderId}`}
                    >
                      <div className="flex items-center justify-between">
                        <div className="space-y-2">
                          <div className="flex items-center gap-2">
                            <span className="font-medium">Order: {suggestion.orderId}</span>
                            {getStatusBadge(suggestion.rtsRtoStatus)}
                            {getConfidenceBadge(suggestion.confidence)}
                            {suggestion.statusChangeDetected && (
                              <Badge className="bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200">
                                Status Changed
                              </Badge>
                            )}
                          </div>
                          <div className="text-sm text-muted-foreground">
                            <div>Dropshipper: {suggestion.dropshipperEmail}</div>
                            {suggestion.previousStatus && (
                              <div className="font-medium text-orange-700 dark:text-orange-300">
                                Previous Status: {suggestion.previousStatus} → {suggestion.rtsRtoStatus}
                              </div>
                            )}
                            <div>Suggested Reversal: ₹{suggestion.suggestedReversalAmount.toFixed(2)}</div>
                            <div>Original Paid: ₹{suggestion.originalPaidAmount.toFixed(2)}</div>
                            <div className="mt-1 p-2 bg-gray-50 dark:bg-gray-800 rounded text-xs">
                              <strong>Reason:</strong> {suggestion.reason}
                            </div>
                          </div>
                        </div>
                        <Button
                          variant={suggestion.statusChangeDetected ? "default" : "outline"}
                          onClick={() => fillFormFromSuggestion(suggestion)}
                          data-testid={`button-apply-suggestion-${suggestion.orderId}`}
                          className={suggestion.statusChangeDetected ? "bg-orange-500 hover:bg-orange-600 text-white" : ""}
                        >
                          {suggestion.statusChangeDetected ? "Process Now" : "Apply Suggestion"}
                        </Button>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Manual Processing Form */}
        <TabsContent value="process">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <CheckCircle className="h-5 w-5" />
                Manual RTS/RTO Reconciliation
              </CardTitle>
            </CardHeader>
            <CardContent>
              <form onSubmit={handleProcessReconciliation} className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <Label htmlFor="order-id">Order ID *</Label>
                    <Input
                      id="order-id"
                      value={reconciliationForm.orderId}
                      onChange={(e) => setReconciliationForm(prev => ({ ...prev, orderId: e.target.value }))}
                      required
                      data-testid="input-order-id"
                    />
                  </div>
                  <div>
                    <Label htmlFor="waybill">Waybill</Label>
                    <Input
                      id="waybill"
                      value={reconciliationForm.waybill}
                      onChange={(e) => setReconciliationForm(prev => ({ ...prev, waybill: e.target.value }))}
                      data-testid="input-waybill"
                    />
                  </div>
                  <div>
                    <Label htmlFor="dropshipper-email">Dropshipper Email *</Label>
                    <Input
                      id="dropshipper-email"
                      type="email"
                      value={reconciliationForm.dropshipperEmail}
                      onChange={(e) => setReconciliationForm(prev => ({ ...prev, dropshipperEmail: e.target.value }))}
                      required
                      data-testid="input-dropshipper-email"
                    />
                  </div>
                  <div>
                    <Label htmlFor="product-uid">Product UID</Label>
                    <Input
                      id="product-uid"
                      value={reconciliationForm.productUid}
                      onChange={(e) => setReconciliationForm(prev => ({ ...prev, productUid: e.target.value }))}
                      data-testid="input-product-uid"
                    />
                  </div>
                  <div>
                    <Label htmlFor="original-paid-amount">Original Paid Amount (₹)</Label>
                    <Input
                      id="original-paid-amount"
                      type="number"
                      step="0.01"
                      value={reconciliationForm.originalPaidAmount}
                      onChange={(e) => setReconciliationForm(prev => ({ ...prev, originalPaidAmount: parseFloat(e.target.value) || 0 }))}
                      data-testid="input-original-paid-amount"
                    />
                  </div>
                  <div>
                    <Label htmlFor="reversal-amount">Reversal Amount (₹) *</Label>
                    <Input
                      id="reversal-amount"
                      type="number"
                      step="0.01"
                      value={reconciliationForm.reversalAmount}
                      onChange={(e) => setReconciliationForm(prev => ({ ...prev, reversalAmount: parseFloat(e.target.value) || 0 }))}
                      required
                      data-testid="input-reversal-amount"
                    />
                  </div>
                  <div>
                    <Label htmlFor="rts-rto-status">RTS/RTO Status</Label>
                    <Select
                      value={reconciliationForm.rtsRtoStatus}
                      onValueChange={(value) => setReconciliationForm(prev => ({ ...prev, rtsRtoStatus: value as 'RTS' | 'RTO' | 'RTO-Dispatched' }))}
                    >
                      <SelectTrigger data-testid="select-rts-rto-status">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="RTS">RTS</SelectItem>
                        <SelectItem value="RTO">RTO</SelectItem>
                        <SelectItem value="RTO-Dispatched">RTO-Dispatched</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <Label htmlFor="rts-rto-date">RTS/RTO Date</Label>
                    <Input
                      id="rts-rto-date"
                      type="date"
                      value={reconciliationForm.rtsRtoDate}
                      onChange={(e) => setReconciliationForm(prev => ({ ...prev, rtsRtoDate: e.target.value }))}
                      data-testid="input-rts-rto-date"
                    />
                  </div>
                </div>
                <div>
                  <Label htmlFor="reconciled-by">Reconciled By</Label>
                  <Input
                    id="reconciled-by"
                    value={reconciliationForm.reconciledBy}
                    onChange={(e) => setReconciliationForm(prev => ({ ...prev, reconciledBy: e.target.value }))}
                    placeholder="Enter your name or ID"
                    data-testid="input-reconciled-by"
                  />
                </div>
                <div>
                  <Label htmlFor="notes">Notes</Label>
                  <Textarea
                    id="notes"
                    value={reconciliationForm.notes}
                    onChange={(e) => setReconciliationForm(prev => ({ ...prev, notes: e.target.value }))}
                    placeholder="Add any relevant notes about this reconciliation..."
                    data-testid="textarea-notes"
                  />
                </div>
                <Button
                  type="submit"
                  disabled={reconcileMutation.isPending}
                  className="w-full"
                  data-testid="button-submit-reconciliation"
                >
                  {reconcileMutation.isPending ? 'Processing...' : 'Process Reconciliation'}
                </Button>
              </form>
            </CardContent>
          </Card>
        </TabsContent>

        {/* History */}
        <TabsContent value="history">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center justify-between">
                <span className="flex items-center gap-2">
                  <Calendar className="h-5 w-5" />
                  Reconciliation History
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => refetchHistory()}
                  disabled={historyLoading}
                  data-testid="button-refresh-history"
                >
                  <RefreshCw className={`h-4 w-4 ${historyLoading ? 'animate-spin' : ''}`} />
                  Refresh
                </Button>
              </CardTitle>
            </CardHeader>
            <CardContent>
              {historyLoading ? (
                <div className="text-center py-8">Loading reconciliation history...</div>
              ) : reconciliationHistory.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground">
                  No reconciliation history found
                </div>
              ) : (
                <div className="space-y-4">
                  {reconciliationHistory.map((record: RtsRtoReconciliation) => (
                    <div
                      key={record.id}
                      className="border rounded-lg p-4"
                      data-testid={`history-record-${record.id}`}
                    >
                      <div className="flex items-center justify-between mb-2">
                        <div className="flex items-center gap-2">
                          <span className="font-medium">Order: {record.orderId}</span>
                          {getStatusBadge(record.rtsRtoStatus)}
                          {getStatusBadge(record.status)}
                        </div>
                        <div className="text-sm text-muted-foreground">
                          {new Date(record.reconciledOn).toLocaleDateString()}
                        </div>
                      </div>
                      <div className="grid grid-cols-2 gap-4 text-sm">
                        <div>
                          <div><strong>Dropshipper:</strong> {record.dropshipperEmail}</div>
                          <div><strong>Original Amount:</strong> ₹{record.originalPaidAmount}</div>
                          <div><strong>Reversal Amount:</strong> ₹{record.reversalAmount}</div>
                        </div>
                        <div>
                          <div><strong>RTS/RTO Date:</strong> {new Date(record.rtsRtoDate).toLocaleDateString()}</div>
                          {record.reconciledBy && <div><strong>Reconciled By:</strong> {record.reconciledBy}</div>}
                          {record.notes && <div><strong>Notes:</strong> {record.notes}</div>}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}