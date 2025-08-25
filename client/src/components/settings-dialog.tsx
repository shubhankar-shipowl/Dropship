import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { Plus, Upload, Edit, Trash2, X } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useToast } from "@/hooks/use-toast";
import { apiRequest, queryClient } from "@/lib/queryClient";
import type { ProductPrice, ShippingRate } from "@shared/schema";

interface SettingsDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  missingData?: {
    missingPrices: Array<{ dropshipperEmail: string; productUid: string; productName: string; sku: string | null }>;
    missingRates: string[];
  };
  onSettingsUpdated: () => void;
}

interface EditingPrice {
  id?: string;
  dropshipperEmail: string;
  productUid: string;
  productName: string;
  sku: string;
  productCostPerUnit: string;
  currency: string;
}

interface EditingRate {
  id?: string;
  shippingProvider: string;
  shippingRatePerOrder: string;
  currency: string;
}

export default function SettingsDialog({ 
  open, 
  onOpenChange, 
  missingData,
  onSettingsUpdated 
}: SettingsDialogProps) {
  const [activeTab, setActiveTab] = useState("product-prices");
  const [editingPrice, setEditingPrice] = useState<EditingPrice | null>(null);
  const [editingRate, setEditingRate] = useState<EditingRate | null>(null);
  const { toast } = useToast();

  // Fetch data
  const { data: productPrices = [], refetch: refetchPrices } = useQuery<ProductPrice[]>({
    queryKey: ['/api/product-prices'],
    enabled: open,
  });

  const { data: shippingRates = [], refetch: refetchRates } = useQuery<ShippingRate[]>({
    queryKey: ['/api/shipping-rates'],
    enabled: open,
  });

  // Mutations
  const saveProductPriceMutation = useMutation({
    mutationFn: async (price: Omit<EditingPrice, 'id'>) => {
      const response = await apiRequest('POST', '/api/product-prices', {
        ...price,
        productCostPerUnit: parseFloat(price.productCostPerUnit),
      });
      return response.json();
    },
    onSuccess: () => {
      toast({ title: "Product price saved successfully" });
      refetchPrices();
      queryClient.invalidateQueries({ queryKey: ['/api/missing-data'] });
      setEditingPrice(null);
      onSettingsUpdated();
    },
    onError: () => {
      toast({ 
        title: "Failed to save product price", 
        variant: "destructive" 
      });
    },
  });

  const deleteProductPriceMutation = useMutation({
    mutationFn: async (id: string) => {
      await apiRequest('DELETE', `/api/product-prices/${id}`);
    },
    onSuccess: () => {
      toast({ title: "Product price deleted successfully" });
      refetchPrices();
      onSettingsUpdated();
    },
    onError: () => {
      toast({ 
        title: "Failed to delete product price", 
        variant: "destructive" 
      });
    },
  });

  const saveShippingRateMutation = useMutation({
    mutationFn: async (rate: Omit<EditingRate, 'id'>) => {
      const response = await apiRequest('POST', '/api/shipping-rates', {
        ...rate,
        shippingRatePerOrder: parseFloat(rate.shippingRatePerOrder),
      });
      return response.json();
    },
    onSuccess: () => {
      toast({ title: "Shipping rate saved successfully" });
      refetchRates();
      queryClient.invalidateQueries({ queryKey: ['/api/missing-data'] });
      setEditingRate(null);
      onSettingsUpdated();
    },
    onError: () => {
      toast({ 
        title: "Failed to save shipping rate", 
        variant: "destructive" 
      });
    },
  });

  const deleteShippingRateMutation = useMutation({
    mutationFn: async (id: string) => {
      await apiRequest('DELETE', `/api/shipping-rates/${id}`);
    },
    onSuccess: () => {
      toast({ title: "Shipping rate deleted successfully" });
      refetchRates();
      onSettingsUpdated();
    },
    onError: () => {
      toast({ 
        title: "Failed to delete shipping rate", 
        variant: "destructive" 
      });
    },
  });

  const handleAddMissingPrices = () => {
    if (!missingData?.missingPrices) return;

    missingData.missingPrices.forEach(item => {
      const newPrice: EditingPrice = {
        dropshipperEmail: item.dropshipperEmail,
        productUid: item.productUid,
        productName: item.productName,
        sku: item.sku || "",
        productCostPerUnit: "0.00",
        currency: "INR",
      };
      saveProductPriceMutation.mutate(newPrice);
    });
  };

  const handleAddMissingRates = () => {
    if (!missingData?.missingRates) return;

    missingData.missingRates.forEach(provider => {
      const newRate: EditingRate = {
        shippingProvider: provider,
        shippingRatePerOrder: "0.00",
        currency: "INR",
      };
      saveShippingRateMutation.mutate(newRate);
    });
  };

  const formatCurrency = (amount: string) => {
    const num = parseFloat(amount);
    return isNaN(num) ? "₹0.00" : new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
    }).format(num);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-6xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center justify-between" data-testid="text-settings-title">
            Settings
            <Button 
              variant="ghost" 
              size="icon" 
              onClick={() => onOpenChange(false)}
              data-testid="button-close-settings"
            >
              <X className="h-4 w-4" />
            </Button>
          </DialogTitle>
          <DialogDescription>
            Manage product prices and shipping rates for payout calculations
          </DialogDescription>
        </DialogHeader>

        <Tabs value={activeTab} onValueChange={setActiveTab}>
          <TabsList className="grid w-full grid-cols-2">
            <TabsTrigger value="product-prices" data-testid="tab-product-prices">
              Product Prices
            </TabsTrigger>
            <TabsTrigger value="shipping-rates" data-testid="tab-shipping-rates">
              Shipping Rates
            </TabsTrigger>
          </TabsList>

          {/* Product Prices Tab */}
          <TabsContent value="product-prices" className="space-y-4">
            <div className="flex justify-between items-center">
              <h4 className="text-md font-medium text-gray-900">Product Price Management</h4>
              <div className="flex space-x-2">
                {missingData && missingData.missingPrices.length > 0 && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handleAddMissingPrices}
                    data-testid="button-add-missing-prices"
                  >
                    Add Missing ({missingData.missingPrices.length})
                  </Button>
                )}
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setEditingPrice({
                    dropshipperEmail: "",
                    productUid: "",
                    productName: "",
                    sku: "",
                    productCostPerUnit: "0.00",
                    currency: "INR",
                  })}
                  data-testid="button-add-price-row"
                >
                  <Plus className="mr-2 h-4 w-4" />
                  Add Row
                </Button>
              </div>
            </div>

            {/* Add/Edit Form */}
            {editingPrice && (
              <div className="border rounded-lg p-4 bg-gray-50">
                <h5 className="font-medium mb-3">
                  {editingPrice.id ? "Edit Product Price" : "Add Product Price"}
                </h5>
                <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                  <div>
                    <Label htmlFor="edit-dropshipper-email">Dropshipper Email</Label>
                    <Input
                      id="edit-dropshipper-email"
                      value={editingPrice.dropshipperEmail}
                      onChange={(e) => setEditingPrice({...editingPrice, dropshipperEmail: e.target.value})}
                      placeholder="dropshipper@example.com"
                      data-testid="input-edit-dropshipper-email"
                    />
                  </div>
                  <div>
                    <Label htmlFor="edit-product-uid">Product UID</Label>
                    <Input
                      id="edit-product-uid"
                      value={editingPrice.productUid}
                      onChange={(e) => setEditingPrice({...editingPrice, productUid: e.target.value})}
                      placeholder="PRD-001"
                      data-testid="input-edit-product-uid"
                    />
                  </div>
                  <div>
                    <Label htmlFor="edit-product-name">Product Name</Label>
                    <Input
                      id="edit-product-name"
                      value={editingPrice.productName}
                      onChange={(e) => setEditingPrice({...editingPrice, productName: e.target.value})}
                      placeholder="Product Name"
                      data-testid="input-edit-product-name"
                    />
                  </div>
                  <div>
                    <Label htmlFor="edit-sku">SKU</Label>
                    <Input
                      id="edit-sku"
                      value={editingPrice.sku}
                      onChange={(e) => setEditingPrice({...editingPrice, sku: e.target.value})}
                      placeholder="SKU-001"
                      data-testid="input-edit-sku"
                    />
                  </div>
                  <div>
                    <Label htmlFor="edit-cost-per-unit">Cost Per Unit</Label>
                    <Input
                      id="edit-cost-per-unit"
                      type="number"
                      step="0.01"
                      min="0"
                      value={editingPrice.productCostPerUnit}
                      onChange={(e) => setEditingPrice({...editingPrice, productCostPerUnit: e.target.value})}
                      placeholder="0.00"
                      data-testid="input-edit-cost-per-unit"
                    />
                  </div>
                  <div>
                    <Label htmlFor="edit-currency">Currency</Label>
                    <Input
                      id="edit-currency"
                      value={editingPrice.currency}
                      onChange={(e) => setEditingPrice({...editingPrice, currency: e.target.value})}
                      placeholder="INR"
                      data-testid="input-edit-currency"
                    />
                  </div>
                </div>
                <div className="flex justify-end space-x-2 mt-4">
                  <Button
                    variant="outline"
                    onClick={() => setEditingPrice(null)}
                    data-testid="button-cancel-price-edit"
                  >
                    Cancel
                  </Button>
                  <Button
                    onClick={() => saveProductPriceMutation.mutate(editingPrice)}
                    disabled={saveProductPriceMutation.isPending}
                    data-testid="button-save-price"
                  >
                    {saveProductPriceMutation.isPending ? "Saving..." : "Save"}
                  </Button>
                </div>
              </div>
            )}

            {/* Table */}
            <div className="border rounded-lg overflow-hidden">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Dropshipper Email</TableHead>
                    <TableHead>Product UID</TableHead>
                    <TableHead>Product Name</TableHead>
                    <TableHead>SKU</TableHead>
                    <TableHead>Cost Per Unit</TableHead>
                    <TableHead>Currency</TableHead>
                    <TableHead>Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {productPrices.map((price) => (
                    <TableRow key={price.id} data-testid={`row-price-${price.id}`}>
                      <TableCell className="font-medium">{price.dropshipperEmail}</TableCell>
                      <TableCell>{price.productUid}</TableCell>
                      <TableCell>{price.productName}</TableCell>
                      <TableCell>{price.sku || "-"}</TableCell>
                      <TableCell>{formatCurrency(price.productCostPerUnit)}</TableCell>
                      <TableCell>{price.currency}</TableCell>
                      <TableCell>
                        <div className="flex space-x-2">
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => setEditingPrice({
                              id: price.id,
                              dropshipperEmail: price.dropshipperEmail,
                              productUid: price.productUid,
                              productName: price.productName,
                              sku: price.sku || "",
                              productCostPerUnit: price.productCostPerUnit,
                              currency: price.currency,
                            })}
                            data-testid={`button-edit-price-${price.id}`}
                          >
                            <Edit className="h-4 w-4" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => deleteProductPriceMutation.mutate(price.id)}
                            disabled={deleteProductPriceMutation.isPending}
                            data-testid={`button-delete-price-${price.id}`}
                          >
                            <Trash2 className="h-4 w-4 text-red-500" />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                  {productPrices.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={7} className="text-center py-8 text-gray-500">
                        No product prices configured. Click "Add Row" to get started.
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>
          </TabsContent>

          {/* Shipping Rates Tab */}
          <TabsContent value="shipping-rates" className="space-y-4">
            <div className="flex justify-between items-center">
              <h4 className="text-md font-medium text-gray-900">Shipping Rate Management</h4>
              <div className="flex space-x-2">
                {missingData && missingData.missingRates.length > 0 && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handleAddMissingRates}
                    data-testid="button-add-missing-rates"
                  >
                    Add Missing ({missingData.missingRates.length})
                  </Button>
                )}
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setEditingRate({
                    shippingProvider: "",
                    shippingRatePerOrder: "0.00",
                    currency: "INR",
                  })}
                  data-testid="button-add-rate-row"
                >
                  <Plus className="mr-2 h-4 w-4" />
                  Add Row
                </Button>
              </div>
            </div>

            {/* Add/Edit Form */}
            {editingRate && (
              <div className="border rounded-lg p-4 bg-gray-50">
                <h5 className="font-medium mb-3">
                  {editingRate.id ? "Edit Shipping Rate" : "Add Shipping Rate"}
                </h5>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <div>
                    <Label htmlFor="edit-shipping-provider">Shipping Provider</Label>
                    <Input
                      id="edit-shipping-provider"
                      value={editingRate.shippingProvider}
                      onChange={(e) => setEditingRate({...editingRate, shippingProvider: e.target.value})}
                      placeholder="Blue Dart"
                      data-testid="input-edit-shipping-provider"
                    />
                  </div>
                  <div>
                    <Label htmlFor="edit-rate-per-order">Rate Per Order</Label>
                    <Input
                      id="edit-rate-per-order"
                      type="number"
                      step="0.01"
                      min="0"
                      value={editingRate.shippingRatePerOrder}
                      onChange={(e) => setEditingRate({...editingRate, shippingRatePerOrder: e.target.value})}
                      placeholder="0.00"
                      data-testid="input-edit-rate-per-order"
                    />
                  </div>
                  <div>
                    <Label htmlFor="edit-rate-currency">Currency</Label>
                    <Input
                      id="edit-rate-currency"
                      value={editingRate.currency}
                      onChange={(e) => setEditingRate({...editingRate, currency: e.target.value})}
                      placeholder="INR"
                      data-testid="input-edit-rate-currency"
                    />
                  </div>
                </div>
                <div className="flex justify-end space-x-2 mt-4">
                  <Button
                    variant="outline"
                    onClick={() => setEditingRate(null)}
                    data-testid="button-cancel-rate-edit"
                  >
                    Cancel
                  </Button>
                  <Button
                    onClick={() => saveShippingRateMutation.mutate(editingRate)}
                    disabled={saveShippingRateMutation.isPending}
                    data-testid="button-save-rate"
                  >
                    {saveShippingRateMutation.isPending ? "Saving..." : "Save"}
                  </Button>
                </div>
              </div>
            )}

            {/* Table */}
            <div className="border rounded-lg overflow-hidden">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Shipping Provider</TableHead>
                    <TableHead>Rate Per Order</TableHead>
                    <TableHead>Currency</TableHead>
                    <TableHead>Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {shippingRates.map((rate) => (
                    <TableRow key={rate.id} data-testid={`row-rate-${rate.id}`}>
                      <TableCell className="font-medium">{rate.shippingProvider}</TableCell>
                      <TableCell>{formatCurrency(rate.shippingRatePerOrder)}</TableCell>
                      <TableCell>{rate.currency}</TableCell>
                      <TableCell>
                        <div className="flex space-x-2">
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => setEditingRate({
                              id: rate.id,
                              shippingProvider: rate.shippingProvider,
                              shippingRatePerOrder: rate.shippingRatePerOrder,
                              currency: rate.currency,
                            })}
                            data-testid={`button-edit-rate-${rate.id}`}
                          >
                            <Edit className="h-4 w-4" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => deleteShippingRateMutation.mutate(rate.id)}
                            disabled={deleteShippingRateMutation.isPending}
                            data-testid={`button-delete-rate-${rate.id}`}
                          >
                            <Trash2 className="h-4 w-4 text-red-500" />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                  {shippingRates.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={4} className="text-center py-8 text-gray-500">
                        No shipping rates configured. Click "Add Row" to get started.
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>
          </TabsContent>
        </Tabs>
      </DialogContent>
    </Dialog>
  );
}