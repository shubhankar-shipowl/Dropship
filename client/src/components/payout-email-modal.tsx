import React, { useState, useEffect, useRef } from "react";
import { Mail, Send, X, Plus, Tag } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useToast } from "@/hooks/use-toast";
import type { PayoutSummary } from "@shared/schema";

interface PayoutEmailModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  dropshipperEmail: string;
  summary: PayoutSummary;
  orderDateFrom: string;
  orderDateTo: string;
  deliveredDateFrom: string;
  deliveredDateTo: string;
}

export default function PayoutEmailModal({
  open,
  onOpenChange,
  dropshipperEmail,
  summary,
  orderDateFrom,
  orderDateTo,
  deliveredDateFrom,
  deliveredDateTo,
}: PayoutEmailModalProps) {
  const { toast } = useToast();
  const [isSending, setIsSending] = useState(false);
  const [labels, setLabels] = useState<Array<{ id: string; name: string }>>([]);
  const [selectedLabel, setSelectedLabel] = useState("Dropshipper");
  const [newLabelName, setNewLabelName] = useState("");
  const [showNewLabelInput, setShowNewLabelInput] = useState(false);
  const [isLoadingLabels, setIsLoadingLabels] = useState(false);
  const [isCreatingLabel, setIsCreatingLabel] = useState(false);
  
  // Store the selected label in a ref to track changes - ALWAYS use this as source of truth
  const selectedLabelRef = useRef<string>("Dropshipper");
  
  // Initialize ref with current state
  useEffect(() => {
    if (selectedLabel) {
      selectedLabelRef.current = selectedLabel;
      console.log('🔄 selectedLabel state updated to:', selectedLabel);
      console.log('🔄 selectedLabelRef.current updated to:', selectedLabelRef.current);
    }
  }, [selectedLabel]);

  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      maximumFractionDigits: 0,
    }).format(amount);
  };

  const formatDate = (date: string) => {
    return new Date(date).toLocaleDateString('en-IN', {
      year: 'numeric',
      month: 'long',
      day: 'numeric',
    });
  };

  // Auto-generate email subject
  const generateSubject = () => {
    return `Payout Statement - ${formatDate(orderDateFrom)} to ${formatDate(orderDateTo)}`;
  };

  // Auto-generate email content
  const generateContent = () => {
    return `Dear Dropshipper,

Please find below your payout statement for the period:

Order Date Range: ${formatDate(orderDateFrom)} to ${formatDate(orderDateTo)}
Delivered Date Range: ${formatDate(deliveredDateFrom)} to ${formatDate(deliveredDateTo)}

PAYOUT SUMMARY:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
COD Received:        ${formatCurrency(summary.codTotal)}
Shipping Cost:       ${formatCurrency(summary.shippingTotal)}
Product Cost:        ${formatCurrency(summary.productCostTotal)}
RTS/RTO Reversal:    ${formatCurrency(summary.rtsRtoReversalTotal)}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FINAL PAYABLE:       ${formatCurrency(summary.finalPayable)}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Order Statistics:
• Total Orders Processed: ${summary.totalOrdersProcessed}
• Orders with Shipping Charges: ${summary.ordersWithShippingCharges}
• Orders with Product Amount: ${summary.ordersWithProductAmount}
• COD Orders: ${summary.ordersWithCodAmount}

Please review the above statement. If you have any questions or concerns, please contact us.

Best regards,
Payout Team`;
  };

  const [to, setTo] = useState(dropshipperEmail);
  const [subject, setSubject] = useState(generateSubject());
  const [content, setContent] = useState(generateContent());

  // Load Gmail labels when modal opens
  useEffect(() => {
    if (open) {
      loadLabels();
      // Reset to default only if modal was just opened (not if label was already selected)
      // Don't reset selectedLabel here - keep the user's selection
    }
  }, [open]);

  const loadLabels = async () => {
    setIsLoadingLabels(true);
    try {
      const response = await fetch('/api/gmail-labels');
      const data = await response.json();
      
      if (data.success && data.labels) {
        setLabels(data.labels);
        // Ensure "Dropshipper" is selected if it exists, otherwise keep it as default
        const hasDropshipper = data.labels.some((l: { name: string }) => l.name === 'Dropshipper');
        if (hasDropshipper && selectedLabel === 'Dropshipper') {
          // Label exists and is selected - good
        } else if (!hasDropshipper && selectedLabel === 'Dropshipper') {
          // Label doesn't exist but is selected - it will be created when email is sent
        }
      }
    } catch (error) {
      console.error('Error loading labels:', error);
      // Don't show error toast - labels are optional
    } finally {
      setIsLoadingLabels(false);
    }
  };

  const handleCreateLabel = async () => {
    if (!newLabelName.trim()) {
      toast({
        title: "Error",
        description: "Please enter a label name",
        variant: "destructive",
      });
      return;
    }

    setIsCreatingLabel(true);
    try {
      const response = await fetch('/api/gmail-labels', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: newLabelName.trim() }),
      });

      const data = await response.json();

      if (data.success && data.label) {
        // Add new label to list
        const updatedLabels = [...labels, data.label].sort((a, b) => a.name.localeCompare(b.name));
        setLabels(updatedLabels);
        // Set the newly created label as selected
        const newLabelName = data.label.name;
        console.log('='.repeat(60));
        console.log('🔄 CREATING AND SELECTING NEW LABEL');
        console.log('🔄 New label name:', newLabelName);
        console.log('🔄 Current selectedLabel state:', selectedLabel);
        console.log('🔄 Current selectedLabelRef.current:', selectedLabelRef.current);
        
        // CRITICAL: Update ref FIRST (immediate, synchronous)
        selectedLabelRef.current = newLabelName;
        console.log('✅ Ref updated IMMEDIATELY to:', selectedLabelRef.current);
        
        // Then update state (async, but ref is already updated)
        setSelectedLabel(newLabelName);
        console.log('✅ State update triggered (will be:', newLabelName, 'after render)');
        
        setNewLabelName("");
        setShowNewLabelInput(false);
        console.log('✅ Label created and selected:', newLabelName);
        console.log('✅ Verification - ref is now:', selectedLabelRef.current);
        console.log('='.repeat(60));
        toast({
          title: "Label Created",
          description: `Label "${newLabelName}" has been created and selected`,
        });
      } else {
        throw new Error(data.message || 'Failed to create label');
      }
    } catch (error: any) {
      console.error('Error creating label:', error);
      toast({
        title: "Error Creating Label",
        description: error.message || "Failed to create label. Please try again.",
        variant: "destructive",
      });
    } finally {
      setIsCreatingLabel(false);
    }
  };

  const handleSend = async () => {
    if (!to || !subject || !content) {
      toast({
        title: "Error",
        description: "Please fill in all fields",
        variant: "destructive",
      });
      return;
    }

    // CRITICAL: Always use ref as source of truth (it's updated immediately when label is created)
    // React state updates are async, so selectedLabel might still be "Dropshipper"
    // but selectedLabelRef.current will have the latest value immediately
    const refValue = selectedLabelRef.current;
    const stateValue = selectedLabel;
    
    // Priority: ref value (most up-to-date) > state value > default
    // Only use state if ref is empty/undefined, or if ref is placeholder
    let currentLabel: string;
    if (refValue && refValue !== "__create_new__" && refValue.trim() !== "") {
      // Ref has a valid value - use it (this is the source of truth)
      currentLabel = refValue.trim();
    } else if (stateValue && stateValue !== "__create_new__" && stateValue.trim() !== "") {
      // Ref is empty/invalid, but state has a value - use state and update ref
      currentLabel = stateValue.trim();
      selectedLabelRef.current = currentLabel; // Sync ref with state
    } else {
      // Both are empty/invalid - use default
      currentLabel = "Dropshipper";
    }
    
    const labelToUse = currentLabel;
    
    console.log('='.repeat(60));
    console.log('📧 PREPARING TO SEND EMAIL');
    console.log('📧 selectedLabel state (might be stale):', JSON.stringify(stateValue));
    console.log('📧 selectedLabelRef.current (source of truth):', JSON.stringify(refValue));
    console.log('📧 currentLabel (chosen):', JSON.stringify(currentLabel));
    console.log('📧 labelToUse (final, after validation):', JSON.stringify(labelToUse));
    console.log('📧 Will send labelName to backend:', JSON.stringify(labelToUse));
    
    // Validation: If we just created a label, it MUST be in the ref
    if (refValue && refValue !== "Dropshipper" && refValue !== "__create_new__") {
      console.log('✅ Using newly created/selected label from ref:', refValue);
    } else if (stateValue && stateValue !== "Dropshipper" && stateValue !== "__create_new__") {
      console.log('⚠️ Using label from state (ref might not be updated):', stateValue);
      // Update ref to match state
      selectedLabelRef.current = stateValue;
    } else {
      console.log('⚠️ No custom label found, using default "Dropshipper"');
    }
    console.log('='.repeat(60));

    setIsSending(true);
    try {
      const requestBody = {
        to,
        subject,
        content,
        summary,
        orderDateFrom,
        orderDateTo,
        deliveredDateFrom,
        deliveredDateTo,
        dropshipperEmail: dropshipperEmail, // Pass dropshipper email for Excel filtering
        labelName: labelToUse, // Pass selected label name (not the placeholder)
      };
      
      console.log('📤 Sending email request with body:', JSON.stringify({
        ...requestBody,
        content: requestBody.content.substring(0, 50) + '...',
      }));
      console.log('📤 Label name being sent:', JSON.stringify(labelToUse));
      
      const response = await fetch('/api/send-payout-email', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody),
      });

      if (!response.ok) {
        const errorData = await response.json();
        console.error('Email sending error:', errorData);
        throw new Error(errorData.message || errorData.error || 'Failed to send email');
      }

      const result = await response.json();
      console.log('Email sent successfully:', result);
      console.log('Label used:', selectedLabel);
      console.log('Label applied:', result.labelApplied);
      console.log('Label name:', result.labelName);

      const labelMessage = result.labelApplied 
        ? ` with "${result.labelName || selectedLabel}" label`
        : '';

      toast({
        title: "Email Sent Successfully!",
        description: `Email has been sent to ${to}${labelMessage}`,
      });
      onOpenChange(false);
    } catch (error: any) {
      console.error('Email sending error:', error);
      let errorMessage = error.message || "Failed to send email. Please try again.";
      
      // Provide more helpful error messages
      if (errorMessage.includes('authentication')) {
        errorMessage = "SMTP authentication failed. Please check your email configuration. For Gmail, make sure you're using an App Password.";
      } else if (errorMessage.includes('connection')) {
        errorMessage = "Could not connect to SMTP server. Please check your internet connection and SMTP settings.";
      } else if (errorMessage.includes('rejected')) {
        errorMessage = "Email was rejected by the server. Please check the recipient email address.";
      }

      toast({
        title: "Error Sending Email",
        description: errorMessage,
        variant: "destructive",
      });
    } finally {
      setIsSending(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[600px]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Mail className="h-5 w-5" />
            Send Payout Email
          </DialogTitle>
          <DialogDescription>
            Review and edit the email before sending to the dropshipper
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-4">
          <div className="space-y-2">
            <Label htmlFor="to">To</Label>
            <Input
              id="to"
              value={to}
              onChange={(e) => setTo(e.target.value)}
              placeholder="dropshipper@example.com"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="subject">Subject</Label>
            <Input
              id="subject"
              value={subject}
              onChange={(e) => setSubject(e.target.value)}
              placeholder="Email subject"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="content">Content</Label>
            <Textarea
              id="content"
              value={content}
              onChange={(e) => setContent(e.target.value)}
              placeholder="Email content"
              className="min-h-[300px] font-mono text-sm"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="label">Gmail Label</Label>
            <div className="flex gap-2">
              <Select
                value={selectedLabel}
                onValueChange={(value) => {
                  console.log('🔄 Select value changed to:', value);
                  if (value === "__create_new__") {
                    setShowNewLabelInput(true);
                    setSelectedLabel("__create_new__");
                    selectedLabelRef.current = "__create_new__";
                  } else {
                    setSelectedLabel(value);
                    selectedLabelRef.current = value; // Update ref immediately
                    setShowNewLabelInput(false);
                    console.log('✅ Label selected:', value, '(ref updated to:', selectedLabelRef.current, ')');
                  }
                }}
                disabled={isLoadingLabels}
              >
                <SelectTrigger id="label" className="flex-1">
                  <div className="flex items-center gap-2">
                    <Tag className="h-4 w-4" />
                    <SelectValue placeholder="Select a label">
                      {selectedLabel === "__create_new__" ? "Create New Label" : selectedLabel}
                    </SelectValue>
                  </div>
                </SelectTrigger>
                <SelectContent>
                  {/* Always show "Dropshipper" as default option */}
                  {!labels.some(l => l.name === 'Dropshipper') && (
                    <SelectItem value="Dropshipper">
                      Dropshipper
                    </SelectItem>
                  )}
                  {labels.map((label) => (
                    <SelectItem key={label.id} value={label.name}>
                      {label.name}
                    </SelectItem>
                  ))}
                  <SelectItem value="__create_new__" className="text-primary font-medium">
                    <div className="flex items-center gap-2">
                      <Plus className="h-4 w-4" />
                      Create New Label
                    </div>
                  </SelectItem>
                </SelectContent>
              </Select>
              {showNewLabelInput && (
                <div className="flex gap-2 flex-1">
                  <Input
                    placeholder="New label name"
                    value={newLabelName}
                    onChange={(e) => setNewLabelName(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        handleCreateLabel();
                      } else if (e.key === 'Escape') {
                        setShowNewLabelInput(false);
                        setSelectedLabel("Dropshipper");
                      }
                    }}
                    autoFocus
                  />
                  <Button
                    type="button"
                    size="sm"
                    onClick={handleCreateLabel}
                    disabled={isCreatingLabel || !newLabelName.trim()}
                  >
                    {isCreatingLabel ? "Creating..." : "Create"}
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    onClick={() => {
                      setShowNewLabelInput(false);
                      setSelectedLabel("Dropshipper");
                      setNewLabelName("");
                    }}
                  >
                    <X className="h-4 w-4" />
                  </Button>
                </div>
              )}
            </div>
            {selectedLabel !== "__create_new__" && !showNewLabelInput && (
              <p className="text-xs text-muted-foreground">
                This label will be applied to the email in Gmail (only works with Gmail API)
              </p>
            )}
          </div>
        </div>
        <DialogFooter>
          <Button
            variant="outline"
            onClick={() => onOpenChange(false)}
            disabled={isSending}
          >
            <X className="h-4 w-4 mr-2" />
            Cancel
          </Button>
          <Button onClick={handleSend} disabled={isSending}>
            {isSending ? (
              <>
                <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
                Sending...
              </>
            ) : (
              <>
                <Send className="h-4 w-4 mr-2" />
                Send Email
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

