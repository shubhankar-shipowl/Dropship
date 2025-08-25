import { RtsRtoReconciliation } from '@/components/rts-rto-reconciliation';

export default function RtsRtoReconciliationPage() {
  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto px-4 py-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold tracking-tight">RTS/RTO Reconciliation</h1>
          <p className="text-muted-foreground mt-2">
            Manage returned orders (RTS/RTO) and process payment reversals and adjustments
          </p>
        </div>
        
        <RtsRtoReconciliation />
      </div>
    </div>
  );
}