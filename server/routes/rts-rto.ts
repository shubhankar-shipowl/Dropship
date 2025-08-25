import type { Express } from "express";
import { storage } from "../storage";
import { insertRtsRtoReconciliationSchema } from "@shared/schema";

export function registerRtsRtoRoutes(app: Express): void {
  // Get pending RTS/RTO orders
  app.get('/api/rts-rto/pending', async (req, res) => {
    try {
      const { dropshipperEmail } = req.query;
      const pendingOrders = await storage.getPendingRtsRtoOrders(dropshipperEmail as string);
      res.json(pendingOrders);
    } catch (error) {
      console.error('Error fetching pending RTS/RTO orders:', error);
      res.status(500).json({ message: 'Error fetching pending RTS/RTO orders' });
    }
  });

  // Get RTS/RTO reconciliation history
  app.get('/api/rts-rto/history', async (req, res) => {
    try {
      const { dropshipperEmail, from, to } = req.query;
      const history = await storage.getRtsRtoHistory({
        dropshipperEmail: dropshipperEmail as string,
        from: from as string,
        to: to as string
      });
      res.json(history);
    } catch (error) {
      console.error('Error fetching RTS/RTO history:', error);
      res.status(500).json({ message: 'Error fetching RTS/RTO history' });
    }
  });

  // Process RTS/RTO reconciliation
  app.post('/api/rts-rto/reconcile', async (req, res) => {
    try {
      const reconciliationData = insertRtsRtoReconciliationSchema.parse(req.body);
      const result = await storage.processRtsRtoReconciliation(reconciliationData);
      res.json(result);
    } catch (error) {
      console.error('Error processing RTS/RTO reconciliation:', error);
      res.status(500).json({ message: 'Error processing RTS/RTO reconciliation' });
    }
  });

  // Auto-detect RTS/RTO reconciliation suggestions
  app.post('/api/rts-rto/auto-detect', async (req, res) => {
    try {
      const { orderDateFrom, orderDateTo, dropshipperEmail } = req.body;
      const suggestions = await storage.autoDetectRtsRtoReconciliations({
        orderDateFrom,
        orderDateTo,
        dropshipperEmail
      });
      res.json(suggestions);
    } catch (error) {
      console.error('Error auto-detecting RTS/RTO reconciliations:', error);
      res.status(500).json({ message: 'Error auto-detecting RTS/RTO reconciliations' });
    }
  });
}