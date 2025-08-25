import { Express } from 'express';
import { storage } from '../storage';

export function registerTransparencyRoutes(app: Express): void {
  // Configuration summary endpoint
  app.get('/api/transparency/config-summary', async (req, res) => {
    try {
      const { dropshipperEmail } = req.query;
      const summary = await storage.getConfigurationSummary(dropshipperEmail as string);
      res.json(summary);
    } catch (error) {
      console.error('Error fetching config summary:', error);
      res.status(500).json({ message: 'Error fetching configuration summary' });
    }
  });

  // Orders endpoint
  app.get('/api/transparency/orders', async (req, res) => {
    try {
      const { dropshipperEmail, orderDateFrom, orderDateTo, deliveredDateFrom, deliveredDateTo } = req.query;
      const orders = await storage.getAllOrderData();
      res.json(orders);
    } catch (error) {
      console.error('Error fetching orders for transparency:', error);
      res.status(500).json({ message: 'Error fetching orders' });
    }
  });

  // Upload sessions endpoint
  app.get('/api/transparency/uploads', async (req, res) => {
    try {
      const uploads = await storage.getAllUploadSessions?.() || [];
      res.json(uploads);
    } catch (error) {
      console.error('Error fetching upload sessions:', error);
      res.status(500).json({ message: 'Error fetching upload sessions' });
    }
  });

  // Product prices endpoint
  app.get('/api/transparency/product-prices', async (req, res) => {
    try {
      const { dropshipperEmail } = req.query;
      const prices = await storage.getProductPrices();
      
      // Filter by dropshipper if specified
      const filteredPrices = dropshipperEmail && dropshipperEmail !== 'all'
        ? prices.filter(price => price.dropshipperEmail === dropshipperEmail)
        : prices;
        
      res.json(filteredPrices);
    } catch (error) {
      console.error('Error fetching product prices for transparency:', error);
      res.status(500).json({ message: 'Error fetching product prices' });
    }
  });

  // Shipping rates endpoint
  app.get('/api/transparency/shipping-rates', async (req, res) => {
    try {
      const { dropshipperEmail } = req.query;
      const rates = await storage.getShippingRates();
      
      // If dropshipper filter is provided, we need to filter based on related products
      // For now, return all rates since shipping rates don't have direct dropshipper association
      res.json(rates);
    } catch (error) {
      console.error('Error fetching shipping rates for transparency:', error);
      res.status(500).json({ message: 'Error fetching shipping rates' });
    }
  });

  // Clear data by table endpoint
  app.delete('/api/transparency/clear/:tableName', async (req, res) => {
    try {
      const { tableName } = req.params;
      
      switch (tableName) {
        case 'orders':
          await storage.clearAllOrders();
          break;
        case 'uploads':
          await storage.clearAllUploadSessions?.();
          break;
        case 'product-prices':
          await storage.clearAllProductPrices?.();
          break;
        case 'shipping-rates':
          await storage.clearAllShippingRates?.();
          break;
        default:
          return res.status(400).json({ message: 'Invalid table name' });
      }
      
      res.json({ message: `${tableName} data cleared successfully` });
    } catch (error) {
      console.error(`Error clearing ${req.params.tableName} data:`, error);
      res.status(500).json({ message: `Error clearing ${req.params.tableName} data` });
    }
  });
}