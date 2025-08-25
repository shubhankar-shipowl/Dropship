import type { Express } from "express";
import { storage } from "../storage";

export function registerAnalyticsRoutes(app: Express): void {
  // Analytics summary
  app.get('/api/analytics/summary', async (req, res) => {
    try {
      console.log('Fetching analytics summary...');
      
      // Get upload sessions count
      const uploadSessions = await storage.getAllUploadSessions?.() || [];
      console.log(`Found ${uploadSessions.length} uploads`);
      
      // Get total orders count
      const allOrders = await storage.getAllOrderData();
      console.log(`Found ${allOrders.length} orders`);
      
      // Calculate basic statistics
      const totalUploads = uploadSessions.length;
      const totalOrders = allOrders.length;
      
      // Calculate date ranges efficiently for large datasets
      let earliestOrder: Date | null = null;
      let latestOrder: Date | null = null;
      
      if (allOrders.length > 0) {
        let minTime = Infinity;
        let maxTime = -Infinity;
        
        for (const order of allOrders) {
          const orderDate = new Date(order.orderDate);
          if (!isNaN(orderDate.getTime())) {
            const time = orderDate.getTime();
            if (time < minTime) minTime = time;
            if (time > maxTime) maxTime = time;
          }
        }
        
        earliestOrder = minTime !== Infinity ? new Date(minTime) : null;
        latestOrder = maxTime !== -Infinity ? new Date(maxTime) : null;
      }
      
      // Get unique dropshippers
      const uniqueDropshippers = await storage.getUniqueDropshippers();
      
      // Status breakdown
      const statusCounts: Record<string, number> = {};
      allOrders.forEach(order => {
        const status = order.status.toLowerCase();
        statusCounts[status] = (statusCounts[status] || 0) + 1;
      });
      
      // Extract specific counts for frontend
      const deliveredOrders = statusCounts['delivered'] || 0;
      const rtsOrders = statusCounts['rts'] || 0;
      const cancelledOrders = statusCounts['cancelled'] || 0;
      
      // Calculate active dropshippers (those with orders)
      const activeDropshippers = uniqueDropshippers.length;
      
      const analytics = {
        totalUploads,
        totalOrders,
        cancelledOrders,
        deliveredOrders,
        rtsOrders,
        totalDropshippers: uniqueDropshippers.length,
        activeDropshippers,
        dateRange: {
          earliest: earliestOrder?.toISOString().split('T')[0] || null,
          latest: latestOrder?.toISOString().split('T')[0] || null
        },
        statusBreakdown: Object.entries(statusCounts)
          .map(([status, count]) => ({ status, count }))
          .sort((a, b) => b.count - a.count)
          .slice(0, 10), // Top 10 statuses
        recentActivity: {
          lastUpload: uploadSessions.length > 0 ? uploadSessions[uploadSessions.length - 1]?.uploadedAt : null,
          ordersThisMonth: allOrders.filter(o => {
            const orderDate = new Date(o.orderDate);
            const now = new Date();
            return orderDate.getMonth() === now.getMonth() && orderDate.getFullYear() === now.getFullYear();
          }).length
        }
      };
      
      res.json(analytics);
    } catch (error) {
      console.error('Error fetching analytics summary:', error);
      res.status(500).json({ message: 'Error fetching analytics summary' });
    }
  });

  // Dropshipper performance
  app.get('/api/analytics/dropshipper-performance', async (req, res) => {
    try {
      const { dateFrom, dateTo } = req.query;
      
      const allOrders = await storage.getAllOrderData();
      
      // Filter by date range if provided
      let filteredOrders = allOrders;
      if (dateFrom && dateTo) {
        const startDate = new Date(dateFrom as string);
        const endDate = new Date(dateTo as string);
        filteredOrders = allOrders.filter(order => {
          const orderDate = new Date(order.orderDate);
          return orderDate >= startDate && orderDate <= endDate;
        });
      }
      
      // Group by dropshipper
      const dropshipperStats: Record<string, {
        totalOrders: number;
        deliveredOrders: number;
        cancelledOrders: number;
        totalValue: number;
        deliveredValue: number;
        products: Set<string>;
      }> = {};
      
      filteredOrders.forEach(order => {
        const email = order.dropshipperEmail;
        if (!dropshipperStats[email]) {
          dropshipperStats[email] = {
            totalOrders: 0,
            deliveredOrders: 0,
            cancelledOrders: 0,
            totalValue: 0,
            deliveredValue: 0,
            products: new Set()
          };
        }
        
        const stats = dropshipperStats[email];
        stats.totalOrders++;
        stats.products.add(order.productUid);
        
        const orderValue = parseFloat(order.productValue) || 0;
        stats.totalValue += orderValue;
        
        const status = order.status.toLowerCase();
        if (status.includes('delivered')) {
          stats.deliveredOrders++;
          stats.deliveredValue += orderValue;
        } else if (status.includes('cancel')) {
          stats.cancelledOrders++;
        }
      });
      
      // Convert to array and calculate percentages
      const performance = Object.entries(dropshipperStats).map(([email, stats]) => ({
        dropshipperEmail: email,
        totalOrders: stats.totalOrders,
        deliveredOrders: stats.deliveredOrders,
        cancelledOrders: stats.cancelledOrders,
        deliveryRate: stats.totalOrders > 0 ? (stats.deliveredOrders / stats.totalOrders * 100) : 0,
        cancellationRate: stats.totalOrders > 0 ? (stats.cancelledOrders / stats.totalOrders * 100) : 0,
        totalValue: Math.round(stats.totalValue),
        deliveredValue: Math.round(stats.deliveredValue),
        uniqueProducts: stats.products.size,
        avgOrderValue: stats.totalOrders > 0 ? Math.round(stats.totalValue / stats.totalOrders) : 0
      })).sort((a, b) => b.totalOrders - a.totalOrders);
      
      res.json(performance);
    } catch (error) {
      console.error('Error fetching dropshipper performance:', error);
      res.status(500).json({ message: 'Error fetching dropshipper performance' });
    }
  });

  // Order trends
  app.get('/api/analytics/order-trends', async (req, res) => {
    try {
      const { period = 'daily', dateFrom, dateTo } = req.query;
      
      const allOrders = await storage.getAllOrderData();
      
      // Filter by date range if provided
      let filteredOrders = allOrders;
      if (dateFrom && dateTo) {
        const startDate = new Date(dateFrom as string);
        const endDate = new Date(dateTo as string);
        filteredOrders = allOrders.filter(order => {
          const orderDate = new Date(order.orderDate);
          return orderDate >= startDate && orderDate <= endDate;
        });
      }
      
      // Group by period
      const trends: Record<string, {
        date: string;
        totalOrders: number;
        deliveredOrders: number;
        cancelledOrders: number;
        totalValue: number;
      }> = {};
      
      filteredOrders.forEach(order => {
        const orderDate = new Date(order.orderDate);
        let key: string;
        
        if (period === 'daily') {
          key = orderDate.toISOString().split('T')[0];
        } else if (period === 'weekly') {
          const weekStart = new Date(orderDate);
          weekStart.setDate(orderDate.getDate() - orderDate.getDay());
          key = weekStart.toISOString().split('T')[0];
        } else if (period === 'monthly') {
          key = `${orderDate.getFullYear()}-${String(orderDate.getMonth() + 1).padStart(2, '0')}`;
        } else {
          key = orderDate.toISOString().split('T')[0]; // Default to daily
        }
        
        if (!trends[key]) {
          trends[key] = {
            date: key,
            totalOrders: 0,
            deliveredOrders: 0,
            cancelledOrders: 0,
            totalValue: 0
          };
        }
        
        const trend = trends[key];
        trend.totalOrders++;
        trend.totalValue += parseFloat(order.productValue) || 0;
        
        const status = order.status.toLowerCase();
        if (status.includes('delivered')) {
          trend.deliveredOrders++;
        } else if (status.includes('cancel')) {
          trend.cancelledOrders++;
        }
      });
      
      // Convert to array and sort by date
      const trendData = Object.values(trends)
        .map(trend => ({
          ...trend,
          totalValue: Math.round(trend.totalValue),
          avgOrderValue: trend.totalOrders > 0 ? Math.round(trend.totalValue / trend.totalOrders) : 0
        }))
        .sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
      
      res.json(trendData);
    } catch (error) {
      console.error('Error fetching order trends:', error);
      res.status(500).json({ message: 'Error fetching order trends' });
    }
  });
}