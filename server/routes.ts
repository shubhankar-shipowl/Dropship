import type { Express } from "express";
import { createServer, type Server } from "http";
import { registerUploadRoutes } from "./routes/upload";
import { registerPayoutRoutes } from "./routes/payout";
import { registerSettingsRoutes } from "./routes/settings";
import { registerAnalyticsRoutes } from "./routes/analytics";
import { registerRtsRtoRoutes } from "./routes/rts-rto";
import { registerTransparencyRoutes } from "./routes/transparency";
import { registerAuthRoutes } from "./routes/auth";
import { registerGmailRoutes } from "./routes/gmail";

export async function registerRoutes(app: Express): Promise<Server> {
  
  // Register all route modules
  registerAuthRoutes(app);
  registerGmailRoutes(app); // Gmail OAuth2 routes - registered early for auth flow
  registerUploadRoutes(app);
  registerPayoutRoutes(app);
  registerSettingsRoutes(app);
  registerAnalyticsRoutes(app);
  registerRtsRtoRoutes(app);
  registerTransparencyRoutes(app);

  // Health check endpoint
  app.get('/api/health', (req, res) => {
    res.json({ 
      status: 'ok', 
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      version: '1.0.0'
    });
  });


  // Create and return HTTP server
  const server = createServer(app);
  return server;
}