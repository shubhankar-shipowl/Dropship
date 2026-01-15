import type { Express, Request, Response } from 'express';
import { gmailService } from '../services/gmail';
import { z } from 'zod';

export function registerGmailRoutes(app: Express): void {
  /**
   * GET /api/gmail/status
   * Check Gmail API authorization status
   */
  app.get('/api/gmail/status', async (req: Request, res: Response) => {
    try {
      const status = gmailService.getStatus();
      
      let userProfile = null;
      if (status.authorized) {
        userProfile = await gmailService.getUserProfile();
      }

      res.json({
        success: true,
        status: {
          ...status,
          userEmail: userProfile?.email || null,
          messagesTotal: userProfile?.messagesTotal || 0,
        },
      });
    } catch (error: any) {
      console.error('Error getting Gmail status:', error);
      res.status(500).json({
        success: false,
        message: error.message || 'Error getting Gmail status',
      });
    }
  });

  /**
   * GET /api/gmail/authorize
   * Redirect user to Google OAuth consent screen
   */
  app.get('/api/gmail/authorize', (req: Request, res: Response) => {
    try {
      // Optional state parameter for security
      const state = req.query.state as string | undefined;
      
      const authUrl = gmailService.getAuthorizationUrl(state);
      
      if (!authUrl) {
        return res.status(500).json({
          success: false,
          message: 'Failed to generate authorization URL. Check cred.json configuration.',
        });
      }

      // Return the URL for manual redirect or redirect directly
      const redirect = req.query.redirect === 'true';
      
      if (redirect) {
        return res.redirect(authUrl);
      }

      res.json({
        success: true,
        message: 'Visit the authorization URL to grant Gmail access',
        authUrl,
        instructions: [
          '1. Click the authorization URL or visit it in your browser',
          '2. Sign in with your Google account',
          '3. Grant the requested permissions',
          '4. You will be redirected back automatically',
        ],
      });
    } catch (error: any) {
      console.error('Error generating auth URL:', error);
      res.status(500).json({
        success: false,
        message: error.message || 'Error generating authorization URL',
      });
    }
  });

  /**
   * GET /api/gmail/oauth2callback
   * OAuth2 callback - receives authorization code from Google
   */
  app.get('/api/gmail/oauth2callback', async (req: Request, res: Response) => {
    try {
      const { code, error: oauthError, error_description } = req.query;

      // Handle OAuth errors
      if (oauthError) {
        console.error('OAuth error:', oauthError, error_description);
        return res.status(400).send(`
          <!DOCTYPE html>
          <html>
          <head>
            <title>Authorization Failed</title>
            <style>
              body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; }
              .error { background: #fee2e2; border: 1px solid #ef4444; border-radius: 8px; padding: 20px; }
              h1 { color: #dc2626; }
              a { color: #2563eb; }
            </style>
          </head>
          <body>
            <div class="error">
              <h1>❌ Authorization Failed</h1>
              <p><strong>Error:</strong> ${oauthError}</p>
              <p><strong>Description:</strong> ${error_description || 'No description provided'}</p>
              <p><a href="/api/gmail/authorize?redirect=true">Try again</a></p>
            </div>
          </body>
          </html>
        `);
      }

      // Validate authorization code
      if (!code || typeof code !== 'string') {
        return res.status(400).send(`
          <!DOCTYPE html>
          <html>
          <head>
            <title>Missing Authorization Code</title>
            <style>
              body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; }
              .error { background: #fef3c7; border: 1px solid #f59e0b; border-radius: 8px; padding: 20px; }
              h1 { color: #d97706; }
              a { color: #2563eb; }
            </style>
          </head>
          <body>
            <div class="error">
              <h1>⚠️ Missing Authorization Code</h1>
              <p>No authorization code was provided by Google.</p>
              <p><a href="/api/gmail/authorize?redirect=true">Start authorization again</a></p>
            </div>
          </body>
          </html>
        `);
      }

      // Exchange code for tokens
      console.log('🔄 Exchanging authorization code for tokens...');
      const result = await gmailService.exchangeCodeForTokens(code as string);

      if (!result.success) {
        return res.status(400).send(`
          <!DOCTYPE html>
          <html>
          <head>
            <title>Token Exchange Failed</title>
            <style>
              body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; }
              .error { background: #fee2e2; border: 1px solid #ef4444; border-radius: 8px; padding: 20px; }
              h1 { color: #dc2626; }
              a { color: #2563eb; }
              code { background: #f3f4f6; padding: 2px 6px; border-radius: 4px; }
            </style>
          </head>
          <body>
            <div class="error">
              <h1>❌ Token Exchange Failed</h1>
              <p><strong>Error:</strong> ${result.error}</p>
              <p>This can happen if:</p>
              <ul>
                <li>The authorization code has expired (codes expire quickly)</li>
                <li>The code was already used</li>
                <li>There's a redirect URI mismatch</li>
              </ul>
              <p><a href="/api/gmail/authorize?redirect=true">Try again</a></p>
            </div>
          </body>
          </html>
        `);
      }

      // Get user profile to show success
      const profile = await gmailService.getUserProfile();

      // Success page
      res.send(`
        <!DOCTYPE html>
        <html>
        <head>
          <title>Gmail Authorization Successful</title>
          <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; background: #f9fafb; }
            .success { background: #d1fae5; border: 1px solid #10b981; border-radius: 12px; padding: 30px; text-align: center; }
            h1 { color: #059669; margin-bottom: 20px; }
            .email { font-size: 18px; background: white; padding: 10px 20px; border-radius: 20px; display: inline-block; margin: 15px 0; }
            .info { background: white; border-radius: 8px; padding: 20px; margin-top: 20px; text-align: left; }
            .info h3 { color: #374151; margin-top: 0; }
            .check { color: #10b981; margin-right: 8px; }
            ul { list-style: none; padding: 0; }
            li { padding: 8px 0; }
            a { color: #2563eb; }
            .btn { display: inline-block; background: #2563eb; color: white; padding: 12px 24px; border-radius: 8px; text-decoration: none; margin-top: 20px; }
            .btn:hover { background: #1d4ed8; }
          </style>
        </head>
        <body>
          <div class="success">
            <h1>✅ Gmail Authorization Successful!</h1>
            <p>Your Gmail account is now connected:</p>
            <div class="email">📧 ${profile?.email || 'Unknown'}</div>
            
            <div class="info">
              <h3>What happens now?</h3>
              <ul>
                <li><span class="check">✓</span> Access token and refresh token saved to <code>token.json</code></li>
                <li><span class="check">✓</span> Gmail API is ready to use</li>
                <li><span class="check">✓</span> Tokens will auto-refresh when needed</li>
                <li><span class="check">✓</span> You can now send emails with labels</li>
              </ul>
            </div>
            
          </div>
        </body>
        </html>
      `);
    } catch (error: any) {
      console.error('OAuth callback error:', error);
      res.status(500).send(`
        <!DOCTYPE html>
        <html>
        <head>
          <title>Authorization Error</title>
          <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; }
            .error { background: #fee2e2; border: 1px solid #ef4444; border-radius: 8px; padding: 20px; }
            h1 { color: #dc2626; }
            a { color: #2563eb; }
          </style>
        </head>
        <body>
          <div class="error">
            <h1>❌ Authorization Error</h1>
            <p>${error.message || 'An unexpected error occurred'}</p>
            <p><a href="/api/gmail/authorize?redirect=true">Try again</a></p>
          </div>
        </body>
        </html>
      `);
    }
  });

  /**
   * GET /api/gmail/test
   * Test Gmail API connection
   */
  app.get('/api/gmail/test', async (req: Request, res: Response) => {
    try {
      const gmail = await gmailService.getGmailClient();

      if (!gmail) {
        return res.status(401).json({
          success: false,
          message: 'Gmail API not authorized. Please authorize first.',
          authorizeUrl: '/api/gmail/authorize',
        });
      }

      const profile = await gmailService.getUserProfile();
      const labels = await gmailService.listLabels();

      res.json({
        success: true,
        message: 'Gmail API is working correctly!',
        profile: {
          email: profile?.email,
          messagesTotal: profile?.messagesTotal,
        },
        labels: labels.slice(0, 10), // Show first 10 labels
        totalLabels: labels.length,
      });
    } catch (error: any) {
      console.error('Gmail test error:', error);
      res.status(500).json({
        success: false,
        message: error.message || 'Error testing Gmail API',
      });
    }
  });

  /**
   * GET /api/gmail/labels
   * Get all Gmail labels
   */
  app.get('/api/gmail/labels', async (req: Request, res: Response) => {
    try {
      const gmail = await gmailService.getGmailClient();

      if (!gmail) {
        return res.status(401).json({
          success: false,
          message: 'Gmail API not authorized',
          labels: [],
        });
      }

      const labels = await gmailService.listLabels();

      res.json({
        success: true,
        labels,
      });
    } catch (error: any) {
      console.error('Error fetching labels:', error);
      res.status(500).json({
        success: false,
        message: error.message || 'Error fetching labels',
        labels: [],
      });
    }
  });

  /**
   * POST /api/gmail/labels
   * Create a new Gmail label
   */
  app.post('/api/gmail/labels', async (req: Request, res: Response) => {
    try {
      const { name } = req.body;

      if (!name || typeof name !== 'string' || name.trim().length === 0) {
        return res.status(400).json({
          success: false,
          message: 'Label name is required',
        });
      }

      const gmail = await gmailService.getGmailClient();

      if (!gmail) {
        return res.status(401).json({
          success: false,
          message: 'Gmail API not authorized',
        });
      }

      const labelId = await gmailService.getOrCreateLabel(name.trim());

      if (!labelId) {
        return res.status(500).json({
          success: false,
          message: 'Failed to create label',
        });
      }

      res.json({
        success: true,
        message: 'Label created successfully',
        label: {
          id: labelId,
          name: name.trim(),
        },
      });
    } catch (error: any) {
      console.error('Error creating label:', error);
      res.status(500).json({
        success: false,
        message: error.message || 'Error creating label',
      });
    }
  });

  /**
   * POST /api/gmail/send
   * Send an email via Gmail API
   */
  app.post('/api/gmail/send', async (req: Request, res: Response) => {
    try {
      const requestSchema = z.object({
        to: z.string().email(),
        cc: z.array(z.string().email()).optional(),
        subject: z.string().min(1),
        htmlBody: z.string().min(1),
        textBody: z.string().optional(),
        labelName: z.string().optional(),
      });

      const request = requestSchema.parse(req.body);

      const gmail = await gmailService.getGmailClient();

      if (!gmail) {
        return res.status(401).json({
          success: false,
          message: 'Gmail API not authorized. Please authorize first.',
          authorizeUrl: '/api/gmail/authorize',
        });
      }

      // Find existing thread for conversation grouping
      let threadId: string | undefined;
      if (request.subject.includes('Payout')) {
        threadId = (await gmailService.findExistingThread(request.to, 'Payout')) || undefined;
      }

      const result = await gmailService.sendEmail({
        to: request.to,
        cc: request.cc,
        subject: request.subject,
        htmlBody: request.htmlBody,
        textBody: request.textBody,
        labelName: request.labelName,
        threadId,
      });

      if (!result.success) {
        return res.status(500).json({
          success: false,
          message: result.error || 'Failed to send email',
        });
      }

      res.json({
        success: true,
        message: 'Email sent successfully',
        messageId: result.messageId,
        labelApplied: result.labelApplied,
      });
    } catch (error: any) {
      console.error('Error sending email:', error);

      if (error instanceof z.ZodError) {
        return res.status(400).json({
          success: false,
          message: 'Invalid request',
          errors: error.errors,
        });
      }

      res.status(500).json({
        success: false,
        message: error.message || 'Error sending email',
      });
    }
  });

  /**
   * POST /api/gmail/revoke
   * Revoke Gmail access and delete tokens
   */
  app.post('/api/gmail/revoke', async (req: Request, res: Response) => {
    try {
      const result = await gmailService.revokeAccess();

      res.json({
        success: true,
        message: result
          ? 'Gmail access revoked and tokens deleted'
          : 'Tokens deleted (revocation may have failed)',
      });
    } catch (error: any) {
      console.error('Error revoking access:', error);
      res.status(500).json({
        success: false,
        message: error.message || 'Error revoking access',
      });
    }
  });

  // Legacy endpoints for backward compatibility
  // These redirect to the new endpoints

  /**
   * GET /api/gmail-oauth-setup (legacy)
   * Redirects to new endpoint
   */
  app.get('/api/gmail-oauth-setup', (req: Request, res: Response) => {
    res.redirect('/api/gmail/authorize');
  });

  /**
   * POST /api/gmail-oauth-callback (legacy)
   * Handle legacy callback format
   */
  app.post('/api/gmail-oauth-callback', async (req: Request, res: Response) => {
    try {
      const { code } = req.body;

      if (!code) {
        return res.status(400).json({
          message: 'Authorization code is required',
        });
      }

      const result = await gmailService.exchangeCodeForTokens(code);

      if (!result.success) {
        return res.status(400).json({
          message: result.error || 'Failed to exchange code for tokens',
        });
      }

      res.json({
        message: 'OAuth2 authorization successful!',
        instructions: [
          'Tokens have been saved to token.json',
          'Gmail API is now ready to use',
          'No need to update .env file - tokens are stored in token.json',
        ],
      });
    } catch (error: any) {
      console.error('Legacy OAuth callback error:', error);
      res.status(500).json({
        message: error.message || 'Error processing OAuth callback',
      });
    }
  });

  /**
   * GET /api/gmail-labels (legacy)
   * Redirects to new endpoint
   */
  app.get('/api/gmail-labels', async (req: Request, res: Response) => {
    // Forward to new endpoint
    const gmail = await gmailService.getGmailClient();

    if (!gmail) {
      return res.status(400).json({
        success: false,
        message: 'Gmail API not configured',
        labels: [],
      });
    }

    const labels = await gmailService.listLabels();

    res.json({
      success: true,
      labels,
    });
  });

  /**
   * POST /api/gmail-labels (legacy)
   */
  app.post('/api/gmail-labels', async (req: Request, res: Response) => {
    const { name } = req.body;

    if (!name || typeof name !== 'string' || name.trim().length === 0) {
      return res.status(400).json({
        success: false,
        message: 'Label name is required',
      });
    }

    const gmail = await gmailService.getGmailClient();

    if (!gmail) {
      return res.status(400).json({
        success: false,
        message: 'Gmail API not configured',
      });
    }

    const labelId = await gmailService.getOrCreateLabel(name.trim());

    if (!labelId) {
      return res.status(500).json({
        success: false,
        message: 'Failed to create label',
      });
    }

    res.json({
      success: true,
      message: 'Label created successfully',
      label: {
        id: labelId,
        name: name.trim(),
      },
    });
  });

  /**
   * GET /api/test-gmail-api (legacy)
   */
  app.get('/api/test-gmail-api', async (req: Request, res: Response) => {
    const gmail = await gmailService.getGmailClient();

    if (!gmail) {
      return res.status(400).json({
        success: false,
        message: 'Gmail API not configured. Please authorize first.',
        configured: {
          credentialsFile: gmailService.getStatus().credentialsLoaded,
          authorized: false,
        },
        authorizeUrl: '/api/gmail/authorize',
      });
    }

    const profile = await gmailService.getUserProfile();
    const labels = await gmailService.listLabels();

    res.json({
      success: true,
      message: 'Gmail API is working correctly!',
      profile: {
        email: profile?.email,
      },
      labelsCount: labels.length,
      note: 'Labels will be applied automatically when sending emails via Gmail API',
    });
  });

  console.log('✅ Gmail routes registered');
}
