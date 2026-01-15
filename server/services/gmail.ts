import { google, gmail_v1 } from 'googleapis';
import { OAuth2Client, Credentials } from 'google-auth-library';
import * as fs from 'fs';
import * as path from 'path';

// Paths for credentials and tokens
const CREDENTIALS_PATH = path.join(process.cwd(), 'cred.json');
const TOKEN_PATH = path.join(process.cwd(), 'token.json');

// Gmail API scopes
const SCOPES = [
  'https://www.googleapis.com/auth/gmail.readonly',
  'https://www.googleapis.com/auth/gmail.send',
  'https://www.googleapis.com/auth/gmail.modify',
  'https://www.googleapis.com/auth/gmail.labels',
];

interface CredentialsFile {
  web: {
    client_id: string;
    project_id: string;
    auth_uri: string;
    token_uri: string;
    auth_provider_x509_cert_url: string;
    client_secret: string;
    redirect_uris: string[];
  };
}

interface StoredTokens {
  access_token: string;
  refresh_token: string;
  scope: string;
  token_type: string;
  expiry_date: number;
}

class GmailService {
  private oauth2Client: OAuth2Client | null = null;
  private gmail: gmail_v1.Gmail | null = null;
  private credentials: CredentialsFile | null = null;
  private isInitialized = false;
  private cachedUserEmail: string | null = null;
  private labelCache: Map<string, string> = new Map(); // name -> id cache
  private labelCacheTime: number = 0;

  /**
   * Load credentials from cred.json
   */
  private loadCredentials(): CredentialsFile | null {
    try {
      if (!fs.existsSync(CREDENTIALS_PATH)) {
        console.error('❌ cred.json file not found at:', CREDENTIALS_PATH);
        return null;
      }

      const content = fs.readFileSync(CREDENTIALS_PATH, 'utf-8');
      const credentials = JSON.parse(content) as CredentialsFile;

      if (!credentials.web?.client_id || !credentials.web?.client_secret) {
        console.error('❌ Invalid cred.json format: missing client_id or client_secret');
        return null;
      }

      console.log('✅ Loaded credentials from cred.json');
      console.log('📋 Project ID:', credentials.web.project_id);
      return credentials;
    } catch (error: any) {
      console.error('❌ Error loading cred.json:', error.message);
      return null;
    }
  }

  /**
   * Load tokens from token.json
   */
  private loadTokens(): StoredTokens | null {
    try {
      if (!fs.existsSync(TOKEN_PATH)) {
        console.log('ℹ️ No token.json found - user needs to authorize');
        return null;
      }

      const content = fs.readFileSync(TOKEN_PATH, 'utf-8');
      const tokens = JSON.parse(content) as StoredTokens;

      if (!tokens.refresh_token) {
        console.warn('⚠️ token.json exists but has no refresh_token');
        return null;
      }

      console.log('✅ Loaded tokens from token.json');
      return tokens;
    } catch (error: any) {
      console.error('❌ Error loading token.json:', error.message);
      return null;
    }
  }

  /**
   * Save tokens to token.json
   */
  saveTokens(tokens: Credentials): void {
    try {
      const tokenData: StoredTokens = {
        access_token: tokens.access_token || '',
        refresh_token: tokens.refresh_token || '',
        scope: tokens.scope || SCOPES.join(' '),
        token_type: tokens.token_type || 'Bearer',
        expiry_date: tokens.expiry_date || Date.now() + 3600 * 1000,
      };

      fs.writeFileSync(TOKEN_PATH, JSON.stringify(tokenData, null, 2));
      console.log('✅ Tokens saved to token.json');
    } catch (error: any) {
      console.error('❌ Error saving tokens:', error.message);
    }
  }

  /**
   * Get the redirect URI based on environment
   */
  getRedirectUri(): string {
    const credentials = this.credentials || this.loadCredentials();
    if (!credentials?.web?.redirect_uris?.length) {
      return 'http://localhost:5003/api/gmail/oauth2callback';
    }

    // Use localhost redirect for development, production URL otherwise
    const isProduction = process.env.NODE_ENV === 'production';
    const redirectUris = credentials.web.redirect_uris;

    if (isProduction) {
      // Prefer non-localhost URI for production
      const productionUri = redirectUris.find((uri) => !uri.includes('localhost'));
      return productionUri || redirectUris[0];
    } else {
      // Prefer localhost URI for development
      const devUri = redirectUris.find((uri) => uri.includes('localhost'));
      return devUri || redirectUris[0];
    }
  }

  /**
   * Initialize the OAuth2 client
   */
  initialize(): boolean {
    if (this.isInitialized && this.oauth2Client) {
      return true;
    }

    this.credentials = this.loadCredentials();
    if (!this.credentials) {
      return false;
    }

    const { client_id, client_secret } = this.credentials.web;
    const redirectUri = this.getRedirectUri();

    console.log('🔧 Initializing OAuth2 client...');
    console.log('📍 Redirect URI:', redirectUri);

    this.oauth2Client = new google.auth.OAuth2(client_id, client_secret, redirectUri);

    // Set up token refresh listener
    this.oauth2Client.on('tokens', (tokens) => {
      console.log('🔄 New tokens received from Google');
      if (tokens.refresh_token) {
        // Merge with existing tokens to preserve refresh_token
        const existingTokens = this.loadTokens();
        this.saveTokens({
          ...existingTokens,
          ...tokens,
        });
      } else if (tokens.access_token) {
        // Only update access token
        const existingTokens = this.loadTokens();
        if (existingTokens) {
          this.saveTokens({
            ...existingTokens,
            access_token: tokens.access_token,
            expiry_date: tokens.expiry_date,
          });
        }
      }
    });

    // Load existing tokens if available
    const storedTokens = this.loadTokens();
    if (storedTokens) {
      this.oauth2Client.setCredentials(storedTokens);
      console.log('✅ OAuth2 client configured with stored tokens');
    }

    this.isInitialized = true;
    return true;
  }

  /**
   * Check if user is authorized (has valid tokens)
   */
  isAuthorized(): boolean {
    if (!this.initialize()) {
      return false;
    }

    const tokens = this.loadTokens();
    return !!(tokens?.refresh_token);
  }

  /**
   * Generate authorization URL for user to grant access
   */
  getAuthorizationUrl(state?: string): string | null {
    if (!this.initialize()) {
      return null;
    }

    const url = this.oauth2Client!.generateAuthUrl({
      access_type: 'offline',
      scope: SCOPES,
      prompt: 'consent', // Force consent to ensure we get a refresh token
      state: state,
    });

    console.log('🔗 Generated authorization URL');
    return url;
  }

  /**
   * Exchange authorization code for tokens
   */
  async exchangeCodeForTokens(code: string): Promise<{ success: boolean; error?: string }> {
    if (!this.initialize()) {
      return { success: false, error: 'Failed to initialize OAuth2 client' };
    }

    try {
      console.log('🔄 Exchanging authorization code for tokens...');
      const { tokens } = await this.oauth2Client!.getToken(code);

      if (!tokens.refresh_token) {
        console.warn('⚠️ No refresh token received. This may happen if user has already authorized.');
        console.warn('   Try revoking access at https://myaccount.google.com/permissions and re-authorize.');
      }

      this.oauth2Client!.setCredentials(tokens);
      this.saveTokens(tokens);

      console.log('✅ Successfully obtained and saved tokens');
      return { success: true };
    } catch (error: any) {
      console.error('❌ Error exchanging code for tokens:', error.message);
      return { success: false, error: error.message };
    }
  }

  /**
   * Get Gmail API client (cached for performance)
   */
  async getGmailClient(): Promise<gmail_v1.Gmail | null> {
    // Return cached client if available and tokens are valid
    if (this.gmail && this.oauth2Client) {
      const tokens = this.loadTokens();
      if (tokens && tokens.expiry_date && tokens.expiry_date > Date.now() + 60000) {
        // Token is still valid (with 1 min buffer), return cached client
        return this.gmail;
      }
    }

    if (!this.initialize()) {
      return null;
    }

    if (!this.isAuthorized()) {
      return null;
    }

    const tokens = this.loadTokens();
    if (tokens) {
      this.oauth2Client!.setCredentials(tokens);
    }

    try {
      // Only refresh if token is expired or about to expire
      const currentTokens = this.oauth2Client!.credentials;
      if (!currentTokens.expiry_date || currentTokens.expiry_date < Date.now() + 60000) {
        const { credentials } = await this.oauth2Client!.refreshAccessToken();
        this.oauth2Client!.setCredentials(credentials);
      }

      this.gmail = google.gmail({ version: 'v1', auth: this.oauth2Client! });
      return this.gmail;
    } catch (error: any) {
      console.error('❌ Error getting Gmail client:', error.message);
      if (error.message.includes('invalid_grant') || error.message.includes('Token has been expired')) {
        if (fs.existsSync(TOKEN_PATH)) {
          fs.unlinkSync(TOKEN_PATH);
        }
      }
      return null;
    }
  }

  /**
   * Get user's email profile (cached)
   */
  async getUserProfile(): Promise<{ email: string; messagesTotal: number } | null> {
    // Return cached email if available
    if (this.cachedUserEmail) {
      return { email: this.cachedUserEmail, messagesTotal: 0 };
    }

    const gmail = await this.getGmailClient();
    if (!gmail) return null;

    try {
      const response = await gmail.users.getProfile({ userId: 'me' });
      this.cachedUserEmail = response.data.emailAddress || '';
      return {
        email: this.cachedUserEmail,
        messagesTotal: response.data.messagesTotal || 0,
      };
    } catch (error: any) {
      console.error('❌ Error getting user profile:', error.message);
      return null;
    }
  }

  /**
   * Get or create a Gmail label (with caching)
   */
  async getOrCreateLabel(labelName: string): Promise<string | null> {
    const trimmedName = labelName.trim();
    if (!trimmedName) return null;

    // Check cache first (valid for 5 minutes)
    const cacheKey = trimmedName.toLowerCase();
    if (this.labelCache.has(cacheKey) && Date.now() - this.labelCacheTime < 300000) {
      return this.labelCache.get(cacheKey)!;
    }

    const gmail = await this.getGmailClient();
    if (!gmail) return null;

    try {
      // Refresh label cache if expired
      if (Date.now() - this.labelCacheTime >= 300000) {
        const response = await gmail.users.labels.list({ userId: 'me' });
        const labels = response.data.labels || [];
        this.labelCache.clear();
        labels.forEach((label) => {
          if (label.type === 'user' && label.name && label.id) {
            this.labelCache.set(label.name.toLowerCase(), label.id);
          }
        });
        this.labelCacheTime = Date.now();
      }

      // Check cache after refresh
      if (this.labelCache.has(cacheKey)) {
        return this.labelCache.get(cacheKey)!;
      }

      // Create new label
      const createResponse = await gmail.users.labels.create({
        userId: 'me',
        requestBody: {
          name: trimmedName,
          labelListVisibility: 'labelShow',
          messageListVisibility: 'show',
        },
      });

      const labelId = createResponse.data.id!;
      this.labelCache.set(cacheKey, labelId);
      return labelId;
    } catch (error: any) {
      console.error(`❌ Error managing label "${labelName}":`, error.message);
      return null;
    }
  }

  /**
   * List all user labels
   */
  async listLabels(): Promise<Array<{ id: string; name: string; type: string }>> {
    const gmail = await this.getGmailClient();
    if (!gmail) return [];

    try {
      const response = await gmail.users.labels.list({ userId: 'me' });
      const labels = response.data.labels || [];

      return labels
        .filter((label) => label.type === 'user')
        .map((label) => ({
          id: label.id || '',
          name: label.name || '',
          type: label.type || '',
        }))
        .sort((a, b) => a.name.localeCompare(b.name));
    } catch (error: any) {
      console.error('❌ Error listing labels:', error.message);
      return [];
    }
  }

  /**
   * Send an email with optional attachment and label (optimized)
   */
  async sendEmail(options: {
    to: string;
    cc?: string[];
    subject: string;
    htmlBody: string;
    textBody?: string;
    attachment?: {
      filename: string;
      content: Buffer;
      contentType: string;
    };
    labelName?: string;
    threadId?: string;
  }): Promise<{
    success: boolean;
    messageId?: string;
    labelApplied?: boolean;
    error?: string;
  }> {
    const gmail = await this.getGmailClient();
    if (!gmail) {
      return { success: false, error: 'Gmail API not available' };
    }

    try {
      // Use cached email or fetch once
      const fromEmail = this.cachedUserEmail || (await this.getUserProfile())?.email || 'noreply@example.com';

      // Build email headers
      const headers: string[] = [
        `From: "Shipowl Finance Team" <${fromEmail}>`,
        `To: ${options.to}`,
      ];

      if (options.cc && options.cc.length > 0) {
        headers.push(`Cc: ${options.cc.join(', ')}`);
      }

      headers.push(`Subject: ${options.subject}`);
      headers.push('MIME-Version: 1.0');

      let emailBody: string;

      if (options.attachment) {
        const boundary = `----=_Part_${Date.now()}`;
        const attachmentBase64 = options.attachment.content.toString('base64');

        headers.push(`Content-Type: multipart/mixed; boundary="${boundary}"`);

        emailBody = [
          ...headers,
          '',
          `--${boundary}`,
          'Content-Type: text/html; charset=utf-8',
          'Content-Transfer-Encoding: 7bit',
          '',
          options.htmlBody,
          '',
          `--${boundary}`,
          `Content-Type: ${options.attachment.contentType}`,
          `Content-Disposition: attachment; filename="${options.attachment.filename}"`,
          'Content-Transfer-Encoding: base64',
          '',
          attachmentBase64,
          '',
          `--${boundary}--`,
        ].join('\r\n');
      } else {
        headers.push('Content-Type: text/html; charset=utf-8');
        emailBody = [...headers, '', options.htmlBody].join('\r\n');
      }

      // Encode email for Gmail API
      const encodedEmail = Buffer.from(emailBody)
        .toString('base64')
        .replace(/\+/g, '-')
        .replace(/\//g, '_')
        .replace(/=+$/, '');

      // Build send request
      const sendRequest: gmail_v1.Params$Resource$Users$Messages$Send = {
        userId: 'me',
        requestBody: {
          raw: encodedEmail,
          ...(options.threadId && { threadId: options.threadId }),
        },
      };

      // Send email
      const sendResponse = await gmail.users.messages.send(sendRequest);
      const messageId = sendResponse.data.id;

      if (!messageId) {
        throw new Error('No message ID returned from Gmail API');
      }

      // Apply label if specified (use cached label ID)
      let labelApplied = false;
      if (options.labelName) {
        try {
          const labelId = await this.getOrCreateLabel(options.labelName);
          if (labelId) {
            await gmail.users.messages.modify({
              userId: 'me',
              id: messageId,
              requestBody: { addLabelIds: [labelId] },
            });
            labelApplied = true;
          }
        } catch {
          // Silently fail label application - email was still sent
        }
      }

      return { success: true, messageId, labelApplied };
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  /**
   * Find existing email thread for conversation threading
   */
  async findExistingThread(recipient: string, subjectPrefix: string): Promise<string | null> {
    const gmail = await this.getGmailClient();
    if (!gmail) return null;

    try {
      const query = `to:${recipient} subject:"${subjectPrefix}"`;
      const response = await gmail.users.messages.list({
        userId: 'me',
        q: query,
        maxResults: 1,
      });

      const messages = response.data.messages || [];
      if (messages.length > 0 && messages[0].threadId) {
        console.log(`✅ Found existing thread: ${messages[0].threadId}`);
        return messages[0].threadId;
      }

      return null;
    } catch (error: any) {
      console.warn(`⚠️ Error searching for thread: ${error.message}`);
      return null;
    }
  }

  /**
   * Revoke access and delete tokens
   */
  async revokeAccess(): Promise<boolean> {
    if (!this.initialize()) {
      return false;
    }

    try {
      const tokens = this.loadTokens();
      if (tokens?.access_token) {
        await this.oauth2Client!.revokeToken(tokens.access_token);
        console.log('✅ Access token revoked');
      }

      // Delete token file
      if (fs.existsSync(TOKEN_PATH)) {
        fs.unlinkSync(TOKEN_PATH);
        console.log('✅ token.json deleted');
      }

      this.gmail = null;
      return true;
    } catch (error: any) {
      console.error('❌ Error revoking access:', error.message);
      // Still delete local token file
      if (fs.existsSync(TOKEN_PATH)) {
        fs.unlinkSync(TOKEN_PATH);
      }
      return false;
    }
  }

  /**
   * Get authorization status and details
   */
  getStatus(): {
    credentialsLoaded: boolean;
    authorized: boolean;
    redirectUri: string;
    scopes: string[];
    tokenPath: string;
    credentialsPath: string;
  } {
    return {
      credentialsLoaded: !!this.loadCredentials(),
      authorized: this.isAuthorized(),
      redirectUri: this.getRedirectUri(),
      scopes: SCOPES,
      tokenPath: TOKEN_PATH,
      credentialsPath: CREDENTIALS_PATH,
    };
  }
}

// Export singleton instance
export const gmailService = new GmailService();

// Export types for use in routes
export type { StoredTokens, CredentialsFile };
