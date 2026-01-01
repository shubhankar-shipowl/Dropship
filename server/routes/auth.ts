import type { Express, Request, Response } from 'express';
import { db } from '../db';
import { users } from '@shared/schema';
import { eq } from 'drizzle-orm';
import { createHash, randomBytes } from 'crypto';

// Simple password hashing using crypto (for production, consider using bcrypt)
function hashPassword(password: string): string {
  const salt = randomBytes(16).toString('hex');
  const hash = createHash('sha256').update(password + salt).digest('hex');
  return `${salt}:${hash}`;
}

function verifyPassword(password: string, hashedPassword: string): boolean {
  const [salt, hash] = hashedPassword.split(':');
  const verifyHash = createHash('sha256').update(password + salt).digest('hex');
  return hash === verifyHash;
}

// Check if user is authenticated
export function requireAuth(req: Request, res: Response, next: () => void) {
  if (req.session && (req.session as any).userId) {
    next();
  } else {
    res.status(401).json({ message: 'Unauthorized' });
  }
}

// Helper function to retry database operations
async function retryDbOperation<T>(
  operation: () => Promise<T>,
  maxRetries = 3,
  delay = 1000
): Promise<T> {
  let lastError: any;
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await operation();
    } catch (error: any) {
      lastError = error;
      // Only retry on connection errors
      if (
        error.code === 'ECONNRESET' ||
        error.code === 'PROTOCOL_CONNECTION_LOST' ||
        error.code === 'ETIMEDOUT' ||
        error.code === 'ECONNREFUSED'
      ) {
        if (i < maxRetries - 1) {
          console.warn(`⚠️ Database connection error, retrying... (${i + 1}/${maxRetries})`);
          await new Promise((resolve) => setTimeout(resolve, delay * (i + 1)));
          continue;
        }
      }
      // For other errors, don't retry
      throw error;
    }
  }
  throw lastError;
}

export function registerAuthRoutes(app: Express): void {
  // Login endpoint
  app.post('/api/auth/login', async (req: Request, res: Response) => {
    try {
      const { username, password } = req.body;

      if (!username || !password) {
        return res.status(400).json({ message: 'Username and password are required' });
      }

      // Find user by username or email with retry logic
      const [user] = await retryDbOperation(async () => {
        return await db
          .select()
          .from(users)
          .where(eq(users.username, username))
          .limit(1);
      });

      if (!user) {
        return res.status(401).json({ message: 'Invalid credentials' });
      }

      // Verify password
      if (!verifyPassword(password, user.password)) {
        return res.status(401).json({ message: 'Invalid credentials' });
      }

      // Set session
      (req.session as any).userId = user.id;
      (req.session as any).username = user.username;
      (req.session as any).email = user.email;

      res.json({
        message: 'Login successful',
        user: {
          id: user.id,
          username: user.username,
          email: user.email,
        },
      });
    } catch (error: any) {
      console.error('Login error:', error);
      
      // Provide more specific error messages
      if (error.code === 'ECONNRESET' || error.code === 'PROTOCOL_CONNECTION_LOST') {
        return res.status(503).json({ 
          message: 'Database connection error. Please try again.' 
        });
      }
      
      res.status(500).json({ message: 'Internal server error' });
    }
  });

  // Logout endpoint
  app.post('/api/auth/logout', (req: Request, res: Response) => {
    req.session?.destroy((err) => {
      if (err) {
        console.error('Logout error:', err);
        return res.status(500).json({ message: 'Error logging out' });
      }
      res.json({ message: 'Logout successful' });
    });
  });

  // Check authentication status
  app.get('/api/auth/me', (req: Request, res: Response) => {
    if (req.session && (req.session as any).userId) {
      res.json({
        authenticated: true,
        user: {
          id: (req.session as any).userId,
          username: (req.session as any).username,
          email: (req.session as any).email,
        },
      });
    } else {
      res.json({ authenticated: false });
    }
  });

  // Create default admin user (for initial setup)
  app.post('/api/auth/setup', async (req: Request, res: Response) => {
    try {
      // Check if any users exist with retry
      const existingUsers = await retryDbOperation(async () => {
        return await db.select().from(users).limit(1);
      });

      if (existingUsers.length > 0) {
        return res.status(400).json({ message: 'Users already exist. Use login instead.' });
      }

      const { username, email, password } = req.body;

      if (!username || !email || !password) {
        return res.status(400).json({ message: 'Username, email, and password are required' });
      }

      const hashedPassword = hashPassword(password);

      await retryDbOperation(async () => {
        return await db
          .insert(users)
          .values({
            username,
            email,
            password: hashedPassword,
          });
      });

      // Fetch the created user to get the ID with retry
      const [createdUser] = await retryDbOperation(async () => {
        return await db
          .select()
          .from(users)
          .where(eq(users.username, username))
          .limit(1);
      });

      res.json({
        message: 'Admin user created successfully',
        user: {
          id: createdUser.id,
          username: createdUser.username,
          email: createdUser.email,
        },
      });
    } catch (error: any) {
      console.error('Setup error:', error);
      if (error.code === 'ECONNRESET' || error.code === 'PROTOCOL_CONNECTION_LOST') {
        return res.status(503).json({ 
          message: 'Database connection error. Please try again.' 
        });
      }
      res.status(500).json({ message: 'Internal server error' });
    }
  });

  // Create user endpoint (for adding additional users)
  // Note: In production, this should be protected with admin authentication
  app.post('/api/auth/create-user', async (req: Request, res: Response) => {
    try {
      const { username, email, password } = req.body;

      if (!username || !email || !password) {
        return res.status(400).json({ message: 'Username, email, and password are required' });
      }

      // Check if user already exists by username with retry
      const [existingUser] = await retryDbOperation(async () => {
        return await db
          .select()
          .from(users)
          .where(eq(users.username, username))
          .limit(1);
      });

      if (existingUser) {
        return res.status(400).json({ message: 'User with this username already exists' });
      }

      // Check if user already exists by email with retry
      const [existingEmail] = await retryDbOperation(async () => {
        return await db
          .select()
          .from(users)
          .where(eq(users.email, email))
          .limit(1);
      });

      if (existingEmail) {
        return res.status(400).json({ message: 'User with this email already exists' });
      }

      const hashedPassword = hashPassword(password);

      await retryDbOperation(async () => {
        return await db
          .insert(users)
          .values({
            username,
            email,
            password: hashedPassword,
          });
      });

      // Fetch the created user to get the ID with retry
      const [createdUser] = await retryDbOperation(async () => {
        return await db
          .select()
          .from(users)
          .where(eq(users.username, username))
          .limit(1);
      });

      res.json({
        message: 'User created successfully',
        user: {
          id: createdUser.id,
          username: createdUser.username,
          email: createdUser.email,
        },
      });
    } catch (error: any) {
      console.error('Create user error:', error);
      if (error.code === 'ECONNRESET' || error.code === 'PROTOCOL_CONNECTION_LOST') {
        return res.status(503).json({ 
          message: 'Database connection error. Please try again.' 
        });
      }
      res.status(500).json({ message: 'Internal server error' });
    }
  });
}

