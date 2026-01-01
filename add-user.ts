import 'dotenv/config';
import { db } from './server/db';
import { users } from './shared/schema';
import { eq } from 'drizzle-orm';
import { createHash, randomBytes } from 'crypto';
import { initializeDatabase, closeDatabase } from './server/db';

// Password hashing function (same as in auth.ts)
function hashPassword(password: string): string {
  const salt = randomBytes(16).toString('hex');
  const hash = createHash('sha256').update(password + salt).digest('hex');
  return `${salt}:${hash}`;
}

async function addUser() {
  const username = 'finance@shipowl.io';
  const email = 'finance@shipowl.io';
  const password = 'Shipowl@6';

  try {
    // Check if user already exists
    const [existingUser] = await db
      .select()
      .from(users)
      .where(eq(users.username, username))
      .limit(1);

    if (existingUser) {
      console.log('❌ User already exists with username:', username);
      process.exit(1);
    }

    // Check if email already exists
    const [existingEmail] = await db
      .select()
      .from(users)
      .where(eq(users.email, email))
      .limit(1);

    if (existingEmail) {
      console.log('❌ User already exists with email:', email);
      process.exit(1);
    }

    // Hash the password
    const hashedPassword = hashPassword(password);

    // Insert the user
    await db.insert(users).values({
      username,
      email,
      password: hashedPassword,
    });

    // Fetch the created user to confirm
    const [createdUser] = await db
      .select()
      .from(users)
      .where(eq(users.username, username))
      .limit(1);

    console.log('✅ User created successfully!');
    console.log('Username:', createdUser.username);
    console.log('Email:', createdUser.email);
    console.log('ID:', createdUser.id);
    
    process.exit(0);
  } catch (error: any) {
    console.error('❌ Error creating user:', error.message || error);
    process.exit(1);
  }
}

// Main function
async function main() {
  try {
    await initializeDatabase();
    await addUser();
  } catch (error: any) {
    console.error('Error:', error.message || error);
    process.exit(1);
  } finally {
    await closeDatabase();
  }
}

main();

