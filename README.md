# Dropshipper Payout Calculation System

A comprehensive dropshipper payout calculation application built with React (frontend) and Express (backend) that processes order data, calculates payouts based on dual date ranges, and manages product prices and shipping rates.

## Features

- **File Upload Processing**: Excel/CSV files up to 200MB for large dataset processing
- **Dual-Range Calculation**: Separate date ranges for order dates (shipping costs) and delivered dates (COD/product costs)
- **COD Amount Calculation**: Excel formula implementation - COD received only for COD orders that are delivered/completed
- **Weight-Based Shipping**: Shipping costs calculated as qty × product weight × shipping rate per kg
- **Auto-Mapping**: Case-insensitive column detection for various Excel/CSV formats
- **RTS/RTO Reconciliation**: Comprehensive system for managing returned orders with automated detection
- **Excel Export**: Generate 4-sheet reports (Order Details, Shipping Details, COD Details, Product Cost Details)
- **Database Transparency**: Complete visibility with search, selection, and data management features

## Tech Stack

### Frontend
- React 18 with TypeScript
- Vite for build tooling
- Radix UI with shadcn/ui components
- Tailwind CSS for styling
- TanStack Query for state management
- React Hook Form with Zod validation

### Backend
- Express.js with TypeScript
- Drizzle ORM with PostgreSQL
- Multer for file upload handling
- PM2 for process management

## Prerequisites

- Node.js 18+ 
- PostgreSQL database
- PM2 (for production deployment)

## Installation

### 1. Clone the Repository
```bash
git clone <your-repository-url>
cd dropshipper-payout-app
```

### 2. Install Dependencies
```bash
npm install
```

### 3. Environment Setup
```bash
# Copy environment template
cp .env.example .env

# Edit environment variables (configure DB_URL, SESSION_SECRET, etc.)
nano .env
```

### 4. Database Setup
```bash
# Push database schema
npm run db:push
```

## Environment Variables

Create a `.env` file with the following variables:

```env
# Application Environment
NODE_ENV=development
PORT=5000

# Database Configuration (Required) - Choose one approach:

# Option 1: Single URL with all connection details (Recommended)
DB_URL=postgresql://username:password@localhost:5432/database_name

# Option 2: Traditional DATABASE_URL (for backward compatibility)
DATABASE_URL=postgresql://username:password@localhost:5432/database_name

# Note: For database migrations (npm run db:push), DATABASE_URL is required
# If using DB_URL, set DATABASE_URL to the same value:
# DATABASE_URL=$DB_URL

# Option 3: Individual components (if needed)
# PGHOST=localhost
# PGPORT=5432
# PGUSER=your_db_user
# PGPASSWORD=your_db_password
# PGDATABASE=your_database_name

# Session Security
SESSION_SECRET=your_super_secret_session_key_here

# Optional Settings
LOG_LEVEL=info
```

## Development

### Start Development Server
```bash
npm run dev
```

This starts both the backend API server and frontend development server with hot reloading.

### Available Scripts
- `npm run dev` - Start development server
- `npm run build` - Build for production
- `npm run start` - Start production server
- `npm run check` - Type checking
- `npm run db:push` - Push database schema changes

## Production Deployment

### Option 1: Quick Deployment (Recommended)
```bash
# Make the script executable
chmod +x start-production.sh

# Run the deployment script
./start-production.sh
```

### Option 2: Manual Deployment
```bash
# 1. Build the application
npm run build

# 2. Install PM2 globally (if not installed)
npm install -g pm2

# 3. Start with PM2
pm2 start ecosystem.config.cjs --env production

# 4. Save PM2 configuration
pm2 save
pm2 startup
```

## PM2 Process Management

### Basic Commands
```bash
# Start application
pm2 start ecosystem.config.cjs --env production

# View application status
pm2 list

# Monitor resources
pm2 monit

# View logs
pm2 logs dropshipper-payout-app

# Restart application
pm2 restart dropshipper-payout-app

# Stop application
pm2 stop dropshipper-payout-app

# Delete application
pm2 delete dropshipper-payout-app
```

### Log Management
```bash
# View recent logs
pm2 logs dropshipper-payout-app --lines 50

# Clear logs
pm2 flush

# View error logs only
pm2 logs dropshipper-payout-app --err
```

## VPS Deployment Steps

### 1. Server Preparation
```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Node.js 18+
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt-get install -y nodejs

# Install PM2 globally
sudo npm install -g pm2

# Install PostgreSQL (if needed)
sudo apt install postgresql postgresql-contrib
```

### 2. Application Deployment
```bash
# Clone repository
git clone <your-repository-url>
cd dropshipper-payout-app

# Install dependencies
npm install

# Set up environment
cp .env.example .env
nano .env  # Configure DB_URL, SESSION_SECRET

# Build and deploy
./start-production.sh
```

### 3. Process Management
```bash
# Configure PM2 to start on boot
pm2 startup
pm2 save

# Monitor application
pm2 monit
```

## Database Management

### Schema Updates
```bash
# Apply schema changes
npm run db:push

# Force push (if data loss warning)
npm run db:push --force
```

### Backup and Restore
```bash
# Backup database
pg_dump $DATABASE_URL > backup.sql

# Restore database
psql $DATABASE_URL < backup.sql
```

## Application Structure

```
├── client/                 # Frontend React application
│   ├── src/
│   │   ├── components/    # Reusable UI components
│   │   ├── pages/         # Application pages
│   │   └── lib/           # Utilities and helpers
├── server/                # Backend Express application
│   ├── routes/            # API route handlers
│   ├── storage.ts         # Database operations
│   └── index.ts           # Server entry point
├── shared/                # Shared types and schemas
│   └── schema.ts          # Database schema definitions
├── ecosystem.config.cjs    # PM2 configuration
├── start-production.sh    # Production deployment script
└── .env.example          # Environment variables template
```

## Key Features

### File Upload & Processing
- Supports Excel (.xlsx) and CSV files
- Automatic column mapping with case-insensitive detection
- Handles large files up to 200MB
- Validates data integrity and formats

### Payout Calculations
- **Dual Date Ranges**: Order dates for shipping costs, delivered dates for COD/product costs
- **COD Calculation**: Individual product values for multi-product waybills
- **Shipping Costs**: Weight-based calculations with configurable rates
- **Product Costs**: Configurable pricing per dropshipper and product

### Excel Export
Generates comprehensive reports with 4 sheets:
1. **Summary**: Overview with totals and calculation details
2. **Order Details**: Complete order information with calculations
3. **Shipping Details**: Shipping costs and provider information
4. **COD Details**: COD amounts for delivered orders only
5. **Product Cost Details**: Product costs and pricing information

### RTS/RTO Management
- Automatic detection of returned orders
- Financial reconciliation with audit trails
- Manual processing capabilities
- Complete history tracking

## Troubleshooting

### Common Issues

**Application won't start:**
```bash
# Check PM2 status
pm2 list

# View error logs
pm2 logs dropshipper-payout-app --err

# Restart application
pm2 restart dropshipper-payout-app
```

**Database connection issues:**
```bash
# Verify database URL
echo $DATABASE_URL

# Test database connection
psql $DATABASE_URL -c "SELECT 1;"

# Check database permissions
npm run db:push
```

**Port already in use:**
```bash
# Find process using port
sudo netstat -tlnp | grep :5000

# Kill process
sudo kill -9 <process_id>
```

### Performance Optimization

**Memory Usage:**
- PM2 automatically restarts if memory exceeds 1GB
- Monitor with `pm2 monit`
- Adjust `max_memory_restart` in ecosystem.config.cjs

**Database Performance:**
- Regular VACUUM and ANALYZE operations
- Index optimization for large datasets
- Connection pooling is handled automatically

## Support

For issues and questions:
1. Check the logs: `pm2 logs dropshipper-payout-app`
2. Verify environment variables are set correctly
3. Ensure database connectivity
4. Check PM2 process status: `pm2 list`

## License

MIT License - see LICENSE file for details.