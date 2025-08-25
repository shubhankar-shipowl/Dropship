# Overview

This is a dropshipping payout calculation system built with React (frontend) and Express (backend) that processes order data, calculates payouts based on dual date ranges, and manages product prices and shipping rates. The system handles file uploads (Excel/CSV), automatically maps columns, calculates payouts considering shipping costs and product costs, and supports reconciliation for returned orders (RTS/RTO).

# User Preferences

Preferred communication style: Simple, everyday language.
File upload limits: Excel/CSV files up to 200MB for large dataset processing.
UI Design Preference: Clean, modern card layouts with centered content and proper web view optimization.

# System Architecture

## Frontend Architecture
- **Framework**: React with TypeScript and Vite for build tooling
- **UI Components**: Radix UI primitives with shadcn/ui component library
- **Styling**: Tailwind CSS with custom design tokens and CSS variables
- **State Management**: TanStack Query for server state management, local React state for UI state
- **Routing**: Wouter for lightweight client-side routing
- **Forms**: React Hook Form with Zod validation
- **File Processing**: XLSX library for Excel file parsing and generation

## Backend Architecture
- **Framework**: Express.js with TypeScript
- **Database ORM**: Drizzle ORM with PostgreSQL
- **File Upload**: Multer for handling multipart form data
- **Data Processing**: Custom column mapping logic for Excel/CSV files with case-insensitive matching
- **API Design**: RESTful endpoints with structured error handling and request/response logging

## Data Storage Solutions
- **Primary Database**: PostgreSQL with Neon serverless hosting
- **Schema Management**: Drizzle migrations and schema definitions
- **Key Tables**:
  - Upload sessions for tracking file imports
  - Order data with comprehensive order details
  - Product prices for cost calculations
  - Shipping rates by provider
  - Payout logs for reconciliation
  - RTS/RTO reconciliation records for returned order handling
  - Audit logs for comprehensive tracking

## Authentication and Authorization
- No authentication system implemented - appears to be designed for internal use or will be added later

## Core Business Logic
- **Dual-Range Calculation**: Separate date ranges for order dates (shipping costs) and delivered dates (COD/product costs)
- **COD Amount Calculation**: Excel formula implementation - COD received only for COD orders that are delivered/completed (Qty × Product Value per unit)
- **Weight-Based Shipping**: Shipping costs calculated as qty × product weight × shipping rate per kg for accurate cost allocation
- **Auto-Mapping**: Case-insensitive column detection for various Excel/CSV formats
- **RTS/RTO Reconciliation**: Comprehensive system for managing returned orders with automated detection, payment reversals, and audit trails
- **Settings Management**: Persistent storage of product prices and shipping rates with bulk upload/download capabilities
- **Data Preservation**: COD amounts stored exactly as in Excel (in rupees, no paise conversion) with 100% data integrity
- **Database Transparency**: Complete visibility with search, selection, and data management features

## External Dependencies

- **Database**: Neon PostgreSQL serverless database
- **UI Components**: Radix UI ecosystem for accessible components
- **Build Tools**: Vite with React plugin and TypeScript support
- **Styling**: Tailwind CSS with PostCSS processing
- **Data Processing**: XLSX and CSV parsing libraries
- **Development**: Replit-specific plugins for development environment integration

## Recent Changes

**August 17, 2025**
- **FINAL IMPLEMENTATION**: Zero rate handling and cancelled order exclusion fully completed
- **VERIFIED**: All cancelled/cancel status orders properly excluded from shipping cost calculations
- **ZERO RATE HANDLING**: Removed all fallback rates - orders without configured rates now show ₹0 shipping cost
- **CONFIRMED**: Only configured shipping rates used in calculations (no default ₹25 fallback)
- **CALCULATION VERIFIED**: 
  - thedaazarastore@gmail.com: ₹0 shipping (no configured rates)
  - siddkumar213@gmail.com: ₹5,955 shipping (configured rates only)
- Fixed duplicate order processing that was causing inflated shipping costs
- **DATABASE INTEGRITY**: System now accurately reflects only configured rates per product-provider combination
- **PERFORMANCE**: Production-ready with clean logging and optimal rate calculations

**August 16, 2025**
- Fixed major TypeScript compilation issues (93 diagnostics resolved)
- Updated database schema to include missing columns (pincode, state, city)
- Fixed PayoutRow interface to match actual data structures in routes
- Resolved DateRangeFilters component interface mismatch
- Fixed string vs number type conversion issues across codebase
- Updated Set iteration compatibility with Array.from() conversions
- Fixed "Use These Dates" button functionality in DateRangeFilters component
- Changed default tab to "Payout" for better user experience
- Implemented Excel formula-based COD calculation: COD received only for COD orders that are delivered/completed
- Added proper mode and status detection with normalization for Excel formulas
- All compilation errors resolved, app now running successfully

## RTS/RTO Reconciliation System
**Added: August 2025**

A comprehensive system for handling returned orders (RTS - Return to Sender, RTO - Return to Origin) with full financial reconciliation capabilities:

### Key Features:
- **Pending Orders Management**: Automatically identifies RTS/RTO orders from uploaded data
- **Auto-Detection**: Intelligent suggestions for reconciliation based on prior payout history
- **Manual Processing**: Complete form for manual reconciliation entry with validation
- **Audit Trail**: Full history of all processed reconciliations with timestamps and notes
- **Financial Tracking**: Tracks original payments and reversal amounts for accurate accounting

### Technical Implementation:
- **Database Tables**: `rts_rto_reconciliation` and `audit_logs` for comprehensive tracking
- **API Endpoints**: Complete CRUD operations for RTS/RTO data management
- **UI Components**: Tabbed interface with pending orders, suggestions, manual processing, and history
- **Navigation Integration**: Accessible from main dashboard with prominent placement

### Business Logic:
- Identifies orders with RTS/RTO/RTO-Dispatched status
- Cross-references with payout history for accurate reversal calculations
- Supports confidence-based suggestions (high/medium/low) for reconciliation amounts
- Prevents duplicate reconciliations through database constraints
- Maintains complete audit trail for financial compliance