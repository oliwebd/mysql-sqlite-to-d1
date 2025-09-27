# mysql-sqlite-to-d1

**Version:** 1.0.0  
**Description:** MySQL to Cloudflare D1 Database migration in Node.js

---

## 📦 Installation

1. Clone the repository:

```bash
git clone https://github.com/oliwebd/mysql-sqlite-to-d1.git .

```

2. Install dependencies:

```bash
npm install
```
---

## 🔧 Environment Setup

Create a `.env` file in the project root to store your credentials.  

### Example `.env`:

```env
# MySQL connection URL
MYSQL_URL="mysql://username:password@localhost:3306/mysqldbname"

# Cloudflare account details
CLOUDFLARE_ACCOUNT_ID="xxxoxxxxxxxxxx"
CLOUDFLARE_API_TOKEN="xxxox-xxxx-xxxx-xxxx"

# Cloudflare D1 database IDs
D1_DATABASE_ID="xxxox-xxxxx-xxxx"
```

---

## ⚡ Scripts

| Script | Description |
|--------|-------------|
| `npm run migrate` | Runs full SQLite → D1 migration. 
| `npm run test-d1-conn` | Test connection to Cloudflare D1 / Check API Troken Ok or But you Run migrate Directly with first option. |
| `npm run start` | Generate SQL from MySQL and create local SQLite test DB |

---

## 🚀 Overview

This Node.js project allows you to migrate MySQL databases to **Cloudflare D1**, using SQLite as an intermediate step for testing. The workflow ensures:

- Schema generation from MySQL / auto dump file for simple migration 
- Data transfer verification
- Clean D1 data before migration without conflicts 
- Batch insertion for performance / 4k row in 30sec
- Foreign key safety during migration / if Foreign key Error during migrations Just delete Key from MySQL then again migrate then make sql query to add in d1/sqlite manually 

---

## 🛠 Features

- Connects to MySQL and generates a complete SQL dump.
- Creates a test SQLite database to verify migration.
- Migrates data into Cloudflare D1, skipping the `_cf_KV` table.
- Batch processing for large datasets.
- Verifies migration by comparing row counts.

---

## 📝 Usage

### Step 1: Generate SQL from MySQL

```bash
npm run start
```

- Connects to MySQL
- Generates `temp/migration.sql`
- Creates `localsqlite.db` for testing

### Step 2: Migrate SQLite → Cloudflare D1

```bash
npm run migrate
```

- Cleans D1 database (skipping `_cf_KV`)
- Creates table schemas
- Inserts data in batches
- Verifies row counts for accuracy

---

## ✅ Example Output

```
🚀 Starting SQLite file → D1 migration...
📖 Reading migration file...
Found 4 CREATE TABLE statements
Found 1739 INSERT statements
...
🎉 Perfect migration! All 4 tables match.
```

---

## 🔑 Dependencies

- [axios](https://www.npmjs.com/package/axios)
- [better-sqlite3](https://www.npmjs.com/package/better-sqlite3)
- [dotenv](https://www.npmjs.com/package/dotenv)
- [mysql2](https://www.npmjs.com/package/mysql2)
- [sqlite3](https://www.npmjs.com/package/sqlite3)

---

## ⚙️ Notes

- SQLite timestamps are stored in ISO format.
- Foreign keys are temporarily disabled during migration for safety. Still d1 check foreign key by default.
- `_cf_KV` table in D1 is never dropped / Default System Table for D1.
- Migration is verified by comparing row counts; mismatched tables are flagged.

---

## 🔗 References

- [Cloudflare D1 Docs](https://developers.cloudflare.com/d1/)
- [Node.js](https://nodejs.org/)

---

## 📜 License

GPLv3

---

## 👤 Author

Oli Miah 
- [Full Stack Web Developer](https://olimiah.vercel.app/) 

--- 

## Buy Me Coffee ☕ 

PayPal: [Buy 💰](https://www.paypal.me/Oli2025)
