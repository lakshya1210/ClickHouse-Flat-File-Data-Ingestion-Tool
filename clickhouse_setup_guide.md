# ClickHouse Setup Guide for ZeoTap Integration

This guide will walk you through the process of installing ClickHouse on your Mac and setting up a database for use with the ZeoTap Integration tool.

## Step 1: Install ClickHouse

### Using Homebrew (Recommended)

1. Open Terminal (you can find it in Applications > Utilities > Terminal)

2. Install Homebrew if you don't have it already:
   ```
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   ```

3. Install ClickHouse using Homebrew:
   ```
   brew install clickhouse
   ```

### Alternative: Manual Installation

If you prefer not to use Homebrew:

1. Download the ClickHouse binary from the official website: https://clickhouse.com/docs/en/install

2. Follow the installation instructions for macOS on the website.

## Step 2: Start ClickHouse Server

1. Start the ClickHouse server using Homebrew:
   ```
   brew services start clickhouse
   ```

   Or if you installed manually, use the command provided in the installation instructions.

2. Verify that ClickHouse is running by connecting to it:
   ```
   clickhouse-client
   ```

   You should see a prompt like this:
   ```
   ClickHouse client version 23.x.x.x
   Connecting to localhost:9000 as user default.
   Connected to ClickHouse server version 23.x.x.
   
   localhost :) 
   ```

   Type `exit` to leave the client.

## Step 3: Create a Database

1. Connect to ClickHouse using the client:
   ```
   clickhouse-client
   ```

2. Create a new database (replace "zeotap_db" with your preferred database name):
   ```sql
   CREATE DATABASE zeotap_db;
   ```

3. Verify the database was created:
   ```sql
   SHOW DATABASES;
   ```

   You should see your new database in the list.

4. Switch to your new database:
   ```sql
   USE zeotap_db;
   ```

## Step 4: Configure the ZeoTap Integration Tool

1. Open the ZeoTap Integration application in your web browser (it should be running at http://localhost:8080 if you started it with the default settings).

2. When configuring ClickHouse as a source or target, use these settings:
   - Host: localhost
   - Port: 8123 (HTTP interface port)
   - Database: zeotap_db (or whatever name you chose)
   - User: default
   - JWT Token: (leave empty unless you've configured authentication)
   - Secure: No (unless you've configured HTTPS)

3. Click "Test Connection" to verify that the application can connect to your ClickHouse database.

## Step 5: Create a Table (Optional)

You can create a table manually in ClickHouse, or let the ZeoTap Integration tool create it for you when importing data from a flat file.

To create a table manually:

1. Connect to ClickHouse:
   ```
   clickhouse-client --database=zeotap_db
   ```

2. Create a table (example):
   ```sql
   CREATE TABLE example_table (
     id UInt32,
     name String,
     value Float64,
     created_date Date
   ) ENGINE = MergeTree()
   ORDER BY id;
   ```

3. Verify the table was created:
   ```sql
   SHOW TABLES;
   ```

## Step 6: Using the ZeoTap Integration Tool

### To Import Data from a CSV File to ClickHouse:

1. In the ZeoTap Integration tool, select "Flat File" as Source and "ClickHouse" as Target.
2. Configure your flat file source (select your CSV file, delimiter, etc.).
3. Configure your ClickHouse target using the settings from Step 4.
4. Enter a target table name (either an existing table or a new one to be created).
5. Select the columns you want to import.
6. Click "Start Ingestion" to begin the data transfer.

### To Export Data from ClickHouse to a CSV File:

1. Select "ClickHouse" as Source and "Flat File" as Target.
2. Configure your ClickHouse source using the settings from Step 4.
3. Select the source table and columns you want to export.
4. Configure your flat file target (file path, delimiter, etc.).
5. Click "Start Ingestion" to begin the data transfer.

## Troubleshooting

### Common Issues:

1. **Connection Refused**: Make sure the ClickHouse server is running. Try restarting it with:
   ```
   brew services restart clickhouse
   ```

2. **Authentication Failed**: By default, ClickHouse uses the "default" user with no password. If you've configured authentication, make sure you're using the correct credentials.

3. **Database Not Found**: Make sure you've created the database and are using the correct database name.

4. **Table Not Found**: Make sure you've created the table or are letting the application create it for you.

### Checking ClickHouse Logs:

If you're having issues, check the ClickHouse logs:
```
brew services info clickhouse
```

Or look at the log files directly:
```
cat /usr/local/var/log/clickhouse-server/clickhouse-server.log
```

## Additional Resources

- [ClickHouse Documentation](https://clickhouse.com/docs/)
- [ClickHouse SQL Reference](https://clickhouse.com/docs/en/sql-reference/)
- [Example Datasets](https://clickhouse.com/docs/getting-started/example-datasets)