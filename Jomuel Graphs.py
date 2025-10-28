import mariadb # To import the SQL file
from mariadb import Error, Cursor, Connection
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
# Connection details
DB_CONFIG = {
 'host': 'localhost',
 'user': 'coen2220',
 'password': 'coen2220',
 'database': 'Group07',
 'port': 3306,
}
def importDatabase():
    try:
# Connect to mariaDB
        connection = mariadb.connect(**DB_CONFIG)
        cursor = connection.cursor()
# Create database if it doesn't exist
        cursor.execute("CREATE DATABASE IF NOT EXISTS Group07")
        cursor.execute("USE Group07")
        print("Database 'Group07' created/selected")
# Connect to SQLite .db file
        SQLiteConnection = sqlite3.connect('Group07.db')
        SQLiteCursor = SQLiteConnection.cursor()
# Get all tables from SQLite
        SQLiteCursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in SQLiteCursor.fetchall()]
        print("Tables found in SQLite:", tables)
        for table in tables:
            print("Processing table: ", table)
# Get table structure from the SQLite file        
        SQLiteCursor.execute(f"PRAGMA table_info(`{table}`)")
        columns = SQLiteCursor.fetchall()
        print(f"Columns: {[col[1] for col in columns]}")
# Create MariaDB table with proper syntax
        createSQLInMariaDB = f"CREATE TABLE IF NOT EXISTS `{table}` ("
        for col in columns:
            columnName = col[1]
            columnType = col[2].upper() if col[2] else 'TEXT'
# Convert SQLite types to MariaDB types
            if 'INT' in columnType:
                mariaType = 'INT'
            elif 'REAL' in columnType or 'FLOAT' in columnType or 'DOUBLE' in columnType:
                mariaType = 'DECIMAL(10,2)'
            elif 'BOOL' in columnType:
                mariaType = 'BOOLEAN'
            else:
                mariaType = 'TEXT'
            createSQLInMariaDB += f"`{columnName}` {mariaType}, "
        createSQLInMariaDB = createSQLInMariaDB.rstrip(', ') + ")"
# Create table in MariaDB
        cursor.execute(createSQLInMariaDB)
        print(f"Table created: {table}")
# Copy all data from SQLite to MariaDB
        SQLiteCursor.execute(f"SELECT * FROM `{table}`")
        rows = SQLiteCursor.fetchall()
        if rows:
# Insert all the info into the new tables
            columnNames = [col[1] for col in columns]
            placeholders = ', '.join(['%s'] * len(columnNames))
            insertInfo = f"INSERT INTO `{table}` ({', '.join([f'`{name}`' for name in columnNames])}) VALUES ({placeholders})"
# Insert in batches of 1000
            batch_size = 1000
            total_batches = (len(rows) - 1) // batch_size + 1
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                cursor.executemany(insertInfo, batch)
                print(f" Inserted batch {i // batch_size + 1}/{total_batches}!")
            print(f"Total {len(rows)} rows inserted into {table}!")
        else:
            print(f"??No data in {table}")
        connection.commit()
        print("Migration completed successfully!")
# Verify the migration
        print("Verification:")
        cursor.execute("SHOW TABLES")
        final_tables = [table[0] for table in cursor.fetchall()]
        print(f"Tables in MariaDB: {final_tables}")
        for table in final_tables:
            cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
            count = cursor.fetchone()[0]
            print(f"  {table}: {count} rows")
        SQLiteConnection.close()
        connection.close()
    except Exception as e:
        print(f"Error: {e}")
SQL_Query01 = ("SELECT CASE "
                "WHEN Description LIKE '%LIGHT%' THEN 'Lighting' "
                "WHEN Description LIKE '%MUG%' OR Description LIKE '%CUP%' THEN 'Drinkware' "
                "WHEN Description LIKE '%BAG%' THEN 'Bags'"
                "WHEN Description LIKE '%TOY%' THEN 'Toys' "
                "WHEN Description LIKE '%DECORATION%' THEN 'Decorations' "
                "ELSE 'Other' "
                "END as Category, "
                "SUM(Price * Quantity) as TotalRevenue, "
                "COUNT(*) as ItemCount "
                "FROM januany_2010_2011 "
                "GROUP BY category "
                "ORDER BY TotalRevenue DESC")
def fetchQUERY01():
    connection: Connection = None
    try:
# Establish connection to query01
        connection = mariadb.connect(**DB_CONFIG)
        cursor: Cursor = connection.cursor(dictionary=True)
        print("--- MariaDB Connector: QUERY01 ---")
# Execute query
        cursor.execute(SQL_Query01)
# Fetch all records at once (fast for small sets)
        results = cursor.fetchall()
# Get the info for the graph
        DataFrame = pd.DataFrame(results)
        Revenue = DataFrame['TotalRevenue']
        Count = DataFrame['ItemCount']
        Category = DataFrame['Category']
        explode = (0.1, 0.1, 0.1, 0.1, 0.1, 0.1)
        print(DataFrame)
        plt.figure(figsize=(10, 8))
        plt.pie(x=Revenue, labels=Category, explode=explode, autopct='%1.1f%%', startangle=90,
                           colors=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD'], shadow=True)
        plt.title('Which product categories generate the most revenue from January 2010-2011 on United Kingdom?', size=15, fontweight='bold')
        plt.axis('equal')
        plt.show()
    except Error as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
# Always close the connection
         if connection:
            connection.close()
            cursor.close()
SQL_Query02 = ("SELECT Country, CASE "
               "WHEN MONTH(STR_TO_DATE(InvoiceDate, '%m/%d/%Y %H:%i:%s')) IN (12, 1, 2) THEN 'Winter'"
               "WHEN MONTH(STR_TO_DATE(InvoiceDate, '%m/%d/%Y %H:%i:%s')) IN (3, 4, 5) THEN 'Spring' "
               "WHEN MONTH(STR_TO_DATE(InvoiceDate, '%m/%d/%Y %H:%i:%s')) IN (6, 7, 8) THEN 'Summer' "
               "WHEN MONTH(STR_TO_DATE(InvoiceDate, '%m/%d/%Y %H:%i:%s')) IN (9, 10, 11) THEN 'Autumn' "
               "END AS Season, "
               "COUNT(*) as TransactionCount, "
               "SUM(Price * Quantity) as TotalRevenue, "
               "AVG(Price) as AverageSpending, "
               "COUNT(DISTINCT Invoice) as UniqueCustomers "
               "FROM purchasingandreapeatingpattens "
               "WHERE Country IN ('Germany', 'France', 'EIRE') "
               "GROUP BY Country, Season "
               "ORDER BY FIELD(Country, 'Germany', 'France', 'EIRE'), "
               "FIELD(Season, 'Winter', 'Spring', 'Summer', 'Autumn')")
def fetchQUER02():
    connection: Connection = None
    try:
# Establish connection to query01
        connection = mariadb.connect(**DB_CONFIG)
        cursor: Cursor = connection.cursor()
        print("--- MariaDB Connector: QUERY02 ---")
# Execute query
        cursor.execute(SQL_Query02)
# Fetch all records at once (fast for small sets)
        results = cursor.fetchall()
        columns = ['Country', 'Season', 'TransactionCount', 'TotalRevenue', 'AverageSpending', 'UniqueCustomers']
# Get the info for the graph
        DataFrame = pd.DataFrame(results, columns=columns)
        print(DataFrame)
        Revenue = DataFrame['TotalRevenue']
        TransactionCount = DataFrame['TransactionCount']
        AverageSpending = DataFrame['AverageSpending']
        UniqueCustomers = DataFrame['UniqueCustomers']
        Seasons = DataFrame['Season']
# Create the graph
        plt.figure(figsize=(12, 8))
        heatmapData = DataFrame.pivot(index='Season', columns='Country', values='TotalRevenue')
        season_order = ['Winter', 'Spring', 'Summer', 'Autumn']
        heatmapData = heatmapData.reindex(season_order)
        data = heatmapData.values
        plt.imshow(data, cmap='viridis', aspect='auto')
        plt.colorbar(label='Total Revenue (€)')
        plt.xticks([0, 1, 2], heatmapData.columns)
        plt.yticks([0, 1, 2, 3], heatmapData.index)
        plt.title('How do purchasing patterns differ by season between Germany, France, and Ireland?', size=15, fontweight='bold')
        plt.xlabel('Country', size=11, fontweight='bold')
        plt.ylabel('Season', size=11, fontweight='bold')
        plt.show()
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if connection:
            connection.close()
            cursor.close()
SQL_Query03 = ("SELECT Country, CASE ")
def fetchQUER03():
    connection: Connection = None
# Run the complete migration
#importDatabase()
#fetchQUERY01()
#fetchQUER02()
