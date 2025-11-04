import mariadb
import JomuelGraphs as JSM
import GabrielGraphs as GAB
import DanielGraphs as DAN
import matplotlib.pyplot as plt
from mariadb import Error, Cursor, Connection
from sqlalchemy import create_engine, text
import os
from datetime import datetime
# Connection details
DB_CONFIG = {
 'host': 'localhost',
 'user': 'coen2220',
 'password': 'coen2220',
 'database': 'Group07',
 'port': 3306,
}
# Graph saving configuration
GRAPHDIRECTORY = "graphs"
os.makedirs(GRAPHDIRECTORY, exist_ok=True)
# Function to save the graphs
def save_graph(graphName, presenterName):
    if plt.get_fignums():  # Check if there are any figures
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{presenterName}_{graphName}_{timestamp}.png"
        filepath = os.path.join(GRAPHDIRECTORY, filename)
        plt.savefig(filepath, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"Graph saved: {filepath}")
        return filepath
    else:
        print("No active figure to save")
        return None
def importDatabase():
    try:
# Connect to mariaDB
        connection : Connection = mariadb.connect(**DB_CONFIG)
        cursor : Cursor = connection.cursor()
# Create database if it doesn't exist
        cursor.execute("CREATE DATABASE IF NOT EXISTS Group07")
        cursor.execute("USE Group07")
        print("Database 'Group07' created/selected")
# Check for tables
        cursor.execute("SHOW TABLES")
        mariaDBTables = [table[0] for table in cursor.fetchall()]
        if mariaDBTables:
# Data already exists in MariaDB then just verify and use it
            print("Data already exists in MariaDB - using existing data")
            print(f"Tables found: {mariaDBTables}")
# Show row counts for verification
            print("\nExisting Data Summary:")
            print("-" * 40)
            for table in mariaDBTables:
                cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
                count = cursor.fetchone()[0]
                print(f"  {table}: {count:,} rows")
            print("Using existing MariaDB data - no import needed")
            return
# If data is empty
        print("Database is empty - importing data from SQLite with SQLAlchemy...")
# Connect to SQAlchemy .db file
        engine = create_engine('sqlite:///Group07.db')
        SQAlchemyConnection = engine.connect()
# Get all tables from SQLAlchemy
        result = SQAlchemyConnection.execute(text("SELECT name FROM sqlite_master WHERE type='table';"))
        tables = [row[0] for row in result]
        print("Tables found in SQAlchemy:", tables)
        for table in tables:
            print("Processing table: ", table)
# Get table structure from the SQAlchemy file
        result = SQAlchemyConnection.execute(text(f"PRAGMA table_info(`{table}`)"))
        columns = result.fetchall()
        print(f"Columns: {[col[1] for col in columns]}")
# Create MariaDB table with proper syntax
        createSQLInMariaDB = f"CREATE TABLE IF NOT EXISTS `{table}` ("
        for col in columns:
            columnName = col[1]
            columnType = col[2].upper() if col[2] else 'TEXT'
# Convert SQAlchemy types to MariaDB types
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
# Copy all data from SQAlchemy to MariaDB
        result = SQAlchemyConnection.execute(text(f"SELECT * FROM `{table}`"))
        rows = result.fetchall()
        if rows:
# Ensure rows are in the correct format (list of tuples)
            rowsAsTuples = [tuple(row) for row in rows]
# Insert all the info into the new tables
            columnNames = [col[1] for col in columns]
            placeholders = ', '.join(['%s'] * len(columnNames))
            insertInfo = f"INSERT INTO `{table}` ({', '.join([f'`{name}`' for name in columnNames])}) VALUES ({placeholders})"
# Insert in batches of 1000
            batch_size = 1000
            total_batches = (len(rowsAsTuples) - 1) // batch_size + 1
            for i in range(0, len(rowsAsTuples), batch_size):
                batch = rowsAsTuples[i:i + batch_size]
                cursor.executemany(insertInfo, batch)
                print(f" Inserted batch {i // batch_size + 1}/{total_batches}!")
            print(f"Total {len(rowsAsTuples)} rows inserted into {table}!")
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
    except Error as error:
        print("Error:", error)
    finally:
# Close connections in finally block
        if connection:
            connection.close()
def showJomuelGrahps():
    print("\n" + "=" * 60)
    print("JOMUEL'S GRAPHS")
    print("=" * 60)
    graphs = [
        ("Revenue_by_Product_Category", JSM.fetchQUERY01),
        ("Seasonal_Purchasing_Patterns", JSM.fetchQUERY02),
        ("European_Countries_Analysis", JSM.fetchQUERY03),
        ("Customer_Repeat_Behavior", JSM.fetchQUERY04),
        ("Annual_Revenue_Analysis", JSM.fetchQUERY05)
    ]
# Show graphs
    input("Press Enter to start showing all Jomuel graphs...")
    for (graphName, graph_function) in graphs:
        fig = graph_function()
        if fig:
            save_graph(graphName, "Jomuel_Graph")  # Save the returned figure
            plt.show()
            plt.close(fig)
def showDanielGraphs():
    print("\n" + "=" * 60)
    print("DANIEL'S GRAPHS")
    print("=" * 60)
    graphs = [
        ("Total_Products_Sold", DAN.plot_total_products_sold),
        ("Best_Selling_Products", DAN.plot_best_selling_products),
        ("Revenue_by_Country", DAN.plot_total_revenue_by_country),
        ("Top_Customers", DAN.plot_top_customers),
        ("Average_Revenue", DAN.plot_avg_revenue)
    ]
# Show graphs
    input("Press Enter to start showing all Daniel graphs...")
    for (graphName, graph_function) in graphs:
        fig = graph_function()
        if fig:
            save_graph(graphName, "Daniel Graph")  # Save the returned figure
            plt.show()
            plt.close(fig)
def showGabrielGraphs():
    print("\n" + "=" * 60)
    print("GABRIEL'S GRAPHS")
    print("=" * 60)
# Show graphs
    input("Press Enter to start showing all Gabriel graphs...")
    GAB.plot_bar()
def main():
# Call the import function
    importDatabase()
# Professional presentation flow
    print("\n" + "=" * 70)
    print("GROUP 07 - DATA VISUALIZATION PRESENTATION")
    print("=" * 70)
# Call First person to present
    print("Now printing the graphs in order(Jomuel, Daniel, Gabriel):")
    showJomuelGrahps()
    showDanielGraphs()
    showGabrielGraphs()
# Show summary
    print(f"\n🎉 All graphs completed and saved in '{GRAPHDIRECTORY}' directory!")
if __name__ == '__main__':
    main()