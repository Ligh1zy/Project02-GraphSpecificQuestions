import mariadb
import JomuelGraphs as JSM
from mariadb import Error, Cursor, Connection
from sqlalchemy import create_engine, text

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
        connection : Connection = mariadb.connect(**DB_CONFIG)
        cursor : Cursor = connection.cursor()
# Create database if it doesn't exist
        cursor.execute("CREATE DATABASE IF NOT EXISTS Group07")
        cursor.execute("USE Group07")
        print("Database 'Group07' created/selected")
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
    except mariadb.Error as error:
        print(error)
    except Exception as e:
        print("Error:", e)
    finally:
# Close connections in finally block
        if SQAlchemyConnection:
            SQAlchemyConnection.close()
        if engine:
            engine.dispose()
        if connection:
            connection.close()
if __name__ == '__main__':
    #importDatabase()
    #JSM.fetchQUERY01()
    JSM.fetchQUERY03()