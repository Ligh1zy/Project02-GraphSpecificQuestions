from sqlalchemy import create_engine, text
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import mariadb
# -------------------------
# 1 Database connection
# -------------------------
USER = "coen2220"
PASSWORD = "coen2220"
HOST = "localhost"
PORT = 3306
DBNAME = "Group07"
engine = create_engine(f"mariadb+mariadbconnector://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}")
# -------------------------
# 2 Queries
# -------------------------
queries = {
    "Best-selling products": """
        SELECT Description, SUM(Quantity) AS TotalSold
        FROM data
        WHERE Quantity > 0 
          AND Description IS NOT NULL 
          AND Description != ''
          AND LENGTH(Description) > 3
        GROUP BY Description
        ORDER BY TotalSold DESC
        LIMIT 10;
    """,

    "Top orders by country": """
        SELECT Country, COUNT(DISTINCT Invoice) AS TotalOrders
        FROM data
        WHERE Country IS NOT NULL AND Country != ''
        GROUP BY Country
        ORDER BY TotalOrders DESC
        LIMIT 10;
    """,

    "Top countries by average order value": """
        SELECT Country,
               SUM(Quantity * Price) / COUNT(DISTINCT Invoice) AS AvgOrderValue
        FROM data
        WHERE Country IS NOT NULL AND Country != ''
          AND Quantity > 0
        GROUP BY Country
        HAVING COUNT(DISTINCT Invoice) >= 5  -- Only countries with at least 5 orders
        ORDER BY AvgOrderValue DESC
        LIMIT 10;
    """,

    "Top customers who made separate purchases": """
        SELECT `customer id` AS CustomerID, COUNT(DISTINCT Invoice) AS PurchaseCount
        FROM data
        WHERE `customer id` IS NOT NULL 
          AND `customer id` != ''
          AND LENGTH(`customer id`) > 1
        GROUP BY `customer id`
        ORDER BY PurchaseCount DESC
        LIMIT 10;
    """,

    "Products with most returns": """
        SELECT Description, ABS(SUM(Quantity)) AS ReturnedUnits
        FROM data
        WHERE Quantity < 0
        AND Description IS NOT NULL 
        AND Description != ''
        AND Description NOT LIKE '%missing%'
        GROUP BY Description
        ORDER BY ReturnedUnits DESC
        LIMIT 10;
    """
}
# -------------------------
# 3 Plotting function
# -------------------------
def create_bar_plot(df, x_col, y_col, title, palette="viridis", save_path=None):
    sns.set(style="whitegrid")
    plt.figure(figsize=(12, 6))
    sns.barplot(x=x_col, y=y_col, data=df, palette=palette, hue=y_col, dodge=False, legend=False)
    plt.title(title)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.tight_layout()
    plt.show()
# -------------------------
# 4 Execute queries & plot
# -------------------------
def plot_bar():
    os.makedirs("graphs", exist_ok=True)  # Folder to save graphs
    with engine.connect() as conn:
        for title, q in queries.items():
            try:
                df = pd.read_sql_query(text(q), conn)
                print(f"\n{title}:\n", df)
# Check if DataFrame is empty
                if df.empty:
                    print(f"No data found for: {title}")
                    continue
# Determine which columns to use for x/y
                if "Description" in df.columns:
                    x_col, y_col = "Description", "TotalSold" if "TotalSold" in df.columns else "ReturnedUnits"
                elif "Country" in df.columns:
                    x_col = "Country"
                    y_col = "TotalOrders" if "TotalOrders" in df.columns else "AvgOrderValue"
                else:
                    x_col, y_col = "CustomerID", "PurchaseCount"
# Save plot in 'graphs' folder
                save_file = os.path.join("graphs", title.replace(" ", "_") + ".png")
# Call the plotting function
                create_bar_plot(df, x_col, y_col, title, save_path=save_file)
            except Exception as e:
                print(f"Error executing query '{title}': {e}")
                continue
    print("Graph generation completed!")