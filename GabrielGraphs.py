from sqlalchemy import create_engine, text
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
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
    df[x_col] = pd.Categorical(df[x_col], categories=df[x_col], ordered=True)
    plt.figure(figsize=(12, 6))
    bars = sns.barplot(x=x_col, y=y_col, data=df, palette=palette, hue=y_col, dodge=False, legend=False)
    plt.title(title)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.xticks([])
# Add value labels on top of bars
    for i, (value, row) in enumerate(zip(df[y_col], df.iterrows())):
        plt.text(i, value + (value * 0.01), f'{value:,.0f}',
                 ha='center', va='bottom', fontsize=9, fontweight='bold')

# Create custom legend based on the chart type
    if "Sold" in y_col or "TotalSold" in y_col:
        total = df[y_col].sum()
        legend_text = f"Total Units Sold: {total:,.0f}"
        plt.legend([legend_text], loc='upper right', frameon=True, fancybox=True, shadow=True)

    elif "Orders" in y_col:
        total = df[y_col].sum()
        legend_text = f"Total Orders: {total:,.0f}"
        plt.legend([legend_text], loc='upper right', frameon=True, fancybox=True, shadow=True)

    elif "Value" in y_col or "AvgOrderValue" in y_col:
        avg_value = df[y_col].mean()
        legend_text = f"Average: ${avg_value:,.2f}"
        plt.legend([legend_text], loc='upper right', frameon=True, fancybox=True, shadow=True)

    elif "Purchase" in y_col:
        total_purchases = df[y_col].sum()
        legend_text = f"Total Purchases: {total_purchases:,.0f}"
        plt.legend([legend_text], loc='upper right', frameon=True, fancybox=True, shadow=True)

    elif "Return" in y_col or "ReturnedUnits" in y_col:
        total_returns = df[y_col].sum()
        legend_text = f"Total Returns: {total_returns:,.0f}"
        plt.legend([legend_text], loc='upper right', frameon=True, fancybox=True, shadow=True)
# Create x-axis legend elements
    x_legend = [f"{i + 1}. {label}" for i, label in enumerate(df[x_col])]
    bar_colors = [bar.get_facecolor() for bar in bars.patches]
    reversed_colors = list(reversed(bar_colors))
    legend_elements = [Patch(facecolor=reversed_colors[i],
                             label=f"{i + 1}. {label}")
                       for i, label in enumerate(df[x_col])]
# Save the first legend and add it back after creating the second
    first_legend = plt.gca().get_legend()
# Create the x-axis legend
    plt.legend(handles=legend_elements, labels=x_legend,
               title=f"{x_col} List",
               loc='center left',
               bbox_to_anchor=(1.05, 0.5),
               frameon=True,
               fancybox=True,
               shadow=True,
               fontsize=9)

# Add the first legend back
    if first_legend:
        plt.gca().add_artist(first_legend)
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Saved: {save_path}")
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