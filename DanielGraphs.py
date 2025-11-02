# ProjectDatabase 1.8 — MariaDB Visual Pro Edition
# Daniel Vega Rivera
# R00299178

import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import time

# DATABASE CONNECTION
engine = create_engine(
    "mariadb+mariadbconnector://coen2220:coen2220@127.0.0.1:3306/Group07",
    pool_pre_ping=True
)

print("Daniel Vega Rivera")

# 1️ Total Products Sold by Country
def plot_total_products_sold():
    query = """
        SELECT Country, SUM(Quantity) AS TotalSales
        FROM data
        GROUP BY Country
        ORDER BY TotalSales DESC
        LIMIT 10;
    """
    df = pd.read_sql(query, engine)
    if df.empty:
        print("No data found for this query.\n")
        return

    plt.figure(figsize=(10, 6))
    colors = plt.cm.Paired(range(len(df)))
    bars = plt.barh(df["Country"], df["TotalSales"], color=colors)
    plt.title("Total Products Sold by Country", fontsize=14, fontweight="bold")
    plt.xlabel("Total Sales")
    plt.ylabel("Country")
    plt.gca().invert_yaxis()

    for bar in bars:
        plt.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                 f"{int(bar.get_width())}", va='center', fontsize=9)

    total_sum = df["TotalSales"].sum()
    plt.legend([f"Total Sales: {total_sum}"], loc="lower right")
    plt.tight_layout()
    plt.show()

# 2️ Top 10 Best-Selling Products
def plot_best_selling_products():
    query = """
        SELECT Description, SUM(Quantity) AS TotalQuantity
        FROM data
        GROUP BY Description
        ORDER BY TotalQuantity DESC
        LIMIT 10;
    """
    df = pd.read_sql(query, engine)
    if df.empty:
        print("No data found for this query.\n")
        return

    plt.figure(figsize=(9, 7))
    wedges, texts, autotexts = plt.pie(
        df["TotalQuantity"],
        autopct="%1.1f%%",
        startangle=140,
        colors=plt.cm.tab10.colors
    )
    plt.setp(autotexts, size=9, weight="bold", color="white")
    plt.title("Top 10 Best-Selling Products Globally", fontsize=14, fontweight="bold")

    plt.legend(
        df["Description"],
        title="Products",
        loc="center left",
        bbox_to_anchor=(1, 0.5),
        fontsize=8
    )
    plt.tight_layout()
    plt.show()

# 3️ Top 10 Countries by Total Revenue
def plot_total_revenue_by_country():
    query = """
        SELECT Country, ROUND(SUM(Quantity * Price), 2) AS Revenue
        FROM data
        GROUP BY Country
        ORDER BY Revenue DESC
        LIMIT 10;
    """
    df = pd.read_sql(query, engine)
    if df.empty:
        print("No data found for this query.\n")
        return

    plt.figure(figsize=(10, 6))
    plt.plot(df["Country"], df["Revenue"], marker="o", color="blue", label="Revenue ($)")
    plt.fill_between(df["Country"], df["Revenue"], color="skyblue", alpha=0.3)
    plt.title("Top 10 Countries by Total Revenue", fontsize=14, fontweight="bold")
    plt.xlabel("Country")
    plt.ylabel("Revenue")

    for i, val in enumerate(df["Revenue"]):
        plt.text(i, val + 0.5, f"{val:.0f}", ha="center", fontsize=9, color="black")

    plt.legend()
    plt.tight_layout()
    plt.show()

# 4️ Top 10 Customers by Total Spending
def plot_top_customers():
    query = """
        SELECT `Customer ID` AS CustomerID, ROUND(SUM(Quantity * Price), 2) AS TotalSpent
        FROM data
        WHERE `Customer ID` IS NOT NULL
        GROUP BY `Customer ID`
        ORDER BY TotalSpent DESC
        LIMIT 10;
    """
    df = pd.read_sql(query, engine)
    if df.empty:
        print("No data found for this query.\n")
        return

    plt.figure(figsize=(10, 6))
    plt.scatter(df["CustomerID"], df["TotalSpent"], color="limegreen", s=100, label="Customers")
    plt.title("Top 10 Customers by Total Spending", fontsize=14, fontweight="bold")
    plt.xlabel("Customer ID")
    plt.ylabel("TotalSpent")

    for i in range(len(df)):
        plt.text(df["CustomerID"][i], df["TotalSpent"][i] + 1,
                 f"{df['TotalSpent'][i]:.0f}", ha='center', fontsize=9, color='black')

    plt.legend()
    plt.tight_layout()
    plt.show()

# 5️ Average Revenue per Country
def plot_avg_revenue():
    query = """
        SELECT Country, ROUND(AVG(Quantity * Price), 2) AS AvgRevenue
        FROM data
        GROUP BY Country
        ORDER BY AvgRevenue DESC;
    """
    df = pd.read_sql(query, engine)
    if df.empty:
        print("No data found for this query.\n")
        return

    plt.figure(figsize=(10, 6))
    bars = plt.bar(df["Country"], df["AvgRevenue"], color="#4A90E2", alpha=0.8)
    plt.title("Average Revenue per Country", fontsize=14, fontweight="bold")
    plt.xlabel("Country")
    plt.ylabel("AvgRevenue")
    plt.xticks(rotation=45)

    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval, f"{int(yval)}",
                 ha="center", va="bottom", fontsize=9)

    plt.tight_layout()
    plt.show()
