import mariadb # To import the SQL file
from mariadb import Error, Cursor, Connection
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from math import pi
import plotly.express as px

# Connection details
DB_CONFIG = {
 'host': 'localhost',
 'user': 'coen2220',
 'password': 'coen2220',
 'database': 'Group07',
 'port': 3306,
}
SQL_Query01 = ("""
SELECT 
    CASE 
        WHEN UPPER(Description) LIKE '%MUG%' OR UPPER(Description) LIKE '%CUP%' OR 
             UPPER(Description) LIKE '%TEA%' OR UPPER(Description) LIKE '%COFFEE%' THEN 'Drinkware & Tea Sets'
        WHEN UPPER(Description) LIKE '%BAG%' THEN 'Bags & Totes'
        WHEN UPPER(Description) LIKE '%T-LIGHT%' OR UPPER(Description) LIKE '%CANDLE%' OR 
             UPPER(Description) LIKE '%LIGHT%' THEN 'Lighting & Candles'
        WHEN UPPER(Description) LIKE '%TOY%' OR UPPER(Description) LIKE '%SOFT%' THEN 'Toys'
        WHEN UPPER(Description) LIKE '%DECORATION%' OR UPPER(Description) LIKE '%GARLAND%' OR 
             UPPER(Description) LIKE '%WREATH%' OR UPPER(Description) LIKE '%FRAME%' OR 
             UPPER(Description) LIKE '%MIRROR%' THEN 'Home Decor'
        WHEN UPPER(Description) LIKE '%APRON%' OR UPPER(Description) LIKE '%GLOVE%' OR 
             UPPER(Description) LIKE '%CUTLERY%' OR UPPER(Description) LIKE '%SPOON%' OR 
             UPPER(Description) LIKE '%LADLE%' OR UPPER(Description) LIKE '%PLATE%' OR 
             UPPER(Description) LIKE '%BOWL%' OR UPPER(Description) LIKE '%DISH%' OR 
             UPPER(Description) LIKE '%CAKE%' OR UPPER(Description) LIKE '%BAKING%' THEN 'Kitchen & Dining'
        WHEN UPPER(Description) LIKE '%HOT WATER BOTTLE%' THEN 'Home Comfort'
        WHEN UPPER(Description) LIKE '%SIGN%' THEN 'Signs & Wall Art'
        WHEN UPPER(Description) LIKE '%CUSHION%' OR UPPER(Description) LIKE '%POUFFE%' THEN 'Cushions & Soft Furnishings'
        WHEN UPPER(Description) LIKE '%DOORMAT%' THEN 'Doormats & Home Accessories'
        WHEN UPPER(Description) LIKE '%STICKER%' OR UPPER(Description) LIKE '%WRAP%' OR 
             UPPER(Description) LIKE '%PEN%' OR UPPER(Description) LIKE '%PENCIL%' THEN 'Stationery & Gift Wrap'
        WHEN UPPER(Description) LIKE '%UMBRELLA%' THEN 'Umbrellas & Rainwear'
        WHEN UPPER(Description) LIKE '%JEWELLERY%' OR UPPER(Description) LIKE '%NECKLACE%' OR 
             UPPER(Description) LIKE '%EARRING%' OR UPPER(Description) LIKE '%BRACELET%' THEN 'Jewellery & Accessories'
        ELSE 'Other'
    END as Category,
    SUM(Price * Quantity) as TotalRevenue,
    COUNT(*) as ItemCount
FROM januany_2010_2011
GROUP BY Category
ORDER BY TotalRevenue DESC
""")
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
        totalRevenue = sum(Revenue)
        percentages = [(rev / totalRevenue) * 100 for rev in Revenue]
# Create labels with percentages for legend
        legendLabels = [f"{category} ({percent:.1f}%)" for category, percent in zip(Category, percentages)]
        plt.figure(figsize=(12, 8))
        wedges, texts = plt.pie(x=Revenue, startangle=90,
                           colors=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD',
                                   '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E9', '#F8C471', '#AED6F1', '#30FF48', '#192AFF'],
                                           shadow=True, wedgeprops={'linewidth': 2, 'edgecolor': 'white'})
        plt.legend(wedges, legendLabels, title="Categories", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
        plt.title('Which product categories generate the most revenue \nfrom January 2010-2011 on United Kingdom?', size=15, fontweight='bold')
        plt.tight_layout()
        plt.show()
    except Error as e:
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
def fetchQUERY02():
    connection: Connection = None
    try:
# Establish connection
        connection = mariadb.connect(**DB_CONFIG)
        cursor: Cursor = connection.cursor()
        print("--- MariaDB Connector: QUERY02 ---")
# Execute query
        cursor.execute(SQL_Query02)
# Fetch all records
        results = cursor.fetchall()
        columns = ['Country', 'Season', 'TransactionCount', 'TotalRevenue', 'AverageSpending', 'UniqueCustomers']
# Create DataFrame
        DataFrame = pd.DataFrame(results, columns=columns)
# Create the bar chart
        plt.figure(figsize=(12, 8))
# Grouped bar chart by country and season
        countries = DataFrame['Country'].unique()
        seasons = ['Winter', 'Spring', 'Summer', 'Autumn']
# Set up the bar positions
        x = np.arange(len(countries))
        width = 0.2
        multiplier = 0
# Create grouped bars for each season
        for i, season in enumerate(seasons):
            seasonData = DataFrame[DataFrame['Season'] == season]
            revenueValues = []
            for country in countries:
                countryData = seasonData[seasonData['Country'] == country]
                if not countryData.empty:
                    revenueValues.append(countryData['TotalRevenue'].iloc[0])
                else:
                    revenueValues.append(0)
            offset = width * multiplier
            bars = plt.bar(x + offset, revenueValues, width, label=season)
            plt.bar_label(bars, padding=3, fmt='€%.0f')
            multiplier += 1
# Show graph
        plt.title('Total Revenue by Country and Season\nGermany, France, and Ireland 2009-2011', size=15, fontweight='bold')
        plt.xlabel('Country', size=11, fontweight='bold')
        plt.ylabel('Total Revenue (€)', size=11, fontweight='bold')
        plt.xticks(x + width * 1.5, countries)
        plt.legend(title='Season')
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        plt.show()
# Exceptions if they happen
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if connection:
            connection.close()
            cursor.close()
SQL_Query03 = ("""
SELECT Country, AVG(Price) as AVGItemPrice, COUNT(Distinct Invoice) as TotalTransactions, 
SUM(Price * Quantity) as TotalRevenue, AVG(Price * Quantity) as AverageTransactionValue,
SUM(Quantity) as ItemsSold FROM AveragePrice WHERE Country IN ('Germany', 'France', 'Belgium', 'Netherlands')
GROUP BY Country ORDER BY AverageTransactionValue DESC            """)
def fetchQUERY03():
    connection: Connection = None
    try:
# Establish connection
        connection = mariadb.connect(**DB_CONFIG)
        cursor: Cursor = connection.cursor()
        print("--- MariaDB Connector: QUERY03 ---")
# Execute query
        cursor.execute(SQL_Query03)
# Fetch all records
        results = cursor.fetchall()
        columns = ['Country', 'AvgItemPrice', 'TotalTransactions', 'TotalRevenue', 'AvgTransValue', 'ItemsSold']
# Create DataFrame
        DataFrame = pd.DataFrame(results, columns=columns)
# Create Bar chart
        size = plt.figure(figsize=(12, 8))
        categories = ['AvgItemPrice', 'TotalTransactions', 'TotalRevenue', 'AvgTransValue', 'ItemsSold']
        lengthCat = len(categories)
        normalize = DataFrame.copy()
        for column in categories:
            normalize[column] = normalize[column] / normalize[column].max()
        axis = size.add_subplot(111, polar=True)
        angles = [n / float(lengthCat) * 2 * pi for n in range(lengthCat)]
        angles += angles[:1]
# Plot each country
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFCE2D']
        for i, (idx, row) in enumerate(DataFrame.iterrows()):
            values = normalize[categories].iloc[i].tolist()
            values += values[:1]  # Complete the circle
            axis.plot(angles, values, 'o-', linewidth=2, label=row['Country'], color=colors[i])
            axis.fill(angles, values, alpha=0.1, color=colors[i])
# Add labels
        plt.xticks(angles[:-1], categories)
        axis.set_rlabel_position(30)
        plt.yticks([0.2, 0.4, 0.6, 0.8], ["20%", "40%", "60%", "80%"], color="black", size=8)
        plt.ylim(0, 1)
        plt.title("What's the average price per transaction in neighboring European countries?\n(Normalized Metrics)", size=15, fontweight='bold')
        plt.legend(loc='upper right', bbox_to_anchor=(1.3, 1.0))
        plt.show()
    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection:
            connection.close()
SQL_Query04 = ("""
WITH customerTransactions AS (
    SELECT 
        Country,
        `Customer ID`,
        COUNT(DISTINCT `Invoice`) as TransactionCount
    FROM purchasingandreapeatingpattens
    WHERE Country IN ('France', 'Germany', 'EIRE')
        AND `Customer ID` IS NOT NULL
    GROUP BY Country, `Customer ID`
)
SELECT 
    Country,
    COUNT(`Customer ID`) as TotalCustomers,
    SUM(TransactionCount) as TotalTransactions,
    ROUND(AVG(TransactionCount), 2) as AvgTransactionsPerCustomer,
    SUM(CASE WHEN TransactionCount = 1 THEN 1 ELSE 0 END) as OneTimeCustomers,
    SUM(CASE WHEN TransactionCount > 1 THEN 1 ELSE 0 END) as RepeatCustomers,
    ROUND(SUM(CASE WHEN TransactionCount > 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(`Customer ID`), 2) as RepeatCustomerRate
FROM customerTransactions
GROUP BY Country
ORDER BY RepeatCustomerRate DESC
               """)
def fetchQUERY04():
    connection: Connection = None
    try:
# Establish connection
        connection = mariadb.connect(**DB_CONFIG)
        cursor: Cursor = connection.cursor()
        print("--- MariaDB Connector: QUERY04 ---")
# Execute query
        cursor.execute(SQL_Query04)
# Fetch all records
        results = cursor.fetchall()
        columns = ['Country', 'TotalCustomers', 'TotalTransactions', 'AvgTransactionsPerCustomer',
           'OneTimeCustomers', 'RepeatCustomers', 'RepeatCustomerRate']
# Create DataFrame
        DataFrame = pd.DataFrame(results, columns=columns)
# Create the graph
        figure, axis = plt.subplots(figsize=(12, 8))
        # Create funnel data
        totalCustomers = DataFrame['TotalCustomers'].astype(int)
        repeatCustomers = DataFrame['RepeatCustomers'].astype(int)
        highValue = [int(repeat * 0.3) for repeat in repeatCustomers]
        yAxis = np.arange(len(DataFrame['Country']))
        width = 0.25
        axis.barh(yAxis - width, totalCustomers, width, label='Total Customers', color='blue')
        axis.barh(yAxis, repeatCustomers, width, label='Repeat Customers', color='lightgreen')
        axis.barh(yAxis + width, highValue, width, label='High-Value Customers', color='gold')
        axis.set_yticks(yAxis)
        axis.set_yticklabels(DataFrame['Country'])
        axis.set_xlabel('Number of Customers')
        axis.set_title('Between France, Germany, and Ireland, how does customer repeat \npurchase behavior differ?', fontweight='bold')
        axis.legend()
        plt.gca().invert_yaxis()
        plt.tight_layout()
        plt.show()
    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection:
            connection.close()
SQL_Query05 = ("""
SELECT Description, AVG(Price) as AvgPrice, AVG(Quantity) as AvgQuantity, SUM(Quantity) as TotalQuantity,
SUM(Price * Quantity) as TotalRevenue, COUNT(*) as TransactionCount FROM priceandquantityfrance
GROUP BY Description ORDER BY TotalRevenue DESC
""")
def fetchQUERY05():
    connection: Connection = None
    try:
# Establish connection
        connection = mariadb.connect(**DB_CONFIG)
        cursor: Cursor = connection.cursor()
        print("--- MariaDB Connector: QUERY05 ---")
# Execute query
        cursor.execute(SQL_Query05)
# Fetch all records
        results = cursor.fetchall()
        columns = ['Description', 'AvgPrice', 'AvgQuantity', 'TotalQuantity', 'TotalRevenue', 'TransactionCount']
# Create DataFrame
        DataFrame = pd.DataFrame(results, columns=columns)
# Create the graph
        bins = [1, 5, 10, 20, 50, 100, float('inf')]
        labels = ['1-5', '5-10', '10-20', '20-50', '50-100', '>100']
        DataFrame['PriceRange'] = pd.cut(DataFrame['AvgPrice'], bins=bins, labels=labels)
# Calculate stats by price range
        price_stats = DataFrame.groupby('PriceRange', observed=False).agg({
            'TotalRevenue': 'sum',
            'TransactionCount': 'sum',
            'Description': 'count'
        }).rename(columns={'Description': 'ProductCount'})
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
# Revenue by price range
        ax1.bar(price_stats.index, price_stats['TotalRevenue'], color='skyblue')
        ax1.set_title('Total Revenue by Price Range')
        ax1.set_ylabel('Total Revenue')
        ax1.tick_params(axis='x', rotation=45)
# Product count by price range
        ax2.bar(price_stats.index, price_stats['ProductCount'], color='lightcoral')
        ax2.set_title('Number of Products by Price Range')
        ax2.set_ylabel('Product Count')
        ax2.tick_params(axis='x', rotation=45)
# Add main title
        plt.suptitle('Price and Quantity Analysis for France', fontsize=16, fontweight='bold')
        plt.tight_layout()
        plt.show()
    except Error as e:
        print(f"Error: {e}")
