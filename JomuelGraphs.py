import mariadb # To import the SQL file
from mariadb import Error, Cursor, Connection
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from math import pi

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
        print(DataFrame)
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
        print(DataFrame)
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
        print(DataFrame.to_string())
# Create Bar chart
        size = plt.figure(figsize=(12, 8))
        categories = ['AvgItemPrice', 'TotalTransactions', 'TotalRevenue', 'AvgTransValue', 'ItemsSold']
        lengthCat = len(categories)
        normalize = DataFrame.copy()
        for column in categories:
            normalize[column] = normalize[column] / normalize[column].max()
        ax = size.add_subplot(111, polar=True)
        angles = [n / float(lengthCat) * 2 * pi for n in range(lengthCat)]
        angles += angles[:1]
# Plot each country
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFCE2D']
        for i, (idx, row) in enumerate(DataFrame.iterrows()):
            values = normalize[categories].iloc[i].tolist()
            values += values[:1]  # Complete the circle
            ax.plot(angles, values, 'o-', linewidth=2, label=row['Country'], color=colors[i])
            ax.fill(angles, values, alpha=0.1, color=colors[i])
# Add labels
        plt.xticks(angles[:-1], categories)
        ax.set_rlabel_position(30)
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