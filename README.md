Group: 07
Members:

Jomuel Santiago Miranda

Gabriel José Morales Rosario

Daniel Vega Rivera

Description:
This project is a data visualization system developed in Python, designed to connect to a MariaDB database, execute multiple SQL queries, and generate various types of charts using Matplotlib and Pandas. The system analyzes and presents business information in a clear, professional, and interactive manner. Each group member is responsible for a specific segment of the presentation, focusing on data retrieval, analysis, and visualization through SQL queries directly connected to MariaDB.

The program uses SQLAlchemy to manage database connections efficiently, Pandas to handle query results, and Matplotlib to generate visualizations. Users interact with the system through prompts that allow navigation between visualizations.

Files Used:

main.py - Main program file that coordinates database connection and graph generation

DanielGraphs.py - Contains Daniel's graph functions and SQL queries

GabrielGraphs.py - Contains Gabriel's graph functions and SQL queries

JomuelGraphs.py - Contains Jomuel's graph functions and SQL queries

Libraries Used:

pandas - For executing SQL queries and manipulating results as DataFrames

matplotlib - For generating 2D charts (bar, line, pie, scatter)

sqlalchemy - For managing database connections between Python and MariaDB

mariadb - Official driver for MariaDB server communication

numpy - For numerical operations and array handling

Program Flow:

Establishes secure connection to MariaDB database using SQLAlchemy

Verifies required tables exist with necessary fields

Executes multiple SQL queries sequentially

Generates specific graphs using Matplotlib for each query

Displays graphs to user and automatically saves them as PNG files

Confirms successful completion of all visualizations
