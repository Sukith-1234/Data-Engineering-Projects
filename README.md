Description of Objects

Solution Architect.pdf -> The diagrams /architecture/technologies use for this project shows in this file.

ConverJSONtoCSV.ipynb  -> this python script use for convert JSON files into CSV format and combined converted CSV files into a single CSV file (User_Finale).Also drop the duplicate user data to keep only unique user data.

User_Final.CSV -> It is the final cleansed user data file which used to connect with SQL Databse.

SQL-Script-Schema Only.sql -> Schema of  Database created for this problem (User table) in MSSQL.

script with data.sql -> Include the schema and user data after push data by Azure Data Factory.

User_Analysis_Final.ipynb -> notebook file include the process and codes which use for the analytics

User_Analysis_Databricks.html -> Databricks solution 


Workflow

1) Download all the JSON files from link https://random-data-api.com/documentation
2) Upload all the downloaded JSON files into google colab (python enviorenemnt)
3) Using ConverJSONtoCSV.ipynb code convert and combie into one unique CSV file (with removing duplicate rows) (User_Final.CSV)
4) Create Table schema in SQL Database using  SQL-Script-Schema Only.sql
5) Upload User_Final.CSV file into Azure Blob Storage (as acontainer service)
6) Using Azure Data Factory push data from Azure Blob storage to Azure SQL Database (you can use SSIS to push csv data into your on prem SQL server)
7) script with data.sql shows the all the inject data of Azure SQL DB
8) Using Databricks connect Azure SQL with Python (Databricks Python Enviorenment) -Inside python file (User-Analysis-Python-Project-Databricks.html) connection string Insert your sever name,database name,user name and password to make connection with Azure SQL DB
9) Execute codes to run the analysis



User_Analysis_Final.ipynb-Include procedure of direct conncetion (SQL DB) using pyodbc library

