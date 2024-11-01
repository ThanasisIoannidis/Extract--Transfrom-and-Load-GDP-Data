# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime


# Code for ETL operations on Country-GDP data

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'

# Function for extracting information
def extract(url, table_attribs):
    page = requests.get(url).text # Extract the web page as text
    data = BeautifulSoup(page, 'html.parser') # Parse the text into an HTML object
    df = pd.DataFrame(columns = table_attribs) # Creating an empty DataFrame with columns of 'table_attribs'
    tables = data.find_all('tbody') # Searching for all tables in the url
    rows = tables[2].find_all('tr') # Retrieving all the rows from the wanted table (third table)
    for row in rows: # Iterating through all rows independently
        col = row.find_all('td') # Retrieve all elements in the current row
        if len(col)!=0: # Making sure the column is not empty
            if col[0].find('a') is not None and '—' not in col[2]: # Checking if the first column has a link inside and that there is no '—' in the third column
                data_dict = {"Country": col[0].a.contents[0], # Selecting the text from the <a> anchor (First column)
                             "GDP_USD_millions": col[2].contents[0]} # .... (Third column)
                df1 = pd.DataFrame(data_dict, index=[0]) # Creating a temporary DataFrame to add the retireved columns in the main DataFrame ('df')
                df = pd.concat([df,df1], ignore_index=True) # Appending the 'df1' to 'df'

    return df

# Function for transorming the gathered data
def transform(df):
    GDP_list = df["GDP_USD_millions"].tolist() # Creating a list with all the values of column name 'GDP_USD_millions'
    GDP_list = [float("".join(x.split(','))) for x in GDP_list] # Converting the values from string to float and removing ','
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"}) # Renaming the 'GDP_USD_millions' column to 'GDP_USD_billions'
    return df

# Converting the DataFrame to CSV
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

# Function to load the CSV to the Database
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False) # Converting the CSV to SQL

# Function for running a query in a specific sql connection
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Function to keep track the speed of the process
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("./etl_project_log.txt","a") as f:
        f.write(timestamp + ' : ' + message + '\n')



# Function Calls
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('World_Economies.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()