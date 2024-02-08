import pandas as pd 
import numpy as np 
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3


url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
initial_table_attribs = ['Name', 'MC_USD_Billion']
final_table_attribs = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
csv_output_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file ='code_log.txt'

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    current_time = datetime.now() # get current timestamp 
    timestamp = current_time.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

log_progress('Preliminaries complete. Initiating ETL process')


def extract(url, tab_attr):
    ''' This function extracts the required information from the website and saves it to a dataframe. 
    The function returns the dataframe for further processing. '''
    html_data = requests.get(url).text
    data = BeautifulSoup (html_data, 'html.parser')
    df =pd.DataFrame(columns = tab_attr)

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
  
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {"Name": col[1].contents[2],
                            "MC_USD_Billion": col[2].contents[0]}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)

    df["MC_USD_Billion"] = df["MC_USD_Billion"].str[:-1].astype(float)
    
    #print(df.dtypes)
    return df

#print(extract(url, initial_table_attribs))
extracted_df = extract(url, initial_table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to respective currencies'''
    exh_rate_df = pd.read_csv(csv_path)
    exh_rate_dict = exh_rate_df.set_index('Currency').to_dict()['Rate']

    df['MC_EUR_Billion'] = [np.round(x*exh_rate_dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_GBP_Billion'] = [np.round(x*exh_rate_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exh_rate_dict['INR'],2) for x in df['MC_USD_Billion']]
  
    return df

transformed_df = transform(extracted_df, './exchange_rate.csv')
#print(transformed_df)
log_progress('Data transformation complete. Initiating Loading process') 
#print('5th largest bank in billion EUR is ', transformed_df['MC_EUR_Billion'][4])



def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)

load_to_csv(transformed_df, csv_output_path)
log_progress('Data saved to CSV file') 


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database table with the provided name. Function returns nothing.'''
    sql_connection = sqlite3.connect(db_name)
    df.to_sql(table_name, sql_connection, if_exists='replace', index = False)
    sql_connection.close()


conn = sqlite3.connect(db_name)
log_progress('SQL Connection initiated') 
load_to_db(transformed_df, conn, table_name)
log_progress('Data loaded to Database as a table, Executing queries') 



def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print('\n', query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, conn)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, conn)

query_statement = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, conn)

log_progress('Process Complete') 
conn.close()
log_progress('Server Connection closed') 