# modules
import os
import json
import pandas as pd
from google.cloud import bigquery
from sqlalchemy.engine import URL
from sqlalchemy import create_engine

with open('config.json') as config_file:
    dataConfig = json.load(config_file)

# parametersTarget
projectNameGCP = dataConfig['bigquery']['projectNameGCP']
datasetGCP = dataConfig['bigquery']['datasetGCP']
modelNameBI = 'SSO'

# parametersSource
P_SERVER = dataConfig['sourceDWH']['SERVER']
P_DATABASE = dataConfig['sourceDWH']['DATABASE']
P_UID = dataConfig['sourceDWH']['UID']
P_PWD = dataConfig['sourceDWH']['PWD']

# data Connection Source
connection_string = "DRIVER={SQL Server Native Client 11.0};SERVER="+P_SERVER+";DATABASE="+P_DATABASE+";UID="+P_UID+";PWD="+P_PWD+""
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
conn = create_engine(connection_url)
# data Connection target
client = bigquery.Client(project=projectNameGCP)

# get list tables from Source
sqlGetListTablesModel = """select name from sys.tables where name like '%"""+str(modelNameBI)+"%'"
df = pd.read_sql(sqlGetListTablesModel, conn)
ListTablesModel = df['name'].to_list()

# Exec in loop all Table
for target_table in ListTablesModel:
    print(f'table name {target_table}')
    # parameters Dynamic loop to sql
    table_id = f'{projectNameGCP}.{datasetGCP}.{target_table}'
    # create our SQL extract query
    sql = """SELECT * FROM [dbo]."""+str(target_table)
    print(f'sql: {sql}')

    # extract data
    df = pd.read_sql(sql, conn)

    df = df.sort_values(by=df.columns[0], ascending=False) #מיון לפי מספר עמודה

    # load data
    job_config = bigquery.LoadJobConfig(
        autodetect=True, #לקחת מבנה של הטבלה ולטעות אותה כמו שהיא
        write_disposition='WRITE_TRUNCATE'
    )

    # open file for loading

    load_job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config
        )
    load_job.result()
    # check how many records were loaded
    destination_table = client.get_table(table_id)
    print(f'You have {destination_table.num_rows} in your table {target_table}')

print(f'Complete load from sql server to bigquery')




