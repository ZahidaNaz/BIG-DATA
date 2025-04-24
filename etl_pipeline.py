import pandas as pd
import io
import pandas as pd
import pyodbc
import requests

def get_data_from_csv():
    csv_data = pd.read_csv(r'C:\Users\Administrator\Desktop\ETL_Pipeline_ZahidaNaz_040\data\csv_sales_data.csv', encoding='unicode_escape')
    print('reading data from CSV')
    return csv_data

# reading data from sql server
def get_data_from_sql_server():
    server = r'DESKTOP-5JN5J4V\SQLEXPRESS'  
    database = 'tmp_sales'

    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'Trusted_Connection=yes;'
    )
    conn = pyodbc.connect(conn_str)
    query = 'SELECT * FROM sales'
    sql_data = pd.read_sql(query, conn)
    conn.close()
    
    #sql_data=pd.DataFrame(sql_data)
    print('reading data from sql server')
    return sql_data

# getting user using api
def get_data_from_api():
    url = 'https://api.escuelajs.co/api/v1/users'
    response = requests.get(url, verify=False)
    
    if response.status_code == 200:
        data = response.json()
        api_data=data
        #api_data_user=pd.DataFrame(api_data)
        print('reading data from API')
        return api_data
    else:
        return response.status_code
    
    import pandas as pd

# Function to extract Excel data
def get_data_from_excel():
    file_path = r'C:\Users\Administrator\Desktop\ETL_Pipeline_ZahidaNaz_040\data\USER_DATA.xlsx'
    excel_data_user = pd.read_excel(file_path, engine='openpyxl')  # Specify engine as openpyxl
    print('Reading data from EXCEL')
    return excel_data_user

def get_all_data():
    #sp_data=get_data_from_sp()
    sql_data=get_data_from_sql_server()
    csv_data=get_data_from_csv()
    api_data_use=get_data_from_api()
    excel_data_user=get_data_from_excel()
    
    #sp_data,
    return sql_data,csv_data,api_data_use,excel_data_user


def combine_data():
    #sp_data,
    sql_data,csv_data,api_data_user,excel_data_user=get_all_data()  
    # combining user datail from different source system
    excel_data_user = pd.DataFrame(excel_data_user)
    api_data_user = pd.DataFrame(api_data_user)
    all_user = pd.concat([api_data_user, excel_data_user], ignore_index=True)
    # combining sales detail
    combine_sales_data = pd.concat([sql_data, csv_data], ignore_index=True)
    #combine_sales_data = pd.concat([combine_sales_data, sp_data], ignore_index=True)
    return all_user,combine_sales_data

all_user,combine_sales_data=combine_data()

def main_transformation_funtion():
    all_user,combine_sales_data=combine_data()
    
    all_user=pd.DataFrame(all_user)
    combine_sales_data=pd.DataFrame(combine_sales_data)

    # rename column
    combine_sales_data.rename(columns={'ORDERLINENUMBER':'id'},inplace=True)
    
    # mearging user and sales detail into one frame
    data_set = pd.merge(combine_sales_data, all_user, on='id', how='left')
    
    #replace null values
    data_set.fillna("NA", inplace=True)
    # date time standerization
    data_set['ORDERDATE'] = pd.to_datetime(data_set['ORDERDATE'], format='%Y-%m-%d', errors='coerce', utc=True)
    data_set['ORDERDATE'] = data_set['ORDERDATE'].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

    #remove duplicate records
    data_set = data_set.drop_duplicates()

    #data standerization
    data_set['ORDERNUMBER']=data_set['ORDERNUMBER'].astype('int')
    data_set['id']=data_set['id'].astype('int')
    data_set['QTR_ID']=data_set['QTR_ID'].astype('int')
    data_set['MONTH_ID']=data_set['MONTH_ID'].astype('int')
    data_set['YEAR_ID']=data_set['YEAR_ID'].astype('int')

    return data_set

def mail_etl_proc():
    data_set=main_transformation_funtion()
    data_set=pd.DataFrame(data_set)

    # data standerization and normalization 
    user_detail=data_set[['id','email',
     'password',
     'CUSTOMERNAME',
     'PHONE',
     'ADDRESSLINE1',
     'ADDRESSLINE2',
     'CITY',
     'STATE',
     'POSTALCODE',
     'COUNTRY',
     'TERRITORY',
     'CONTACTLASTNAME',
     'CONTACTFIRSTNAME',
     'role',
     'avatar']]
    sale_detail=data_set[[
     'ORDERNUMBER',
     'QUANTITYORDERED',
     'PRICEEACH',
     'SALES',
     'ORDERDATE',
     'STATUS',
     'QTR_ID',
     'MONTH_ID',
     'YEAR_ID',
     'PRODUCTLINE',
     'MSRP',
     'PRODUCTCODE']]
    bridge_tbl_user_sales=data_set[['id','ORDERNUMBER','creationAt',
     'updatedAt']]
    
    server = 'DESKTOP-5JN5J4V\SQLEXPRESS'  
    database = 'tmp_sales'


    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'Trusted_Connection=yes;'
    )
    

    conn = pyodbc.connect(conn_str)
    
    # for user detail
    cursor = conn.cursor()
    users = user_detail.values.tolist()
    #print(users[0])
    cursor.executemany('''
        INSERT INTO tbl_user_detail (id,
     email,
     password,
     CUSTOMERNAME,
     PHONE,
     ADDRESSLINE1,
     ADDRESSLINE2,
     CITY,
     STATE,
     POSTALCODE,
     COUNTRY,
     TERRITORY,
     CONTACTLASTNAME,
     CONTACTFIRSTNAME,
     role,
     avatar)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', users)
    conn.commit()

    # insert sales detail
    sales = sale_detail.values.tolist()
    #print(users[0])
    cursor.executemany('''
        INSERT INTO tbl_sale (ORDERNUMBER,
    QUANTITYORDERED,
    PRICEEACH,
    SALES,
    ORDERDATE,
    STATUS,
    QTR_ID,
    MONTH_ID,
    YEAR_ID,
    PRODUCTLINE,
    MSRP,
    PRODUCTCODE)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', sales)
    conn.commit()

    # insert into brdige tbl

    #bridge = bridge_tbl_user_sales.values.tolist()
    #bridge=pd.DataFrame(bridge)
    print(bridge_tbl_user_sales)
    for i,row in bridge_tbl_user_sales.iterrows():
        cursor.execute('''
            INSERT INTO bridge_tbl_user_sales (id,ORDERNUMBER,creationAt,updatedAt)
            VALUES (?, ?, ?, ?)
        ''', row['id'],row['ORDERNUMBER'],row['creationAt'],row['updatedAt'])
    conn.commit()
    conn.close()
    result='Records Inserted Successfully'
    return result

#calling funtion
a=mail_etl_proc()
print(a)
#data_set=main_transformation_funtion()
#data_set=pd.DataFrame(data_set)
#print(data_set)

import time
import schedule
from helper import mail_etl_proc  

def run_job():
    a = mail_etl_proc()
    print(a)
    print('job is running')

schedule.every(1).days.do(run_job)
while True:
    schedule.run_pending()
    time.sleep(86400)  # sleep for 1 day
