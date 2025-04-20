import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import numpy as np

#Customer dataset
path="C:\\Users\\Hiii T470 Hello\\Pictures\\Customers.csv"
df_customer=pd.read_csv(path,encoding='latin-1')
df_customer['Birthday']=pd.to_datetime(df_customer['Birthday'],format="%m/%d/%Y")
df_customer.drop(['State Code','Zip Code'],axis=1,inplace=True)

#Excahnge dataset
path="C:\\Users\\Hiii T470 Hello\\Pictures\\Exchange_Rates.csv"
df_Exchange=pd.read_csv(path,encoding='latin-1')
df_Exchange.rename(columns={'Currency Code':'Currency'},inplace=True)
df_Exchange['Date']=pd.to_datetime(df_Exchange['Date'],format="%m/%d/%Y")

#Product dataset
path="C:\\Users\\Hiii T470 Hello\\Pictures\\Products.csv"
df_products=pd.read_csv(path,encoding='latin-1')
df_products['Unit Cost USD']=df_products['Unit Cost USD'].astype(str)
df_products['Unit Cost USD']=df_products['Unit Cost USD'].str.replace("$","").str.replace(",","")
df_products['Unit Cost USD']=df_products['Unit Cost USD'].astype(float)

df_products['Unit Price USD']=df_products['Unit Price USD'].astype(str)
df_products['Unit Price USD']=df_products['Unit Price USD'].str.replace("$","").str.replace(",","")
df_products['Unit Price USD']=df_products['Unit Price USD'].astype(float)

df_products.drop(['SubcategoryKey'],axis=1,inplace=True)

df_products.columns = df_products.columns.str.strip().str.replace(" ", "_")

#Sales dataset
path="C:\\Users\\Hiii T470 Hello\\Pictures\\Sales.csv"
df_sales=pd.read_csv(path,encoding='latin-1')
df_sales.columns=df_sales.columns.str.strip().str.replace(" ","_")
df_sales["Order_Date"]=pd.to_datetime(df_sales["Order_Date"])
df_sales["Delivery_Date"]=pd.to_datetime(df_sales["Delivery_Date"])
df_sales["Delivery_Date"]=df_sales["Delivery_Date"].fillna(0)

#Stores table
path="C:\\Users\\Hiii T470 Hello\\Pictures\\Stores.csv"
df_stores=pd.read_csv(path,encoding='latin-1')
df_stores['Open Date']=pd.to_datetime(df_stores['Open Date'])
df_stores["Square Meters"]=df_stores["Square Meters"].fillna(0)
df_stores.columns = df_stores.columns.str.strip().str.replace(" ", "_")


#Create Tables in postresql
mydb=psycopg2.connect(
    host="localhost",
    user="postgres",
    password="yoga0129",
    dbname="Dataspark",
    port="5432"
)
cursor=mydb.cursor()
try:
    create_table1='''create table if not exists customer_details(CustomerKey int,
                                                                 Gender VARCHAR(225),
                                                                 Name VARCHAR(225),
                                                                 City VARCHAR(225),
                                                                 State VARCHAR(225),
                                                                 Country VARCHAR(225),
                                                                 Continent VARCHAR(225),
                                                                 Birthday VARCHAR(225))'''
    cursor.execute(create_table1)
    mydb.commit()
except:
    print("customer details created successfully")

try:
    create_table2='''create table if not exists exchange_rates(Date DATE,
                                                               Currency VARCHAR(10),
                                                               Exchange FLOAT
                                                                )'''
    cursor.execute(create_table2)
    mydb.commit()
except:
    print("Exchange_rate table created successfully")

try:
    create_table3='''create table if not exists products(ProductKey INT,
                                                         Product_Name VARCHAR(255) ,
                                                         Brand VARCHAR(30),
                                                         Color VARCHAR(30),
                                                         Unit_Cost_USD FLOAT,
                                                         Unit_Price_USD FLOAT,
                                                         Subcategory VARCHAR(255),
                                                         CategoryKey INT,
                                                         Category VARCHAR(255))'''
    cursor.execute(create_table3)
    mydb.commit()
except:
    print("product table created successfully")

try:
    create_table4='''create table if not exists sales(Order_Number INT,
                                                      Line_Item INT,
                                                      Order_Date VARCHAR(50),
                                                      Delivery_Date VARCHAR(50),
                                                      CustomerKey INT,
                                                      StoreKey INT,
                                                      ProductKey INT,
                                                      Quantity INT,
                                                      Currency_Code VARCHAR(50))'''
    cursor.execute(create_table4)
    mydb.commit()
except:
    print("sales table created successfully")

try:
    create_table5='''create table if not exists stores(StoreKey INT,
                                                       Country VARCHAR(255),
                                                       State VARCHAR(255),
                                                       Square_Meters FLOAT,
                                                       Open_Date DATE)'''
    cursor.execute(create_table5)
    mydb.commit()
except:
    print("store table created successsfully")



#Insert the data in the tables
mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="yoga0129",
        dbname="Dataspark",
        port="5432"
    )
cursor = mydb.cursor()
df1 = df_customer
df2=df_Exchange
df3=df_products
df4=df_sales
df5=df_stores
for index,row in df1.iterrows():
    insert_customer='''INSERT INTO customer_details(CustomerKey,
                                                 Gender,
                                                 Name,
                                                 City,
                                                 State,
                                                 Country,
                                                 Continent,
                                                 Birthday)VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'''
    values1=(
        row["CustomerKey"],
        row["Gender"],
        row["Name"],
        row["City"],
        row["State"],
        row["Country"],
        row["Continent"],
        row["Birthday"]
    )
    try:
        cursor.execute(insert_customer,values1)
        mydb.commit()
    except Exception as e:
        print(f"Error: {e}")

for index,row in df2.iterrows():
    insert_exchange='''INSERT INTO exchange_rates(Date,
                                                  Currency,
                                                  Exchange)VALUES(%s, %s, %s)'''
    values2=(
        row["Date"],
        row["Currency"],
        row["Exchange"]
    )
    try:
        cursor.execute(insert_exchange,values2)
        mydb.commit()
    except Exception as e:
        print(f"Error:{e}")

for index,row in df3.iterrows():
    insert_product='''INSERT INTO products(ProductKey,
                                           Product_Name,
                                           Brand,
                                           Color,
                                           Unit_Cost_USD,
                                           Unit_Price_USD,
                                           Subcategory,
                                           CategoryKey,
                                           Category)VALUES(%s, %s, %s, %s, %s, %s, %s, %s,%s)'''
    values3=(
        row["ProductKey"],
        row["Product_Name"],
        row["Brand"],
        row["Color"],
        row["Unit_Cost_USD"],
        row["Unit_Price_USD"],
        row["Subcategory"],
        row["CategoryKey"],
        row["Category"])
    try:
        cursor.execute(insert_product,values3)
        mydb.commit()
    except Exception as e:
        print(f"Error:{e}")

for index,row in df4.iterrows():
    insert_sales='''INSERT INTO sales(Order_Number,
                                      Line_Item,
                                      Order_Date,
                                      Delivery_Date,
                                      CustomerKey,
                                      StoreKey,
                                      ProductKey,
                                      Quantity,
                                      Currency_Code)VALUES(%s, %s, %s, %s, %s, %s, %s, %s,%s)'''
    values4=(row["Order_Number"],
             row["Line_Item"],
             row["Order_Date"],
             row["Delivery_Date"],
             row["CustomerKey"],
             row["StoreKey"],
             row["ProductKey"],
             row["Quantity"],
             row["Currency_Code"])
    try:
        cursor.execute(insert_sales,values4)
        mydb.commit()
    except Exception as e:
        print(f"Error:{e}")

for index,row in df5.iterrows():
    insert_stores='''INSERT INTO stores(StoreKey,
                                        Country,
                                        State,
                                        Square_Meters,
                                        Open_Date)VALUES(%s, %s, %s, %s, %s)'''
    values5=(row["StoreKey"],
             row["Country"],
             row["State"],
             row["Square_Meters"],
             row["Open_Date"])
    try:
        cursor.execute(insert_stores,values5)
        mydb.commit()
    except Exception as e:
        print(f"Error:{e}")
        


    

    

