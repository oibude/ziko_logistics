# Import Libraries
import pandas as pd
import io
import os
from azure.storage.blob import BlobServiceClient, BlobClient
from dotenv import load_dotenvgit

# Extract file
ziko_df= pd.read_csv(r'C:\Users\HP SPECTRE\Documents\Data Journey\10Alytics\ETL_Orchestration\ziko_logistics_project\ziko_logistics\ziko_logistics_data.csv')

#Correcting the date format
ziko_df['Date']=ziko_df['Date'].astype('datetime64[ns]')

#Data Cleaning and Transformation
ziko_df.fillna({
                        'Unit_Price': ziko_df['Unit_Price'].mean(),
                        'Total_Cost': ziko_df['Total_Cost'].mean(),
                        'Discount_Rate': 0.0,
                        'Return_Reason': 'unknown'}, inplace=True)

#Customer Table
customer= ziko_df[['Customer_ID','Customer_Name', 'Customer_Phone', 'Customer_Email','Customer_Address']].copy().drop_duplicates().reset_index(drop=True)

#Product Table
product= ziko_df[['Product_ID', 'Product_List_Title', 'Quantity', 'Unit_Price', 'Discount_Rate']].copy().drop_duplicates().reset_index(drop=True)

#Transaction Fact table
transaction_fact= ziko_df.merge(customer, on= ['Customer_ID','Customer_Name', 'Customer_Phone', 'Customer_Email','Customer_Address'], how= 'left')\
                        .merge(product, on= ['Product_ID', 'Product_List_Title', 'Quantity', 'Unit_Price', 'Discount_Rate'], how='left')\
                        [['Transaction_ID', 'Customer_ID', 'Product_ID', 'Sales_Channel','Order_Priority', 'Warehouse_Code', 'Ship_Mode', 'Delivery_Status',
                          'Customer_Satisfaction', 'Item_Returned', 'Return_Reason','Payment_Type', 'Taxable', 'Region', 'Country',]]

#Load into csv files
customer.to_csv(r'datasets\customer.csv', index=False)
product.to_csv(r'datasets\product.csv')
transaction_fact.to_csv(r'datasets\transaction_fact.csv', index=False)

print('Files have been loaded into local machine')
#Data Loading and Azure Connection

#Data load
#Azure connection
load_dotenv()

connect_str= os.getenv('CONNECT_STR')
blob_service_client= BlobServiceClient.from_connection_string(connect_str)

container_name=os.getenv('CONTAINER_NAME')
container_client= blob_service_client.get_container_client(container_name)

# Create afunction that would load the data into azure blob storage as a parquet file
def upload_df_to_blob_as_parquet(df, container_client, blob_name):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_client= container_client.get_blob_client(blob_name)
    blob_client.upload_blob(buffer, blob_type= "BlockBlob", overwrite=True)
    print(f'{blob_name} uploaded to Blob storage sucessfully')

upload_df_to_blob_as_parquet(customer, container_client, 'rawdata/customer.parquet')
upload_df_to_blob_as_parquet(product, container_client, 'rawdata/product.parquet')
upload_df_to_blob_as_parquet(transaction_fact, container_client, 'rawdata/transaction_fact.parquet')