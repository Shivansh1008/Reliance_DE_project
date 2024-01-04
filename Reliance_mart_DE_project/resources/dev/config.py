import os

key = "youtube_project"
iv = "youtube_encyptyo"
salt = "youtube_AesEncryption"

#AWS Access And Secret key
region_name='Asia Pacific (Mumbai) ap-south-1'
aws_access_key = ""
aws_secret_key = ""
bucket_name = "reliance-mart-de-project"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"
#sales_partitioned_data_mart

#Database credential
# MySQL database connection properties
database_name = "DE_PROJECT"
url= f"jdbc:mysql://reliance-de-project.cepzfvolpzow.ap-south-1.rds.amazonaws.com:3306/{database_name}"

properties = {
    "user": "admin",
    "password": "123456789",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Table name
customer_table = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
Mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]

# File Download location
local_directory = "/config/workspace/spark_project/file_from_s3"
customer_data_mart_local_file = "/config/workspace/spark_project/customer_data_mart"
sales_team_data_mart_local_file = "/config/workspace/spark_project/sales_team_data_mart"
sales_team_data_mart_partitioned_local_file = "/config/workspace/spark_project/sales_partition_data"
error_folder_path_local = "/config/workspace/spark_project/error_files"
