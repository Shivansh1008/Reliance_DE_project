import sys
import os
import shutil
import datetime
# Ensure the project root is in the sys.path
sys.path.append('/config/workspace/Reliance_DE_proj')

# Import statements for the necessary modules
from resources.dev import config
from src.main.utility.s3_client_connection import *
from src.main.utility.logging_config import *
from src.main.utility.my_sql_session import *
from src.main.read.aws_read_my import *
from src.main.download.aws_file_download import *
from src.main.utility.spark_session import *
from src.main.move.aws_move_file import *
from src.main.transformations.jobs.dimension_tables_join import *
from src.main.write.dataframe_writer import *
from src.main.upload.upload_to_s3 import *
from src.main.read.database_read import *
from src.main.transformations.jobs.customer_mart_sql_tranform_write import *
from src.main.transformations.jobs.sales_mart_sql_transform_write import *
from src.main.delete.local_file_delete import *




# Get AWS credentials from the config module
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key


# Create S3 client
S3Client = S3ClientProvider(aws_access_key,aws_secret_key)
s3_client = S3Client.get_client()
bucket_name=config.bucket_name
folder_path=config.s3_source_directory
local_directory=config.local_directory
db_name=config.database_name


# List buckets
bucket_list=[]
for bucket in s3_client.buckets.all():
    bucket_list.append(bucket.name)
logger.info("List of Buckets: %s", bucket_list)

#Check if local directory already has a file
#if file is there check if the same file present in the satging area
#with status as A. If so then don't delete and try to re-run
#Else give an error and not process the next file

csv_file=[file for file in os.listdir(local_directory)if file.endswith('.csv')]

#Create MySQL cnnection 
connection=get_mysql_connection()
cursor=connection.cursor()
if csv_file:
    statement= f"""SELECT DISTINCT FILE_NAME FROM {db_name}.{config.product_staging_table} 
                    WHERE status= 'A' 
              AND file_name in ({str(csv_file)[1:-1]})"""

    logger.info(f"Dynamically sql statment created:%s", statement)

    cursor.execute(statement)
    data=cursor.fetchall()

    if data:
        logger.info("Your last run was failed plese check")
else:
    logger.info("Your last run was successful!!")
try:
    s3_reader=S3Reader()
    s3_absolute_file_path=s3_reader.list_files(s3_client,bucket_name,folder_path=folder_path)

    logger.info("Absolute path on S3 bucket for csv file %s ",s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception("No data available to process")

except Exception as e:
    logger.error ("Existed with error: %s",e)
    raise e

prefix=f"s3://{bucket_name}/"
file_path=[url[len(prefix):] for url in s3_absolute_file_path]
logging.info(f"File path available on S3 under %s bucket and folder name is %s ",bucket_name ,file_path)

try:
    downloader=S3FileDownloader(s3_client,bucket_name,local_directory)
    downloader.download_files(file_path)
except Exception as e:
    logger.error("File download erorr:%s" , e)
    sys.exit()

#Get a list of all files in the local directory
all_files=os.listdir(local_directory)
logger.info(f"List of files present at my local directory after download {all_files}")

#Filter the files with.csv in their name
if all_files:
    csv_files=[]
    error_files=[]
    for files in all_files:
        if files.endswith('.csv'):
            csv_files.append(os.path.join(local_directory,files ))
        else:
            error_files.append(os.path.join(local_directory, files))
    if not csv_files:
        logger.error("No CSV data avaialable to process to request")
        raise Exception("No CSV data avaialable to process to request")
else:
    logger.error("There is no data to Process")
    raise Exception("There is no data to Process") 

logger.info('***********************Listing the file****************************************')
logger.info("List of csv files thats need to be processes %s" , csv_files)   

logger.info('***********************Creating Spark Session****************************************')

spark=spark_session()

logger.info("***********************Spark Session Created****************************************")

#check the requird column in the scheam of csv file
#if not required column keep it in a list or error files
#else union all teh data into one dataframe

logger.info("********************checking scehama for data loaded in s3*************************")

correct_files=[]
for csv in csv_files:
    file_name=os.path.basename(csv)
    data_schema=spark.read.option("header",True).option("inferSchema",True).csv(f"/input_data/{file_name}", sep=",").columns
    logger.info(f"schema for the {csv} is {data_schema}")
    logger.info(f"Mandatory column schema is {config.Mandatory_columns}")
    missing_column= set(config.Mandatory_columns)- set(data_schema)
    logger.info(f"missing columns are {missing_column}")

    if missing_column:
        error_files.append(csv)
    else:
        logger.info("No missing column for the {csv}")
        correct_files.append(csv)

logger.info(f"***********************List of correct files ****************************{correct_files}")
logger.info(f"************************List of error files******************************{error_files}")
logger.info(f"********************Moving Error files to error directory if any**************************")

#Move Error files to error directory on local
error_folder_path=config.error_folder_path_local
if error_files:
    for file_path in error_files:
        file_name=os.path.basename(file_path)
        if os.path.exists(file_path):
            shutil.move(file_path, error_folder_path)
            logger.info(f"Moved '{file_name}' from {file_path}")

            source_prefix=config.s3_source_directory
            destination_prefix=config.s3_error_directory

            message=move_s3_to_s3(s3_client, bucket_name, source_prefix, destination_prefix,file_name)
            logger.info(f"{message}")

        else:
            logger.error(f"'{file_path}' does not exist")       
else:
    logger.info("**************************There is no error files available at our dataset**********************")

#Additional columns need to be taken care of 
#Determine extra columns

#Before running the process 
#Stage table needs to be updated with tattus as Active(A) and inactive(I)

logger.info("***************Updating the product_staging_table that we have started the Process************")
insert_statement=[]
current_date=datetime.datetime.now() #sysdate
formatted_date=current_date.strftime("%Y-%m-%d %H:%M:%S") #formated date
if correct_files:
    for file in correct_files:
        filename=os.path.basename(file)
        statements=f""" INSERT INTO {db_name}.{config.product_staging_table} (file_name,file_location,created_date,status)
                        values('{filename}','{file}','{formatted_date}','A')"""

        insert_statement.append(statements)
    logger.info(f"Insert statement created for staging table ---{insert_statement}")
    logger.info("****************Connecting with My SQL server ************************************")
    connection =get_mysql_connection()
    cursor=connection.cursor()
    logger.info("*******************MY SQL server connected successfully**********************************")
    for statement in insert_statement:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("**********************There is no files to process ***********************************")
    raise Exception("*******************No Data available with correct files******************************")


logger.info("************************Staging table  updated successfully************************************")

logger.info("*************************Fixing extra columns coming from source***********************************")



schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True)
])

logger.info("********************** creating empty dataframe ****************************************")
final_df_to_process=spark.createDataFrame([],schema=schema)

for data in correct_files:
    data=os.path.basename(data)
    data_df = spark.read.option("header",True).option("inferSchema",True).csv(f"/input_data/{data}", sep=",")
    data_schema=data_df.columns
    extra_columns=list(set(data_schema)  - set(config.Mandatory_columns))
    logger.info("Extra column present at source is {extra_columns}")
    if extra_columns:
        data_df=data_df.withColumn("additional_column" ,concat_ws("," ,*extra_columns))\
            .select("customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost","additional_column")

        logger.info(f"processed {data} and addedd in 'additional_column'")
    else:
        data_df=data_df.withColumn("additional_column" ,lit(None))\
        .select("customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost","additional_column")

    final_df_to_process=final_df_to_process.union(data_df)
#final_df_to_process =data_df
logger.info("**************Final Dataframe from source which will be going to process**************************")
final_df_to_process.show()

#Enrich the data fron all dimension table 
#also create a datamart for sales_team and their incentive,address and all
#another datamart for customer who bought how much each days of month 
#for every month there should be a file and inside that
#there should be a store_is segregation
#Read the data from parquet and generate a csv file
#in which there will be a sales_person_name,sales_person_store_id
#sales_person_total_billing_done_for_each_month,total_incentive

#connecting with Databasereader
database_client=DatabaseReader(config.url,config.properties)

#creating df for all tables

#customer_table
logger.info("*****************************Loading customer table into customer_table_df******************")
customer_table_df = database_client.create_dataframe(spark,config.customer_table)
customer_table_df.show()

#Product_table
logger.info("*****************************Loading product table into product_table_df******************")
product_table_df = database_client.create_dataframe(spark,config.product_table)
product_table_df.show()
#product_staging_table
logger.info("*****************************Loading product_staging_table table into product_staging_table_df******************")
product_staging_table_df = database_client.create_dataframe(spark,config.product_staging_table)

#sales_team_table
logger.info("*****************************Loading custsales_teamomer table into sales_team_table_df******************")
sales_team_table_df = database_client.create_dataframe(spark,config.sales_team_table)

#store_table
logger.info("*****************************Loading store table into store_table_df******************")
store_table_df = database_client.create_dataframe(spark,config.store_table)

s3_customer_store_sales_df_join= dimension_table_join(final_df_to_process,customer_table_df,store_table_df,sales_team_table_df)


#Final enriched data

logger.info("*********************************Final Enriched data********************************")
s3_customer_store_sales_df_join.show()

#write the customer data into customer data mart in parquet format
#file will be written to local first
#move the RAW data to s3 bucket for reporting tool
#Write reporting data into MySQL table also

logger.info("****************************Write the data into Customer Data Mart********************************")
final_customer_data_mart_df=s3_customer_store_sales_df_join\
                        .select("ct.customer_id","ct.first_name","ct.last_name","ct.address","ct.pincode",
                        "phone_number","sales_date","total_cost")

logger.info("****************************Final Data for customer Data Mart********************************")
final_customer_data_mart_df.show()

#parquet_writer=dataframe_writer("overwrite","parquet")
#parquet_writer.df_writer(final_customer_data_mart_df,config.customer_data_mart_local_file)

logger.info(f"****************customer data written to local disk at {config.customer_data_mart_local_file}********************************")

#Move  data on s3 bucket for customer_data_mart
logger.info("****************Data movement from local to s3 for customer data mart*****************************")

s3_uploader=UploadToS3(s3_client)
s3_directory=config.s3_customer_datamart_directory
message=s3_uploader.upload_to_s3(s3_directory,bucket_name,config.customer_data_mart_local_file)
logger.info(f"{message}")

#Sales team Data Mart
logger.info("****************************Write the data into sales team Data Mart********************************")

final_sales_team_data_mart_df=s3_customer_store_sales_df_join\
                        .select("store_id","sales_person_id","sales_person_first_name","sales_person_last_name",
                        "manager_id","is_manager","sales_person_address",
                        "sales_person_pincode","sales_date","total_cost",
                        expr("SUBSTRING(sales_date,1,7)as sales_month")
                        )

logger.info("****************************Final Data for sales team Data Mart********************************")
final_sales_team_data_mart_df.show()

parquet_writer=dataframe_writer("overwrite","parquet")
parquet_writer.df_writer(final_sales_team_data_mart_df,config.sales_team_data_mart_local_file)

logger.info(f"****************sales team data written to local disk at {config.sales_team_data_mart_local_file}********************************")

#Move  data on s3 bucket for sales_team_data_mart
logger.info("****************Data movement from local to s3 for sales team data mart*****************************")

s3_uploader=UploadToS3(s3_client)
s3_directory=config.s3_sales_datamart_directory
message=s3_uploader.upload_to_s3(s3_directory,bucket_name,config.sales_team_data_mart_local_file)
logger.info(f"{message}")


#Also writing the data into partition 
final_sales_team_data_mart_df.write.format("parquet").option("header","true").mode("overwrite")\
                    .partitionBy("sales_month","store_id")\
                    .option("path",config.sales_team_data_mart_partitioned_local_file).save()


#Move data on S3 for partitioned folder
s3_prefix="sales_partitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp()) * 1000
for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path = os.path.join(root, file)
        relative_file_path=os.pathrelpath(local_file_path,config.sales_team_data_mart_partitioned_local_file)
        s3_key=f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        message=s3_client.Bucket(bucket_name).upload_file(local_file_path, s3_key)

logger.info(f"{message}")

#calculation for customer mart
#find out the customer total purchase every month
#write the data in MySQL table

logger.info("************Calculating customer every purchased amount ********************")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("********Calculation of customer mart done and written into the table*********************")


#calculation for sales team mart
#find out the total sales done by each sales person every month
#Give the top performer 1% incentive of total sales of the month 
#Rest sales person will get nothing
#write the data into MySQL table

logger.info("************Calculating sales every month billed amount ********************")
sales_mart_calculation_table_write(final_sales_team_data_mart_df)
logger.info("********Calculation of sales mart done and written into the table*********************")





######################################### Last Step #######################################
#Move the file on S3 into processed folder and delete the local file

s3_source_directory = source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
message= move_s3_to_s3(s3_client,bucket_name,s3_source_directory,destination_prefix)
logger.info(f"{message}")


logger.info("************************Deleting sales data from local ***************************************")
delete_local_file(config.local_directory)
logger.info("************************Deleting sales data from local ***************************************")


logger.info("************************Deleting customer data from local ***************************************")
delete_local_file(config.customer_data_mart_local_file)
logger.info("************************Deleting customer data from local ***************************************")


logger.info("************************Deleting sales data from local ***************************************")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("************************Deleting sales data from local ***************************************")


logger.info("************************Deleting sales data from local ***************************************")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("************************Deleting sales data from local ***************************************")



#Update the status of staging table
update_statements= []
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements= f"update {db_name}.{config.product_staging_table} " \
                    f" SET status = 'I' ,updated_date= '{formatted_date}'"\
                    f" WHERE file_name = '{filename}'"

        update_statements.append(statements)
    logger.info(f"Updated statements created for staging table --- {update_statements}")
    logger.info("*************************** Connecting with My SQL server **************************************")
    connection=get_mysql_connection()
    cursor=connection.cursor()
    logger.info("***************************MySQL server connected successfully ********************************")
    for statement in update_statements:
        cursor.execute(statements)
        connection.commit()
    cursor.close()
    connection.commit()
else:
    logger.error("*************There is some error in process in betweeen **************************************")
    sys.exit()

input("Press enter to terminate ")


