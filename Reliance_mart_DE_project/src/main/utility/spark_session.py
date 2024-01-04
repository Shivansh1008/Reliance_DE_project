
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.main.utility.logging_config import *


def spark_session():
    spark = SparkSession.builder.master("spark://localhost:7077") \
        .appName("DE_Project")\
        .config("spark.driver.extraClassPath", "/config/workspace/Hadoop_command/mysql-connector-j-8.2.0.jar")\
        .config("spark.jars", "/config/workspace/Hadoop_command/mysql-connector-j-8.2.0.jar") \
        .getOrCreate()
        
    logger.info("spark session %s",spark)
    return spark
