import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":
    
    # Create spark session
    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()
     
    # Function to check if transcations file is valid
    def transaction_check_data(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            # Convert timestamp to int
            int(fields[11]) 
            # COnvert gas price to int
            int(fields[9]) 
            return True
        except:
            return False

    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")  
    
    # Load transaction data
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    # Filter invalid transactions
    trans_clean_data = transactions.filter(transaction_check_data) #clean transactions text file
    
    # Extract date and gas price
    trans_val = trans_clean_data.map(lambda b: (time.strftime("%m %Y",time.gmtime(int(b.split(',')[11]))),int(b.split(',')[9])))
    # Count the number of transaction records for each date
    trans_val_records = spark.sparkContext.broadcast(trans_val.countByKey())
    
    # Calculate average gas price for each month-year
    trans_val = trans_val.reduceByKey(lambda x, y: x + y)
    trans_val = trans_val.map(lambda x: (x[0], x[1]/trans_val_records.value[x[0]]))
    trans_val = trans_val.collect()
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_gg' + date_time + '/gas_guzzlers.txt')
    my_result_object.put(Body=json.dumps(trans_val[:100]))
    
    
    spark.stop()
    
    
    
    
    
    
    