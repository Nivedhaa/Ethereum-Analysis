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
            # COnvert gas used to int
            int(fields[8])
            return True
        except:
            return False
       
    def contact_check_data(line):
        try:
            fields = line.split(',')
            if len(fields)!=6:
                return False
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
    transactions  = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    # Load contracts data
    contracts = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    
    # Filter invalid transactions and contracts
    clean_transactions = transactions.filter(transaction_check_data)
    clean_contracts = contracts.filter(contact_check_data)
   
    # Extract date and gas used from transactions
    transaction_features = clean_transactions.map(lambda b: (b.split(',')[6],(time.strftime("%m %y",time.gmtime(int(b.split(',')[11]))),int(b.split(',')[8])))) 
    # Extract address and byte from contracts
    contract_features = clean_contracts.map(lambda x: (x.split(",")[0], x.split(",")[1])) 
   
    # Join transaction and contracts
    joined = transaction_features.join(contract_features) 
   
    # Extract monthly gas used average
    monthly_average = joined.map(lambda x: (x[1][0][0], x[1][0][1]) ) 
    months = spark.sparkContext.broadcast(monthly_average.countByKey()) 
    monthly_average = monthly_average.reduceByKey(operator.add) 
    monthly_average = monthly_average.map(lambda x: (x[0], x[1]/months.value[x[0]]))  
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_avg_gas_used' + date_time + '/avg_gas_used.txt')
    my_result_object.put(Body=json.dumps(monthly_average.take(100)))
    
    
    spark.stop()
    
    
    
    
    
    
    