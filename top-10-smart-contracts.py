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
    
    # Create Spark Session
    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()
    
    # Check if contracts is valid
    def contracts_check_lines(line):
        try:
            fields = line.split(',')
            # Check of the number of fields is 6
            if len(fields)!=6:
                return False
            else:
                return True
        except:
            return False
    
    # Check if transactions is valid
    def transactions_check_lines(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            int(fields[3])
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
    
    # Load transactions and contracts files
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    contracts = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    
    # Filter out invalid lines from transactions and contracts
    transactions_clean = transactions.filter(transactions_check_lines)
    contracts_clean = contracts.filter(contracts_check_lines)
    
    # Extract address and value from each transaction
    transaction_features = transactions_clean.map(lambda x: (x.split(',')[6], int(x.split(',')[7])))
    # Add one to each contract address
    contracts_features = contracts_clean.map(lambda x: (x.split(',')[0],1))
    # Combine transaction and contract
    transaction_contract = transaction_features.union(cf)
    # Reduce by address for total value for each address 
    address_value = transaction_contract.reduceByKey(lambda x, y: x + y)
    # Get top 10 address in descending order
    top10_sc = address_value.takeOrdered(10, key=lambda l: -1*l[1])
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_top_10' + date_time + '/transactions_top_10.txt')
    my_result_object.put(Body=json.dumps(top10_sc))
    
    
    spark.stop()
    
    
    
    
    
    
    