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
    
    # Create Spark session
    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()
    
    # Function to check validity of transactions.csv
    def transactions_check_lines(line):
        try:
            fields = line.split(',')
            # Check if there are right number of fields
            if len(fields)!=15:
                return False
            # Convert fields to integer
            int(fields[11])
            int(fields[8])
            return True
        except:
            return False
    
    # Function to check validity of scams.csv
    def scams_check_lines(line):
        try:
            fields = line.split(',')
            if len(fields)!=8:
                return False
            # Convert id to integer
            int(fields[0])
            return True
        except:
            return False
    
    # Functions to filter trasnaction scams by scam category
    def scamming(x):
        if x[1][1][1] == 'Scamming':
            return True
    
    def phishing(x):
        if x[1][1][1] == 'Phishing':
            return True
        
    def fake_ico(x):
        if x[1][1][1] == 'Fake ICO':
            return True

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
    
    # Reading transactions.csv
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    
    # Reading scams.csv
    scams = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/scams.csv")
    
    # Applying filters on transactions and scams
    transcations_filtered = transactions.filter(transactions_check_lines)
    scams_filtered = scams.filter(scams_check_lines)
    
    # Extracting address,date,value from transactions
    transaction_features = transcations_filtered.map(lambda b: (b.split(',')[6],(time.strftime("%m %y",time.gmtime(int(b.split(',')[11]))),int(b.split(',')[7]))))
    
    # Extracting address, id, category from scams
    scams_features = scams_filtered.map(lambda b: (b.split(',')[6],(int(b.split(',')[0]),str(b.split(',')[4]))))
    
    # Join transaction and scam features
    transaction_scams = transaction_features.join(scams_features) 
    
    # Top 3 lucrative form of scams with ids
    all_category = transaction_scams.map(lambda x: (x[1][1][0], x[1][0][1])) 
    lucrative_scams = all_category.reduceByKey(lambda x, y: x + y)
    top_10_scam_ids = lucrative_scams.takeOrdered(10, key=lambda x: -x[1]) 
    
    # Filter the scam 
    scams = transaction_scams.filter(scamming).map(lambda x: (x[1][0][0], x[1][0][1])).reduceByKey(lambda x, y: x + y).sortByKey(ascending=True)
    phishes = transaction_scams.filter(phishing).map(lambda x: (x[1][0][0], x[1][0][1])).reduceByKey(lambda x, y: x + y).sortByKey(ascending=True)
    f_ico = transaction_scams.filter(fake_ico).map(lambda x: (x[1][0][0], x[1][0][1])).reduceByKey(lambda x, y: x + y).sortByKey(ascending=True)
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_ids' + date_time + '/most_lucrative_scam_ids.txt')
    my_result_object.put(Body=json.dumps(top_10_scam_ids))
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_scams' + date_time + '/scamming.txt')
    my_result_object.put(Body=json.dumps(scams.take(100))) 
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_phishes' + date_time + '/phishing.txt')
    my_result_object.put(Body=json.dumps(phishes.take(100)))
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_fico' + date_time + '/fico.txt')
    my_result_object.put(Body=json.dumps(f_ico.take(100))) 
    
    spark.stop()
    
    
    
    
    
    
    