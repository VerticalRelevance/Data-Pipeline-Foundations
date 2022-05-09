import boto3

s3 = boto3.client('s3')

bucket_name = "pipelinecuratedzone"

partition_list = ["transactions","customer_data_USA"]

for x in partition_list:
    s3.put_object(Bucket= bucket_name, 
                  Key=(x + '/')
    )
