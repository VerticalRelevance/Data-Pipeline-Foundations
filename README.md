
# Data Pipeline Foundations

This playbook provides guidance on how to build a data pipeline within AWS including ingestion, transformation, and automation strategies.



## How to Run

1. Install and setup the CDK environment. 

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

2. Create a Sample MySQL database and load sample data

You can create a sample database using a website such as https://www.freemysqlhosting.net/
Sample data: https://www.mysqltutorial.org/mysql-sample-database.aspx

3. Put Database Credential Secrets in Secrets Manager

Create a secret in secrets manager that contains the database name, username, password, port, and host. Next, specify the secret ARN in the data_ingestion.py file.

4. Deploy the Ingestion Stack

Use the command cdk deploy DataIngestion to deploy this stack.

5. Generate Sample Streaming Data

In the streaming_data.py file, enter the name of the stream just created in the DataIngestion stack and run the code to generate sample data.

6. Run DMS task


In the run_dms_task.py file, enter the task ARN created in the DataIngestion stack and run the one time migration task.

7. Deploy the DataTransformations Stack

Use the command cdk deploy DataTransformations to deploy this stack.

8. Create S3 Bucket Partitions for your glue jobs

If your glue jobs need to deliver data to a particular folder in S3, instead of manually creating the partition you can run the add_s3_bucket_partition file by specifying the bucket name and partition names.

