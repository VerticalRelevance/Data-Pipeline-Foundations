import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": True,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://pipelinerawzone/BatchData/"], "recurse": True},
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1651637502707 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://pipelinerawzone/StreamingData/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1651637502707",
)

# Script generated for node Apply Mapping
ApplyMapping_node1651640088766 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("customernumber", "string", "customer_id_", "int"),
        ("customername", "string", "customer_name", "string"),
        ("contactlastname", "string", "customer_last_name", "string"),
        ("contactfirstname", "string", "customer_first_name", "string"),
        ("city", "string", "city", "string"),
        ("state", "string", "state", "string"),
        ("postalcode", "string", "postal_code", "string"),
        ("country", "string", "country", "string"),
        ("creditlimit", "string", "creditlimit", "string"),
    ],
    transformation_ctx="ApplyMapping_node1651640088766",
)

# Script generated for node Join
Join_node1651640425025 = Join.apply(
    frame1=ApplyMapping_node1651640088766,
    frame2=AmazonS3_node1651637502707,
    keys1=["customer_id_"],
    keys2=["customer_id"],
    transformation_ctx="Join_node1651640425025",
)

# Script generated for node SQL
SqlQuery0 = """
select * from myDataSource
where country is not null
"""
SQL_node1651784747557 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": Join_node1651640425025},
    transformation_ctx="SQL_node1651784747557",
)

# Script generated for node Amazon S3
AmazonS3_node1651640772927 = glueContext.write_dynamic_frame.from_options(
    frame=SQL_node1651784747557,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://pipelinetransformedzone/customer_data/",
        "partitionKeys": ["country"],
    },
    transformation_ctx="AmazonS3_node1651640772927",
)

job.commit()
