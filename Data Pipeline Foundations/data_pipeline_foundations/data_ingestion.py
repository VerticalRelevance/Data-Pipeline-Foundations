from constructs import Construct
from aws_cdk import(
    Duration,
    Stack,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_kinesis as kinesis,
    aws_iam as iam,
    aws_kinesisfirehose as firehose,
    aws_logs as logs,
    aws_dms as dms,
    aws_secretsmanager as secretsmanager,
    aws_datasync as datasync,
    aws_s3_deployment as deployment,
    CfnOutput
)


class DataIngestion(Stack):



    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        ############################
        #General Ingestion Resources
        ############################

        #Creating the Raw Zone Bucket
        self.raw_bucket  = self.create_s3_bucket(bucket_name = "PipelineRawZone")

        #Creating a CloudWatch Log Group
        self.log_group = self.create_log_group(log_group_name = "PipelineLogGroup")
        
        #Adding a Stream to the CloudWatch Log Group
        self.log_stream = self.create_log_stream(log_stream_name = "PipelineLogStream",
                                                 log_group = self.log_group)
    
        #Dependencies
        self.log_stream.node.add_dependency(self.log_group)


    
        ##############################
        #Streaming Ingestion Resources
        ##############################

        #Creating an On demand Kinesis Data Stream
        self.data_stream = self.create_data_stream_on_demand(stream_name = "PipelineStream")
        
        #Creating the IAM Role for Kinesis Firehose
        self.firehose_iam = self.create_iam_role_firehose()

        #Creating the Firehose Consumer
        firehose_consumer = self.create_firehose_consumer(firehose_name = "PipelineFirehose",
                                                          data_stream_source = self.data_stream)

        #Dependencies
        firehose_consumer.node.add_dependency(self.log_stream)
        firehose_consumer.node.add_dependency(self.firehose_iam)
        firehose_consumer.node.add_dependency(self.data_stream)
        firehose_consumer.node.add_dependency(self.raw_bucket)

        ##########################
        #Batch Ingestion Resources
        ##########################

        #Creating the VPC for DMS
        self.dms_vpc = self.create_vpc(vpc_name = "DataPipelineVPC")

        #Creating the DMS Subnet Group
        dms_subnet_group = self.create_dms_subnet_group(subnet_group_name = "DataPipelineSubnetGroup")

        #Creating the DMS Replication Instance
        dms_rep_instance = self.create_dms_replication_instance(instance_name = "PipelineReplicationInstance",
                                                                instance_class = "dms.t3.medium",
                                                                sub_group = dms_subnet_group)

        #Creating the IAM role for DMS
        self.dms_iam_role = self.create_iam_role_dms()


        #Importing secret Source Endpoint Database Credentials
        sql_db_secret = secretsmanager.Secret.from_secret_complete_arn(self, "SQLDBSecret", 
                                                                       "arn:aws:secretsmanager:us-west-1:899456967600:secret:SushiMYSQL1-Z742ps"
                                                                       )
        #Creating the DMS Source Endpoint
        dms_source_endpoint = self.create_dms_source_endpoint(endpoint_name = "SQLDatabase",
                                                              secret = sql_db_secret)

        #Creating the DMS Target Endpoint
        dms_target_endpoint = self.create_dms_target_endpoint(endpoint_name = "RawBucket",
                                                              bucket_partition = "BatchData")

        #Creating the DMS Replication Task
        dms_replication_task = self.create_dms_replication_task(task_name = "PipelineDMSTask",
                                                                dms_source = dms_source_endpoint,
                                                                dms_target = dms_target_endpoint,
                                                                rep_instance = dms_rep_instance)

        #Adding Dependencies
        dms_subnet_group.node.add_dependency(self.dms_vpc)
        dms_rep_instance.node.add_dependency(dms_subnet_group)
        dms_source_endpoint.node.add_dependency(sql_db_secret)
        dms_target_endpoint.node.add_dependency(self.dms_iam_role)
        dms_replication_task.node.add_dependency(dms_source_endpoint)
        dms_replication_task.node.add_dependency(dms_target_endpoint)


        #######################################
        #Creating Flat File Ingestion Resources
        #######################################

        #Create Flat File S3 Bucket
        self.flat_file_bucket  = self.create_s3_bucket(bucket_name = "SampleFlatFileBucket")

        #Upload Sample Flat File Data to S3
        object_deployment = self.upload_to_s3(directory = "./flat_files",
                                              deployment_bucket = self.flat_file_bucket)

        #Create DataSync IAM Role
        self.datasync_role = self.create_iam_role_datasync()

        #Create DataSync Source Location
        datasync_source = self.create_datasync_source_location(location_name = "FileBucket")

        #Create DataSync Target Location
        datasync_target = self.create_datasync_destination_location(location_name = "RawBucket",
                                                                    bucket_prefix = "/FlatFiles")

        #Create DataSync Task
        datasync_task = self.create_datasync_task(task_name = "FlatFileTask",
                                                  data_source = datasync_source,
                                                  data_target = datasync_target)

        #Adding Dependencies
        datasync_source.node.add_dependency(self.datasync_role)
        datasync_target.node.add_dependency(self.datasync_role)
        datasync_task.node.add_dependency(datasync_source)
        datasync_task.node.add_dependency(datasync_target)

    
    ############################
    #ALL FUNCTIONS FOR RESOURCES
    ############################
    
    #############################
    #Function to Create S3 Bucket

    def create_s3_bucket(self, bucket_name: str):

        bucket = s3.Bucket(self, bucket_name,
                           bucket_name = bucket_name.lower(), #making bucket name lowercase
                           encryption = s3.BucketEncryption.KMS_MANAGED,
                           block_public_access = s3.BlockPublicAccess(block_public_acls = True,  #blocking all public access
                                                                      block_public_policy = True, 
                                                                      ignore_public_acls  = True, 
                                                                      restrict_public_buckets = True)
        )

        return bucket


    ##############################################
    #Function to Create Provisioned Kinesis Stream

    def create_data_stream_provisioned(self, stream_name: str, shard_count: int):

        data_stream = kinesis.CfnStream(self, stream_name,
                                        name = stream_name,
                                        retention_period_hours = 168, 
                                        shard_count = shard_count,
                                        stream_encryption = kinesis.CfnStream.StreamEncryptionProperty(encryption_type = "KMS", 
                                                                                                       key_id = "alias/aws/kinesis"),
                                        stream_mode_details = kinesis.CfnStream.StreamModeDetailsProperty(stream_mode="PROVISIONED")
        )

        return data_stream


    #################################################
    #Function to Create On-Demand Kinesis Data Stream
    
    def create_data_stream_on_demand(self, stream_name: str):

        data_stream = kinesis.CfnStream(self, stream_name,
                                        name = stream_name,
                                        retention_period_hours = 168, 
                                        stream_encryption = kinesis.CfnStream.StreamEncryptionProperty(encryption_type = "KMS", 
                                                                                                       key_id = "alias/aws/kinesis"),
                                        stream_mode_details = kinesis.CfnStream.StreamModeDetailsProperty(stream_mode="ON_DEMAND")
        )

        return data_stream


    ##########################################################
    #Function to Create IAM Role for Kinesis Firehose Consumer    

    def create_iam_role_firehose(self):

        firehose_role = iam.Role(self, "Firehose Role", 
                                 assumed_by = iam.ServicePrincipal(service = "firehose.amazonaws.com"), 
                                 description = "Kinesis Role for Data Pipeline"
        )


        #Policy to get access to Raw Zone bucket
        firehose_role.add_to_principal_policy(statement = iam.PolicyStatement(actions = ["s3:AbortMultipartUpload",
                                                                                         "s3:GetBucketLocation",
                                                                                         "s3:GetObject",
                                                                                         "s3:ListBucket",
                                                                                         "s3:ListBucketMultipartUploads",
                                                                                         "s3:PutObject"], 
                                                                            resources = [self.raw_bucket.bucket_arn + "/*",
                                                                                         self.raw_bucket.bucket_arn])
        )


        #Policy to Add logs to CloudWatch
        firehose_role.add_to_principal_policy(statement = iam.PolicyStatement(actions = ["logs:PutLogEvents"], 
                                                                              resources = ["*"])
        )


        #Policy to get data from the Kinesis Data Stream
        firehose_role.add_to_principal_policy(statement = iam.PolicyStatement(actions = ["kinesis:DescribeStream",
                                                                                         "kinesis:GetShardIterator",
                                                                                         "kinesis:GetRecords",
                                                                                         "kinesis:ListShards"], 
                                                                              resources = [self.data_stream.attr_arn])
        )

        #Policy to decrpyt data from Kinesis Data Stream
        firehose_role.add_to_principal_policy(statement = iam.PolicyStatement(actions = ["kms:Decrypt"], 
                                                                              resources = ["*"])
        )


        return firehose_role


    ########################################
    #Function to Create CloudWatch Log Group

    def create_log_group(self, log_group_name: str):

        log_group = logs.LogGroup(self, log_group_name,
                                  log_group_name = log_group_name
        )

        return log_group

                                
    #########################################
    #Function to Create CloudWatch Log Stream

    def create_log_stream(self, log_stream_name: str, log_group): #pass the log group object created above

        log_stream = log_group.add_stream(id = log_stream_name,
                                          log_stream_name = log_stream_name
        )
                                 

        return log_stream


    #####################################
    #Function to Create Firehose Consumer

    def create_firehose_consumer(self, firehose_name: str, data_stream_source):

        #Parameter to partition the data
        data_partition_parameters = [firehose.CfnDeliveryStream.ProcessorParameterProperty(parameter_name = "MetadataExtractionQuery", 
                                                                                           parameter_value = "{customer_id:.customer_id}"),
                                     firehose.CfnDeliveryStream.ProcessorParameterProperty(parameter_name = "JsonParsingEngine", 
                                                                                           parameter_value = "JQ-1.6")
        ]

        data_partition = firehose.CfnDeliveryStream.ProcessorProperty(type = "MetadataExtraction",
                                                                      parameters = data_partition_parameters
        )

        #Paramter to append delimiter to incoming records
        delimiter = firehose.CfnDeliveryStream.ProcessorProperty(type = "AppendDelimiterToRecord",
                                                                 parameters = [firehose.CfnDeliveryStream.ProcessorParameterProperty(parameter_name = "Delimiter", 
                                                                                                                                     parameter_value = "\\n")]
        )


        processing_config = firehose.CfnDeliveryStream.ProcessingConfigurationProperty(enabled = True,
                                                                                              processors = [data_partition, delimiter]
        )

        delivery_stream = firehose.CfnDeliveryStream(self, firehose_name,
                                                     delivery_stream_name = firehose_name,
                                                     delivery_stream_type = "KinesisStreamAsSource",

                                                     kinesis_stream_source_configuration = firehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(kinesis_stream_arn = data_stream_source.attr_arn,
                                                                                                                                                               role_arn = self.firehose_iam.role_arn),
                                                                            
                                                     extended_s3_destination_configuration = firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(bucket_arn = self.raw_bucket.bucket_arn,
                                                                                                                                                                   role_arn = self.firehose_iam.role_arn,
                                                                                                                                                                   buffering_hints = firehose.CfnDeliveryStream.BufferingHintsProperty(interval_in_seconds = 60, 
                                                                                                                                                                                                                                       size_in_m_bs = 64), 
                                                                                                                                                                   cloud_watch_logging_options = firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(enabled = True,
                                                                                                                                                                                                                                                             log_group_name = self.log_group.log_group_name,
                                                                                                                                                                                                                                                             log_stream_name = self.log_stream.log_stream_name),
                                                                                                                                                                   dynamic_partitioning_configuration = firehose.CfnDeliveryStream.DynamicPartitioningConfigurationProperty(enabled = True,
                                                                                                                                                                                                                                                                            retry_options = firehose.CfnDeliveryStream.RetryOptionsProperty(duration_in_seconds = 300)),
                                                                                                                                                                   error_output_prefix = "StreamingData/Error",
                                                                                                                                                                   prefix = "StreamingData/!{partitionKeyFromQuery:customer_id}",
                                                                                                                                                                   processing_configuration = processing_config
                                                     )
        )    

        return delivery_stream    


    ###############################
    #Function to Create VPC for DMS

    def create_vpc(self, vpc_name: str):

        
        #Creating the VPC
    
        vpc = ec2.Vpc(self, vpc_name,
                           cidr = "10.0.0.0/16",   
                           default_instance_tenancy = ec2.DefaultInstanceTenancy("DEFAULT"), 
                           enable_dns_hostnames = True, 
                           enable_dns_support =  True,  
                           max_azs = 2, 
                           subnet_configuration = [ec2.SubnetConfiguration(name = vpc_name + "PublicSubnet", 
                                                         subnet_type = ec2.SubnetType("PUBLIC"))  
                            ],
                           vpc_name = vpc_name
        ) 


        #Creating a Security Group for the VPC

        self.vpc_security_group = ec2.SecurityGroup(self, vpc_name + "Sg", 
                                                    vpc = vpc,
                                                    allow_all_outbound = True, 
                                                    description = "Allow MYSQL Connection",
                                                    security_group_name = vpc_name + "AllowSQL"
        )

        #Adding rule to allow MYSQL traffic and traffic from the SG
        self.vpc_security_group.add_ingress_rule(peer = self.vpc_security_group, connection = ec2.Port.all_traffic())

        self.vpc_security_group.add_ingress_rule(peer = ec2.Peer.any_ipv4(), connection = ec2.Port.tcp(3306))


        return vpc


    def create_dms_subnet_group(self, subnet_group_name: str):

        public_subnets = self.dms_vpc.public_subnets
        public_subnet_ids = [public_subnets[0].subnet_id, public_subnets[1].subnet_id]


        dms_subnet_group = dms.CfnReplicationSubnetGroup(self, subnet_group_name,
                                                         replication_subnet_group_description = subnet_group_name,
                                                         subnet_ids = public_subnet_ids,
                                                         replication_subnet_group_identifier = subnet_group_name.lower()
        )

        return dms_subnet_group

                                                       

    def create_dms_replication_instance(self, instance_name: str, instance_class: str, sub_group):

        replication_instance = dms.CfnReplicationInstance(self, instance_name,
                                                          replication_instance_class = instance_class, 
                                                          publicly_accessible = True,
                                                          replication_instance_identifier = instance_name,
                                                          replication_subnet_group_identifier = sub_group.replication_subnet_group_identifier,
                                                          resource_identifier = instance_name,
                                                          vpc_security_group_ids = [self.vpc_security_group.security_group_id]
        )

        return replication_instance


    def create_iam_role_dms(self):

        dms_role = iam.Role(self, "SushiPipelineDMS", 
                            assumed_by = iam.ServicePrincipal(service = "dms.amazonaws.com"), 
                            description = "Role for DMS to access S3 raw zone bucket")
        

        dms_role.add_to_principal_policy(statement = iam.PolicyStatement(actions = ["s3:PutObject",
                                                                                    "s3:DeleteObject",
                                                                                    "s3:ListBucket"], 
                                                                         resources = [self.raw_bucket.bucket_arn + "/*",
                                                                                      self.raw_bucket.bucket_arn]))

        return dms_role

    
    def create_dms_source_endpoint(self, endpoint_name: str, secret): #pass in secret object for secret

        my_sql_endpoint = dms.CfnEndpoint(self, endpoint_name, 
                                          endpoint_type="source",
                                          engine_name="mysql", 
                                          endpoint_identifier = endpoint_name,
                                          password = secret.secret_value_from_json("password").to_string(), 
                                          username = secret.secret_value_from_json("username").to_string(),
                                          port = 3306, 
                                          server_name = secret.secret_value_from_json("host").to_string()
        )

        return my_sql_endpoint


    def create_dms_target_endpoint(self, endpoint_name: str, bucket_partition: str):

        dms_s3_endpoint = dms.CfnEndpoint(self, "SushiPipelineLandingZoneEndpoint", 
                                          endpoint_type="target",
                                          engine_name="s3", 
                                          endpoint_identifier = "SushiPipelineLandingZoneEndpoint",
                                          s3_settings = dms.CfnEndpoint.S3SettingsProperty(add_column_name = True,
                                                                                           bucket_folder = "BatchData",
                                                                                           bucket_name = self.raw_bucket.bucket_name,
                                                                                           service_access_role_arn = self.dms_iam_role.role_arn))


        return dms_s3_endpoint


    def create_dms_replication_task(self, task_name: str, dms_source, dms_target, rep_instance):

        full_load_replication = dms.CfnReplicationTask(self, task_name,
                                                         migration_type = "full-load",
                                                         replication_instance_arn = rep_instance.ref,
                                                         source_endpoint_arn = dms_source.ref,
                                                         table_mappings = "{ \"rules\": [ { \"rule-type\": \"selection\", \"rule-id\": \"1\", \"rule-name\": \"1\", \"object-locator\": { \"schema-name\": \"%\", \"table-name\": \"%\" }, \"rule-action\": \"include\" } ] }",
                                                         target_endpoint_arn = dms_target.ref,
                                                         replication_task_identifier = task_name,
        )

        return full_load_replication



    def create_iam_role_datasync(self):

        data_sync_role = iam.Role(self, "DataSyncZoneRole", 
                                  assumed_by = iam.ServicePrincipal(service = "datasync.amazonaws.com"), 
                                  description = "Role for DataSync for Flat File Ingestion")

        data_sync_role.add_to_principal_policy(statement = iam.PolicyStatement(actions = ["s3:GetBucketLocation",
                                                                                          "s3:ListBucket",
                                                                                          "s3:ListBucketMultipartUploads",
                                                                                          "s3:AbortMultipartUpload",
                                                                                          "s3:DeleteObject",
                                                                                          "s3:GetObject",
                                                                                          "s3:ListMultipartUploadParts",
                                                                                          "s3:GetObjectTagging",
                                                                                          "s3:PutObjectTagging",
                                                                                          "s3:PutObject"], 
                                                                               resources = [self.raw_bucket.bucket_arn + "/*",
                                                                                            self.raw_bucket.bucket_arn,
                                                                                            self.flat_file_bucket.bucket_arn + "/*",
                                                                                            self.flat_file_bucket.bucket_arn]))

        return data_sync_role


    def create_datasync_source_location(self, location_name: str):

        data_sync_config = datasync.CfnLocationS3.S3ConfigProperty(bucket_access_role_arn = self.datasync_role.role_arn)

        s3_location = datasync.CfnLocationS3(self, location_name,
                                             s3_bucket_arn = self.flat_file_bucket.bucket_arn, 
                                             s3_config = data_sync_config)

        return s3_location



    def create_datasync_destination_location(self, location_name: str, bucket_prefix: str):

        data_sync_config = datasync.CfnLocationS3.S3ConfigProperty(bucket_access_role_arn = self.datasync_role.role_arn)   

        s3_location = datasync.CfnLocationS3(self, location_name,
                                             s3_bucket_arn = self.raw_bucket.bucket_arn, 
                                             s3_config = data_sync_config,
                                             subdirectory = bucket_prefix)

        return s3_location



    def create_datasync_task(self, task_name: str, data_source, data_target):

        datasync_task = datasync.CfnTask(self, task_name, 
                                         source_location_arn = data_source.attr_location_arn,
                                         destination_location_arn = data_target.attr_location_arn, 
                                         name = task_name,
                                         options = datasync.CfnTask.OptionsProperty(transfer_mode = "CHANGED"),
                                         schedule = datasync.CfnTask.TaskScheduleProperty(schedule_expression = "cron(0 0 20/24 ? * * *)")
        )

        return datasync_task
                                        

    def upload_to_s3(self, directory: str, deployment_bucket):

        object_deployment = deployment.BucketDeployment(self, "Deployment",
                                                        sources = [deployment.Source.asset(directory)],
                                                        destination_bucket = deployment_bucket
        )

        return object_deployment





