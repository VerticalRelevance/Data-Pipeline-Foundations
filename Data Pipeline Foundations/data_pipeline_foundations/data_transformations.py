from constructs import Construct
from aws_cdk import(
    Duration,
    Stack,
    CfnOutput,
    aws_s3 as s3,
    aws_glue as glue,
    aws_iam as iam,
    aws_stepfunctions as stepfunctions,
    aws_events as events,
    aws_s3_deployment as deployment,
    aws_cloudtrail as trails
)


class DataTransformations(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        #########################
        #Transformation Resources
        #########################


        #Creating the Transformed Bucket
        self.transformed_bucket  = self.create_s3_bucket(bucket_name = "PipelineTransformedZone")

        #Creating the Curated Bucket
        self.curated_bucket  = self.create_s3_bucket(bucket_name = "PipelineCuratedZone")

        #Creating the Glue Script Bucket
        self.glue_script_bucket  = self.create_s3_bucket(bucket_name = "PipelineGlueScriptBucket")

        #Uploading all of the Glue Scripts to the Glue Script Bucket
        glue_script_upload = self.upload_to_s3(directory = "./glue_job_scripts",
                                               deployment_bucket = self.glue_script_bucket,
                                               deployment_name = "glueupload")


        #Creating the Glue IAM Role
        self.glue_iam_role = self.create_iam_role_glue()


        #Creating Glue Jobs - Raw to Transformed

        transformed_customer_data_table = self.create_glue_job(job_name = "transformed_customer_data_table", 
                                                               script_location = "s3://pipelinegluescriptbucket/transformed_customer_data.py")

        transformed_transactions_table = self.create_glue_job(job_name = "transformed_transactions_table", 
                                                              script_location = "s3://pipelinegluescriptbucket/transformed_transactions.py")


        #Creating Glue Jobs - Transformed to Curated
        curated_customer_data_table = self.create_glue_job(job_name = "curated_customer_data_table", 
                                                           script_location = "s3://pipelinegluescriptbucket/curated_customer_data_USA.py")

        curated_transactions_table = self.create_glue_job(job_name = "curated_transactions_table", 
                                                          script_location = "s3://pipelinegluescriptbucket/curated_transactions.py")


        #Dependencies
        glue_script_upload.node.add_dependency(self.glue_script_bucket)
        transformed_customer_data_table.node.add_dependency(glue_script_upload)
        transformed_transactions_table.node.add_dependency(glue_script_upload)
        curated_customer_data_table.node.add_dependency(glue_script_upload)
        curated_transactions_table.node.add_dependency(glue_script_upload)
        transformed_customer_data_table.node.add_dependency(self.glue_iam_role)
        transformed_transactions_table.node.add_dependency(self.glue_iam_role)
        curated_customer_data_table.node.add_dependency(self.glue_iam_role)
        curated_transactions_table.node.add_dependency(self.glue_iam_role)
        transformed_customer_data_table.node.add_dependency(self.transformed_bucket)
        transformed_transactions_table.node.add_dependency(self.transformed_bucket)
        curated_customer_data_table.node.add_dependency(self.curated_bucket)
        curated_transactions_table.node.add_dependency(self.curated_bucket)


        ###################
        #Workflow Resources
        ###################

        #Creating S3 Bucket to store stepfunction workflow script
        self.workflow_bucket  = self.create_s3_bucket(bucket_name = "PipelineWorkflowBucket")

        #Uploading workflow script to S3 bucket
        workflow_upload =  self.upload_to_s3(directory = "./workflow_script",
                                             deployment_bucket = self.workflow_bucket,
                                             deployment_name = "workflowupload")

        #Creating IAM role for step functions
        self.step_function_role = self.create_iam_role_stepfunction()

        #Creating Step function state machine
        state_machine = self.create_stepfunction_state_machine(stepfunction_name = "pipelinestatemachine", #no special characters in name
                                                                    bucket_name = self.workflow_bucket.bucket_name, 
                                                                    script_file_name = "data_pipeline.json")


        #Creating IAM role for eventbridge
        self.event_bridge_role = self.create_iam_role_event_bridge()

        #Creating Eventbridge Rule to kick off glue workflow
        event_bridge_rule = self.create_eventbridge_rule_time(rule_name = "StateMachineExecute", 
                                                              state_machine_arn = state_machine.attr_arn)

        
        #Dependencies
        workflow_upload.node.add_dependency(self.workflow_bucket)
        state_machine.node.add_dependency(workflow_upload)
        state_machine.node.add_dependency(self.step_function_role)
        event_bridge_rule.node.add_dependency(self.event_bridge_role)
        event_bridge_rule.node.add_dependency(state_machine)




    ############################
    #ALL FUNCTIONS FOR RESOURCES
    ############################


    def create_s3_bucket(self, bucket_name):

        bucket = s3.Bucket(self, bucket_name,
                           bucket_name = bucket_name.lower(), #making bucket name lowercase
                           encryption = s3.BucketEncryption.KMS_MANAGED,
                           block_public_access = s3.BlockPublicAccess(block_public_acls = True,  #blocking all public access
                                                                      block_public_policy = True, 
                                                                      ignore_public_acls  = True, 
                                                                      restrict_public_buckets = True)
        )

        return bucket

    def create_iam_role_glue(self):

        glue_role = iam.Role(self, "PipelineGlueRole", 
                             assumed_by = iam.ServicePrincipal(service = "glue.amazonaws.com"), 
                             description = "Role for glue for the data pipeline")


        glue_role.add_managed_policy(policy = iam.ManagedPolicy.from_managed_policy_arn(self, "ManagedPolicyGlue", 
                                                                                        managed_policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"))

        glue_role.add_to_principal_policy(statement = iam.PolicyStatement(actions = ["s3:ListBucket",
                                                                                     "s3:GetObject",
                                                                                     "s3:PutObject",
                                                                                     "s3:DeleteObject"], 
                                                                          resources = ["*"]))

        return glue_role


    def upload_glue_script_to_s3(self, directory: str):

        object_deployment = deployment.BucketDeployment(self, "Deployment",
                                                        sources = [deployment.Source.asset(directory)],
                                                        destination_bucket = self.flat_file_bucket
        )

        return object_deployment


    def create_glue_job(self, job_name: str, script_location: str):

        glue_job = glue.CfnJob(self, job_name, 
                               command = glue.CfnJob.JobCommandProperty(name = "glueetl",
                                                                        script_location= script_location), 
                               role = self.glue_iam_role.role_arn,
                               glue_version = "3.0", 
                               name = job_name
                           #    default_arguments = {"--job-bookmark-option": "job-bookmark-enable"}
                            )

        return glue_job


    def create_iam_role_stepfunction(self):

        step_function_role = iam.Role(self, "PipelineStepFunctionRole", 
                                      assumed_by = iam.ServicePrincipal(service = "states.amazonaws.com"),
                                      description = "Role for stepfunctions to execute all the jobs in the pipeline")

        #Adding policies to the IAM role
        step_function_role.add_to_principal_policy(statement = iam.PolicyStatement(actions = ["s3:ListBucket",
                                                                                              "s3:GetObject"], 
                                                                                    resources = [self.workflow_bucket.bucket_arn,
                                                                                                 self.workflow_bucket.bucket_arn + "/*"]))

        step_function_role.add_to_principal_policy(statement = iam.PolicyStatement(actions = ["glue:StartJobRun",
                                                                                              "glue:GetJobRuns",
                                                                                              "glue:GetJobRun",
                                                                                              "glue:BatchStopJobRun"], 
                                                                                   resources = ["*"]))

        step_function_role.add_to_principal_policy(statement = iam.PolicyStatement(actions = ["logs:*"], 
                                                                                    resources = ["*"]))

        return step_function_role

    def create_stepfunction_state_machine(self, stepfunction_name: str, bucket_name: str, script_file_name: str):

        step_function_location = stepfunctions.CfnStateMachine.S3LocationProperty(bucket = bucket_name, key = script_file_name)

        step_function = stepfunctions.CfnStateMachine(self, stepfunction_name,
                                                      role_arn = self.step_function_role.role_arn,
                                                      definition_s3_location = step_function_location,
                                                      state_machine_name = stepfunction_name)

        return step_function

    def cloudtrail_trail(self):

        trail = trails.CfnTrail(self, "S3LoggingTrail",
                                is_logging = True,
                                )


    def create_iam_role_event_bridge(self):

        event_bridge_role = iam.Role(self, "PipelineEventBridgeRole", 
                                      assumed_by = iam.ServicePrincipal(service = "events.amazonaws.com"),
                                      description = "Role for Eventbridge to execute the stepfunction statemachine")

        #Adding policies to the IAM role
        event_bridge_role.add_to_principal_policy(statement = iam.PolicyStatement(actions = ["states:StartExecution"], 
                                                                                    resources = ["*"]))
                                                                                
        return event_bridge_role

        
    def create_eventbridge_rule_time(self, rule_name: str, state_machine_arn: str):

        rule = events.CfnRule(self, rule_name,
                              role_arn = self.event_bridge_role.role_arn,
                              schedule_expression = "cron(0 23 * * ? *)",
                              state = "ENABLED",
                              targets = [events.CfnRule.TargetProperty(arn = state_machine_arn,
                                                                       id = "PipelineStateMachine",
                                                                       role_arn = self.event_bridge_role.role_arn)])

        return rule


    def upload_to_s3(self, directory: str, deployment_bucket, deployment_name: str):

        object_deployment = deployment.BucketDeployment(self, deployment_name,
                                                        sources = [deployment.Source.asset(directory)],
                                                        destination_bucket = deployment_bucket
        )

        return object_deployment