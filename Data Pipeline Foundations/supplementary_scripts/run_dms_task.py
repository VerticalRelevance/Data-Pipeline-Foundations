import boto3

#Enter replication task ARN
replication_task_arn = ""

dms = boto3.client('dms')



start_task = dms.start_replication_task(ReplicationTaskArn = replication_task_arn,
                                        StartReplicationTaskType = 'start-replication',
)
