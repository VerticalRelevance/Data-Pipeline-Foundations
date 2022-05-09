import boto3

#Enter replication task ARN
replication_task_arn = "arn:aws:dms:us-west-1:899456967600:task:3PI6UXFCV5C62LJNCS33PKJTIEMHBGAM4PPLWCI"

dms = boto3.client('dms')



start_task = dms.start_replication_task(ReplicationTaskArn = replication_task_arn,
                                        StartReplicationTaskType = 'start-replication',
)