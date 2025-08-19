import boto3
import datetime

rds = boto3.client('rds', region_name="source_region>")

TARGET_KMS_KEY_ARN = '<backup_customer_managed_key_arn>'

def lambda_handler(event, context):
   db_id = event.get("db_name")
   print(f"Start of cross region snapshot creation for DB instance {db_id}")
   snapshot_to_copy_id = build_snapshot_id(db_id)
   snapshot_to_copy_arn = f"<arn_prefix_of_the_snapshot>:{snapshot_to_copy_id}-re-encrypted"
   print(f"Start of copying {snapshot_to_copy_arn}")


   date_str = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d-%H-%M-%S')
   imported_snapshot_id = f"{snapshot_to_copy_id}-imported"


   copy_snapshot(snapshot_to_copy_arn, imported_snapshot_id)
  
   delete_last_week_snapshot(db_id)


def delete_last_week_snapshot(db_id):
   last_week_snapshot_id = f"{db_id}-{get_last_week_date()}-re-encrypted"
   try:
       rds.delete_db_snapshot(DBSnapshotIdentifier=last_week_snapshot_id)
       rds.get_waiter('db_snapshot_deleted').wait(DBSnapshotIdentifier=last_week_snapshot_id)
       print(f"last week re encrypted snapshot {last_week_snapshot_id} deleted")
   except rds.exceptions.DBSnapshotNotFoundFault:
       print(f"No last week snapshot {last_week_snapshot_id} to delete")


def copy_snapshot(snapshot_to_copy_arn, imported_snapshot_id):
   rds.copy_db_snapshot(
       SourceDBSnapshotIdentifier=snapshot_to_copy_arn,
       TargetDBSnapshotIdentifier=imported_snapshot_id,
       KmsKeyId=TARGET_KMS_KEY_ARN,
       SourceRegion="ap-southeast-2"
   )
   rds.get_waiter('db_snapshot_available').wait(DBSnapshotIdentifier=imported_snapshot_id)
   print(f"End of snapshot copy {imported_snapshot_id} ")


def build_snapshot_id(db_id):
   snapshot_name_appender = '%Y-%m-%d' # Only once in a day snapshot will be taken. change this for more frequent snapshots
   date_str = datetime.datetime.now(datetime.timezone.utc).strftime(snapshot_name_appender)
   snapshot_id = f"{db_id}-{date_str}"
   return snapshot_id


def get_last_week_date():
   today = datetime.datetime.now(datetime.timezone.utc)
   last_week = today - datetime.timedelta(days=7)
   return last_week.strftime('%Y-%m-%d')
