import boto3
import datetime

TARGET_ACCOUNT_ID = '<fill_target_id>'
CUSTOMER_MANAGED_KEY = '<fill_arn_of_the_customer_managed_key>'
rds_src = boto3.client("rds", region_name="<fill_source_region>")


def lambda_handler(event, context):
   db_id = event.get("db_name")
   print(f"Start of cross region snapshot creation for DB instance {db_id}")


   snapshot_id = build_snapshot_id(db_id)
   create_a_new_snapshot(snapshot_id, db_id)


   # Re-encrypt to your SOURCE CMK (same account, same region)
   re_encrypted_snapshot_id = f"{snapshot_id}-re-encrypted"
   re_encrypt_snapshot_using_customer_managed_key(snapshot_id, re_encrypted_snapshot_id)
   share_snapshot_with_backup_account(re_encrypted_snapshot_id)


   delete_original_snapshot(snapshot_id)


   #cleanup the re-encrypted snapshot of last week if exists
   last_week_snapshot_id = f"{db_id}-{get_last_week_date()}-cmk"
   cleanup_last_weeks_reencrypted_snapshot(last_week_snapshot_id)




def cleanup_last_weeks_reencrypted_snapshot(last_week_snapshot_id):
   try:
       rds_src.delete_db_snapshot(DBSnapshotIdentifier=last_week_snapshot_id)
       print(f"Last week snapshot {last_week_snapshot_id} deleted")
       #wait for deletion to complete
       rds_src.get_waiter('db_snapshot_deleted').wait(DBSnapshotIdentifier=last_week_snapshot_id)
       print(f"last week re encrypted snapshot {last_week_snapshot_id} deleted")
   except rds_src.exceptions.DBSnapshotNotFoundFault:
       print(f"No last week snapshot {last_week_snapshot_id} to delete")
  
def delete_original_snapshot(snapshot_id):
   rds_src.delete_db_snapshot(DBSnapshotIdentifier=snapshot_id)
   print(f"Original snapshot {snapshot_id} deleted")


def share_snapshot_with_backup_account(re_encrypted_snapshot_id):
   rds_src.modify_db_snapshot_attribute(
       DBSnapshotIdentifier=re_encrypted_snapshot_id,
       AttributeName='restore',
       ValuesToAdd=[TARGET_ACCOUNT_ID]
   )
   print(f"Snapshot restore attribute changed for account {TARGET_ACCOUNT_ID} and snapshot {re_encrypted_snapshot_id}")


def re_encrypt_snapshot_using_customer_managed_key(snapshot_id, re_encrypted_snapshot_id):
   rds_src.copy_db_snapshot(
       SourceDBSnapshotIdentifier=snapshot_id,             
       TargetDBSnapshotIdentifier=re_encrypted_snapshot_id,
       KmsKeyId=CUSTOMER_MANAGED_KEY
   )
   rds_src.get_waiter('db_snapshot_available').wait(DBSnapshotIdentifier=re_encrypted_snapshot_id)
   print(f"End of snapshot {re_encrypted_snapshot_id} rencryption and copying")


def create_a_new_snapshot(snapshot_id, db_id):
   rds_src.create_db_snapshot(DBSnapshotIdentifier=snapshot_id, DBInstanceIdentifier=db_id)
   rds_src.get_waiter('db_snapshot_available').wait(DBSnapshotIdentifier=snapshot_id)
   print(f"End of local Snapshot {snapshot_id} creation for DB instance {db_id}")


def build_snapshot_id(db_id):
   snapshot_name_appender = '%Y-%m-%d' # Only once in a day snapshot will be taken. change this for more frequent snapshots
   date_str = datetime.datetime.now(datetime.timezone.utc).strftime(snapshot_name_appender)
   snapshot_id = f"{db_id}-{date_str}"
   return snapshot_id


def get_last_week_date():
   today = datetime.datetime.now(datetime.timezone.utc)
   last_week = today - datetime.timedelta(days=7)
   return last_week.strftime('%Y-%m-%d')
