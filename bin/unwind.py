#!/usr/bin/env python3
"""
AWS DMS Data Ingestion Unwind Script

This script reads from working-parameters.json and systematically destroys
all AWS resources created during the deployment in the correct order.
"""

import boto3
import pyodbc
import json
import logging
import os
import time
from pathlib import Path
from botocore.exceptions import ClientError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('unwind.log')
    ]
)
logger = logging.getLogger(__name__)

class UnwindManager:
    """Manages the systematic destruction of AWS DMS infrastructure."""
    
    def __init__(self):
        self.working_params_file = Path(__file__).parent / 'working-parameters.json'
        self.env_file = Path(__file__).parent / '.env'
        self.params = self.load_parameters()
        self.load_environment()
        
        # Initialize AWS clients
        self.region = self.params.get('configuration', {}).get('region', 'us-east-1')
        self.dms_client = boto3.client('dms', region_name=self.region)
        self.rds_client = boto3.client('rds', region_name=self.region)
        self.s3_client = boto3.client('s3', region_name=self.region)
        self.iam_client = boto3.client('iam')
        self.ec2_client = boto3.client('ec2', region_name=self.region)
        self.cloudwatch_client = boto3.client('cloudwatch', region_name=self.region)
        
    def load_parameters(self):
        """Load deployment parameters from working-parameters.json"""
        try:
            with open(self.working_params_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load parameters from {self.working_params_file}: {e}")
            return {}
    
    def load_environment(self):
        """Load environment variables from .env file"""
        try:
            with open(self.env_file, 'r') as f:
                for line in f:
                    if '=' in line and not line.strip().startswith('#'):
                        key, value = line.strip().split('=', 1)
                        os.environ[key] = value
        except Exception as e:
            logger.warning(f"Failed to load environment from {self.env_file}: {e}")
    
    def confirm_destruction(self):
        """Ask user for confirmation before destroying resources"""
        resources = self.params.get('created_resources', {})
        if not resources:
            logger.info("No resources found in working-parameters.json")
            return False
            
        print("\n🚨 WARNING: This will PERMANENTLY DELETE the following AWS resources:")
        print("=" * 70)
        
        for key, value in resources.items():
            if value:  # Only show non-empty values
                print(f"  • {key}: {value}")
        
        print("=" * 70)
        print("This action CANNOT be undone!")
        
        response = input("\nType 'DESTROY' (all caps) to confirm destruction: ")
        return response == "DESTROY"
    
    def delete_cloudwatch_alarms(self):
        """Delete CloudWatch alarms created for DMS monitoring"""
        logger.info("🔥 Deleting CloudWatch alarms...")
        
        try:
            task_id = self.params.get('created_resources', {}).get('migration_task_id')
            if not task_id:
                logger.info("No migration task ID found, skipping alarm deletion")
                return
            
            # Standard alarm names created by the deployment
            alarm_names = [
                f"DMS-Task-Failure-{task_id}",
                f"DMS-High-Replication-Lag-{task_id}",
                f"DMS-Low-Throughput-{task_id}"
            ]
            
            # Delete alarms in batches (CloudWatch allows max 100 per call)
            for alarm_name in alarm_names:
                try:
                    self.cloudwatch_client.delete_alarms(AlarmNames=[alarm_name])
                    logger.info(f"  ✅ Deleted CloudWatch alarm: {alarm_name}")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ResourceNotFound':
                        logger.info(f"  ⚠️  Alarm not found (already deleted): {alarm_name}")
                    else:
                        logger.error(f"  ❌ Failed to delete alarm {alarm_name}: {e}")
            
            logger.info("✅ CloudWatch alarms deletion completed")
            
        except Exception as e:
            logger.error(f"❌ Error deleting CloudWatch alarms: {e}")
    
    def delete_dms_migration_task(self):
        """Delete DMS migration task"""
        logger.info("🔥 Deleting DMS migration task...")
        
        try:
            # Search for ALL replication tasks and delete them
            logger.info("  Searching for all replication tasks...")
            deleted_any = False
            
            try:
                response = self.dms_client.describe_replication_tasks()
                all_tasks = response.get('ReplicationTasks', [])
                
                if not all_tasks:
                    logger.info("  No replication tasks found")
                    return
                
                for task in all_tasks:
                    task_arn = task['ReplicationTaskArn']
                    task_id = task['ReplicationTaskIdentifier']
                    logger.info(f"  Found task: {task_id} - {task_arn}")
                    self._delete_single_task(task_arn)
                    deleted_any = True
                
                if deleted_any:
                    # Wait a bit for deletions to propagate
                    logger.info("  Waiting for task deletions to complete...")
                    time.sleep(10)
                    
            except ClientError as e:
                logger.error(f"Could not list replication tasks: {e}")
                
            # Also try the task from parameters if it exists
            task_arn = self.params.get('created_resources', {}).get('migration_task_arn')
            if task_arn:
                logger.info(f"  Also trying task from parameters: {task_arn}")
                self._delete_single_task(task_arn)
                    
        except Exception as e:
            logger.error(f"❌ Error deleting migration task: {e}")
    
    def _delete_single_task(self, task_arn):
        """Delete a single DMS replication task"""
        try:
            response = self.dms_client.describe_replication_tasks(
                Filters=[{'Name': 'replication-task-arn', 'Values': [task_arn]}]
            )
            
            if response['ReplicationTasks']:
                task = response['ReplicationTasks'][0]
                task_status = task['Status']
                task_id = task['ReplicationTaskIdentifier']
                logger.info(f"    Task {task_id} status: {task_status}")
                
                # Stop the task if it's running
                if task_status in ['running', 'starting', 'resuming', 'modifying']:
                    logger.info(f"    Stopping migration task: {task_id}")
                    try:
                        self.dms_client.stop_replication_task(ReplicationTaskArn=task_arn)
                        
                        # Wait for task to stop with shorter timeout per attempt
                        logger.info(f"    Waiting for task {task_id} to stop...")
                        max_wait_time = 300  # 5 minutes max
                        wait_interval = 10
                        elapsed = 0
                        
                        while elapsed < max_wait_time:
                            time.sleep(wait_interval)
                            elapsed += wait_interval
                            
                            check_response = self.dms_client.describe_replication_tasks(
                                Filters=[{'Name': 'replication-task-arn', 'Values': [task_arn]}]
                            )
                            
                            if check_response['ReplicationTasks']:
                                current_status = check_response['ReplicationTasks'][0]['Status']
                                logger.info(f"      Task {task_id} status: {current_status}")
                                
                                if current_status in ['stopped', 'failed', 'ready']:
                                    logger.info(f"    Task {task_id} stopped successfully")
                                    break
                            else:
                                logger.info(f"    Task {task_id} no longer exists")
                                break
                        else:
                            logger.warning(f"    Task {task_id} did not stop within timeout, trying to delete anyway")
                            
                    except ClientError as stop_error:
                        logger.warning(f"    Could not stop task {task_id}: {stop_error}")
                        # Continue and try to delete anyway
            
            # Delete the task
            logger.info(f"    Deleting task: {task_arn}")
            self.dms_client.delete_replication_task(ReplicationTaskArn=task_arn)
            logger.info(f"  ✅ Deleted migration task: {task_arn}")
            
        except ClientError as e:
            if e.response['Error']['Code'] in ['ResourceNotFoundFault']:
                logger.info(f"  ⚠️  Task not found: {task_arn}")
            elif e.response['Error']['Code'] in ['InvalidResourceStateFault']:
                logger.warning(f"  ⚠️  Task in invalid state, may already be deleting: {task_arn}")
            else:
                logger.error(f"  ❌ Failed to delete task {task_arn}: {e}")
                raise
    
    def delete_dms_endpoints(self):
        """Delete DMS source and target endpoints"""
        logger.info("🔥 Deleting DMS endpoints...")
        
        try:
            resources = self.params.get('created_resources', {})
            
            # Delete source endpoint
            source_arn = resources.get('source_endpoint_arn')
            if source_arn:
                try:
                    self.dms_client.delete_endpoint(EndpointArn=source_arn)
                    logger.info(f"  ✅ Deleted source endpoint: {source_arn}")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ResourceNotFoundFault':
                        logger.info(f"  ⚠️  Source endpoint not found: {source_arn}")
                    else:
                        logger.error(f"  ❌ Failed to delete source endpoint: {e}")
            
            # Delete target endpoint  
            target_arn = resources.get('target_endpoint_arn')
            if target_arn:
                try:
                    self.dms_client.delete_endpoint(EndpointArn=target_arn)
                    logger.info(f"  ✅ Deleted target endpoint: {target_arn}")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ResourceNotFoundFault':
                        logger.info(f"  ⚠️  Target endpoint not found: {target_arn}")
                    else:
                        logger.error(f"  ❌ Failed to delete target endpoint: {e}")
                        
        except Exception as e:
            logger.error(f"❌ Error deleting DMS endpoints: {e}")
    
    def delete_dms_replication_instance(self):
        """Delete DMS replication instance"""
        logger.info("🔥 Deleting DMS replication instance...")
        
        try:
            resources = self.params.get('created_resources', {})
            instance_arn = resources.get('replication_instance_arn')
            
            if not instance_arn:
                logger.info("No replication instance ARN found, skipping")
                return
            
            try:
                # Delete the replication instance
                self.dms_client.delete_replication_instance(
                    ReplicationInstanceArn=instance_arn
                )
                logger.info(f"  🕒 Initiated deletion of replication instance: {instance_arn}")
                
                # Wait for deletion to complete (this can take several minutes)
                logger.info("  ⏳ Waiting for replication instance deletion (this may take 5-10 minutes)...")
                waiter = self.dms_client.get_waiter('replication_instance_deleted')
                waiter.wait(
                    Filters=[{'Name': 'replication-instance-arn', 'Values': [instance_arn]}],
                    WaiterConfig={'Delay': 30, 'MaxAttempts': 40}
                )
                logger.info("  ✅ Replication instance deleted successfully")
                
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundFault':
                    logger.info(f"  ⚠️  Replication instance not found: {instance_arn}")
                else:
                    logger.error(f"  ❌ Failed to delete replication instance: {e}")
                    
        except Exception as e:
            logger.error(f"❌ Error deleting replication instance: {e}")
    
    def delete_database(self):
        """Drop the database and table"""
        logger.info("🔥 Dropping source database...")
        
        try:
            resources = self.params.get('created_resources', {})
            rds_endpoint = resources.get('rds_endpoint')
            
            if not rds_endpoint:
                logger.info("No RDS endpoint found, skipping database deletion")
                return
                
            password = os.getenv('AURORA_DB_PASSWORD')
            if not password:
                logger.warning("AURORA_DB_PASSWORD not set, skipping database deletion")
                return
            
            # Connect and drop database
            conn_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={rds_endpoint},1433;"
                f"UID=admin;"
                f"PWD={password};"
                f"TrustServerCertificate=yes;"
            )
            
            conn = pyodbc.connect(conn_string, timeout=30)
            try:
                # Set autocommit mode to avoid transaction issues
                conn.autocommit = True
                cursor = conn.cursor()
                
                # Drop database if it exists
                cursor.execute("IF EXISTS (SELECT name FROM sys.databases WHERE name = 'SRC_DB') DROP DATABASE [SRC_DB]")
                logger.info("  ✅ Dropped database SRC_DB")
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"❌ Error dropping database: {e}")
    
    def delete_rds_instance(self):
        """Delete RDS SQL Server instance"""
        logger.info("🔥 Deleting RDS instance...")
        
        try:
            resources = self.params.get('created_resources', {})
            instance_id = resources.get('rds_instance_id')
            
            if not instance_id:
                logger.info("No RDS instance ID found, skipping")
                return
            
            try:
                self.rds_client.delete_db_instance(
                    DBInstanceIdentifier=instance_id,
                    SkipFinalSnapshot=True,
                    DeleteAutomatedBackups=True
                )
                logger.info(f"  🕒 Initiated RDS instance deletion: {instance_id}")
                logger.info("  ⏳ Waiting for RDS instance deletion (this may take 10-15 minutes)...")
                
                # Wait for deletion
                waiter = self.rds_client.get_waiter('db_instance_deleted')
                waiter.wait(
                    DBInstanceIdentifier=instance_id,
                    WaiterConfig={'Delay': 30, 'MaxAttempts': 60}
                )
                logger.info("  ✅ RDS instance deleted successfully")
                
            except ClientError as e:
                if e.response['Error']['Code'] == 'DBInstanceNotFound':
                    logger.info(f"  ⚠️  RDS instance not found: {instance_id}")
                else:
                    logger.error(f"  ❌ Failed to delete RDS instance: {e}")
                    
        except Exception as e:
            logger.error(f"❌ Error deleting RDS instance: {e}")
    
    def delete_s3_bucket(self):
        """Delete S3 bucket and all contents"""
        logger.info("🔥 Deleting S3 bucket and contents...")
        
        try:
            resources = self.params.get('created_resources', {})
            bucket_name = resources.get('s3_bucket_name')
            
            if not bucket_name:
                logger.info("No S3 bucket name found, skipping")
                return
            
            try:
                # Delete all objects in bucket first
                logger.info(f"  🕒 Emptying bucket: {bucket_name}")
                
                # List and delete all objects
                paginator = self.s3_client.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=bucket_name):
                    if 'Contents' in page:
                        objects = [{'Key': obj['Key']} for obj in page['Contents']]
                        if objects:
                            self.s3_client.delete_objects(
                                Bucket=bucket_name,
                                Delete={'Objects': objects}
                            )
                            logger.info(f"  Deleted {len(objects)} objects")
                
                # Delete all object versions (for versioned buckets)
                paginator = self.s3_client.get_paginator('list_object_versions')
                for page in paginator.paginate(Bucket=bucket_name):
                    versions = []
                    if 'Versions' in page:
                        versions.extend([{'Key': v['Key'], 'VersionId': v['VersionId']} for v in page['Versions']])
                    if 'DeleteMarkers' in page:
                        versions.extend([{'Key': dm['Key'], 'VersionId': dm['VersionId']} for dm in page['DeleteMarkers']])
                    
                    if versions:
                        self.s3_client.delete_objects(
                            Bucket=bucket_name,
                            Delete={'Objects': versions}
                        )
                        logger.info(f"  Deleted {len(versions)} object versions")
                
                # Delete the bucket
                self.s3_client.delete_bucket(Bucket=bucket_name)
                logger.info(f"  ✅ Deleted S3 bucket: {bucket_name}")
                
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchBucket':
                    logger.info(f"  ⚠️  S3 bucket not found: {bucket_name}")
                else:
                    logger.error(f"  ❌ Failed to delete S3 bucket: {e}")
                    
        except Exception as e:
            logger.error(f"❌ Error deleting S3 bucket: {e}")
    
    def delete_iam_roles(self):
        """Delete IAM roles"""
        logger.info("🔥 Deleting IAM roles...")
        
        try:
            resources = self.params.get('created_resources', {})
            
            # Delete custom IAM role
            role_name = resources.get('iam_role_name')
            if role_name:
                try:
                    # Detach all policies first
                    attached_policies = self.iam_client.list_attached_role_policies(RoleName=role_name)
                    for policy in attached_policies['AttachedPolicies']:
                        self.iam_client.detach_role_policy(
                            RoleName=role_name,
                            PolicyArn=policy['PolicyArn']
                        )
                        logger.info(f"  Detached policy: {policy['PolicyName']}")
                    
                    # Delete the role
                    self.iam_client.delete_role(RoleName=role_name)
                    logger.info(f"  ✅ Deleted IAM role: {role_name}")
                    
                except ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchEntity':
                        logger.info(f"  ⚠️  IAM role not found: {role_name}")
                    else:
                        logger.error(f"  ❌ Failed to delete IAM role: {e}")
            
            # Delete DMS VPC role
            try:
                self.iam_client.detach_role_policy(
                    RoleName='dms-vpc-role',
                    PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole'
                )
                self.iam_client.delete_role(RoleName='dms-vpc-role')
                logger.info("  ✅ Deleted DMS VPC role")
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchEntity':
                    logger.info("  ⚠️  DMS VPC role not found")
                else:
                    logger.error(f"  ❌ Failed to delete DMS VPC role: {e}")
                    
        except Exception as e:
            logger.error(f"❌ Error deleting IAM roles: {e}")
    
    def delete_security_group(self):
        """Delete security group (optional - may be used by other resources)"""
        logger.info("🔥 Deleting security group...")
        
        try:
            resources = self.params.get('created_resources', {})
            sg_id = resources.get('security_group_id')
            
            if not sg_id:
                logger.info("No security group ID found, skipping")
                return
            
            try:
                self.ec2_client.delete_security_group(GroupId=sg_id)
                logger.info(f"  ✅ Deleted security group: {sg_id}")
            except ClientError as e:
                if e.response['Error']['Code'] == 'InvalidGroup.NotFound':
                    logger.info(f"  ⚠️  Security group not found: {sg_id}")
                elif e.response['Error']['Code'] == 'DependencyViolation':
                    logger.warning(f"  ⚠️  Security group still in use, skipping: {sg_id}")
                else:
                    logger.error(f"  ❌ Failed to delete security group: {e}")
                    
        except Exception as e:
            logger.error(f"❌ Error deleting security group: {e}")
    
    def save_unwind_progress(self):
        """Save unwind completion status"""
        logger.info("💾 Saving unwind completion status...")
        
        try:
            progress_file = Path(__file__).parent / 'unwind-progress.json'
            progress_data = {
                'unwind_completed': True,
                'completion_time': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
                'original_deployment_params': self.params,
                'status': 'All AWS resources successfully destroyed'
            }
            
            with open(progress_file, 'w') as f:
                json.dump(progress_data, f, indent=2)
            
            logger.info(f"  ✅ Unwind progress saved to unwind-progress.json")
            logger.info("  📁 Original files preserved for reference:")
            logger.info("    - working-parameters.json (original deployment state)")
            logger.info("    - continuity.json (deployment continuity)")
            logger.info("    - unwind.log (detailed unwind log)")
                    
        except Exception as e:
            logger.error(f"❌ Error saving unwind progress: {e}")
    
    def run_unwind(self):
        """Execute the complete unwind process"""
        logger.info("🚨 Starting AWS DMS Infrastructure Unwind")
        logger.info("=" * 50)
        
        # Check if unwind already completed
        progress_file = Path(__file__).parent / 'unwind-progress.json'
        if progress_file.exists():
            try:
                with open(progress_file, 'r') as f:
                    progress = json.load(f)
                if progress.get('unwind_completed'):
                    logger.info("✅ Unwind already completed!")
                    logger.info(f"   Completed at: {progress.get('completion_time', 'Unknown')}")
                    logger.info("   Status: " + progress.get('status', 'Unknown'))
                    return True
            except Exception:
                logger.warning("Could not read unwind-progress.json, continuing with unwind")
        
        if not self.confirm_destruction():
            logger.info("❌ Unwind cancelled by user")
            return False
        
        try:
            # Step 1: CloudWatch alarms (quick)
            self.delete_cloudwatch_alarms()
            
            # Step 2: DMS Migration Task (must be first DMS resource)
            self.delete_dms_migration_task()
            
            # Step 3: DMS Endpoints 
            self.delete_dms_endpoints()
            
            # Step 4: DMS Replication Instance (takes time)
            self.delete_dms_replication_instance()
            
            # Step 5: Database (quick)
            self.delete_database()
            
            # Step 6: RDS Instance (takes most time)
            self.delete_rds_instance()
            
            # Step 7: S3 Bucket and contents
            self.delete_s3_bucket()
            
            # Step 8: IAM Roles
            self.delete_iam_roles()
            
            # Step 9: Security Group (optional)
            self.delete_security_group()
            
            # Step 10: Save completion status
            self.save_unwind_progress()
            
            logger.info("=" * 50)
            logger.info("✅ Infrastructure unwind completed successfully!")
            logger.info("All AWS resources have been destroyed.")
            logger.info("Check unwind-progress.json for completion details.")
            return True
            
        except Exception as e:
            logger.error(f"❌ Unwind failed: {e}")
            return False

def main():
    """Main entry point"""
    unwinder = UnwindManager()
    
    if not unwinder.params:
        logger.error("❌ No parameters found - nothing to unwind")
        return 1
    
    success = unwinder.run_unwind()
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())