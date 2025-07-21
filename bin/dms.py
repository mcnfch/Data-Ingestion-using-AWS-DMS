"""
DMS configuration module for AWS DMS Data Ingestion Pipeline.
Handles DMS replication instance, endpoints, and migration tasks.
"""

import boto3
import json
import time
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

def create_replication_instance(instance_id, instance_class='dms.t3.micro', region='us-east-1'):
    """
    Create DMS replication instance.
    
    Args:
        instance_id (str): Replication instance identifier
        instance_class (str): Instance class for replication instance
        region (str): AWS region
    
    Returns:
        str: Replication instance ARN
    """
    dms_client = boto3.client('dms', region_name=region)
    
    try:
        # Check if replication instance already exists
        try:
            response = dms_client.describe_replication_instances(
                Filters=[
                    {
                        'Name': 'replication-instance-id',
                        'Values': [instance_id]
                    }
                ]
            )
            if response['ReplicationInstances']:
                instance = response['ReplicationInstances'][0]
                if instance['ReplicationInstanceStatus'] == 'available':
                    logger.info(f"Replication instance {instance_id} already exists and is available")
                    return instance['ReplicationInstanceArn']
        except ClientError as e:
            logger.debug(f"Replication instance check error: {e}")
        
        # Create replication instance
        logger.info(f"Creating DMS replication instance: {instance_id}")
        response = dms_client.create_replication_instance(
            ReplicationInstanceIdentifier=instance_id,
            ReplicationInstanceClass=instance_class,
            AllocatedStorage=20,
            VpcSecurityGroupIds=[],
            PubliclyAccessible=True,
            MultiAZ=False,
            Tags=[
                {
                    'Key': 'Project',
                    'Value': 'AWS-DMS-Data-Ingestion'
                },
                {
                    'Key': 'Purpose',
                    'Value': 'Data-Replication'
                }
            ]
        )
        
        replication_instance_arn = response['ReplicationInstance']['ReplicationInstanceArn']
        
        # Wait for replication instance to be available
        logger.info("Waiting for replication instance to become available...")
        waiter = dms_client.get_waiter('replication_instance_available')
        waiter.wait(
            Filters=[
                {
                    'Name': 'replication-instance-id',
                    'Values': [instance_id]
                }
            ],
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 40
            }
        )
        
        logger.info(f"Replication instance created successfully: {replication_instance_arn}")
        return replication_instance_arn
        
    except ClientError as e:
        logger.error(f"Error creating replication instance: {e}")
        raise

def create_source_endpoint(endpoint_id, server_name, password, database_name, 
                          replication_instance_arn, username='admin', port=1433, region='us-east-1'):
    """
    Create DMS source endpoint for SQL Server.
    
    Args:
        endpoint_id (str): Endpoint identifier
        server_name (str): SQL Server hostname/endpoint
        password (str): Database password
        database_name (str): Database name
        replication_instance_arn (str): DMS replication instance ARN
        username (str): Database username
        port (int): Database port
        region (str): AWS region
    
    Returns:
        str: Source endpoint ARN
    """
    dms_client = boto3.client('dms', region_name=region)
    
    try:
        # Check if endpoint already exists
        try:
            response = dms_client.describe_endpoints(
                Filters=[
                    {
                        'Name': 'endpoint-id',
                        'Values': [endpoint_id]
                    }
                ]
            )
            if response['Endpoints']:
                endpoint = response['Endpoints'][0]
                logger.info(f"Source endpoint {endpoint_id} already exists")
                return endpoint['EndpointArn']
        except ClientError as e:
            logger.debug(f"Source endpoint check error: {e}")
        
        # Create source endpoint
        logger.info(f"Creating DMS source endpoint: {endpoint_id}")
        response = dms_client.create_endpoint(
            EndpointIdentifier=endpoint_id,
            EndpointType='source',
            EngineName='sqlserver',
            Username=username,
            Password=password,
            ServerName=server_name,
            Port=port,
            DatabaseName=database_name,
            Tags=[
                {
                    'Key': 'Project',
                    'Value': 'AWS-DMS-Data-Ingestion'
                },
                {
                    'Key': 'Purpose',
                    'Value': 'Source-Endpoint'
                }
            ]
        )
        
        endpoint_arn = response['Endpoint']['EndpointArn']
        
        # Test connection
        logger.info("Testing source endpoint connection...")
        test_response = dms_client.test_connection(
            ReplicationInstanceArn=replication_instance_arn,
            EndpointArn=endpoint_arn
        )
        
        logger.info(f"Source endpoint created successfully: {endpoint_arn}")
        return endpoint_arn
        
    except ClientError as e:
        logger.error(f"Error creating source endpoint: {e}")
        raise

def create_target_endpoint(endpoint_id, role_arn, bucket_name, bucket_folder, region='us-east-1'):
    """
    Create DMS target endpoint for S3.
    
    Args:
        endpoint_id (str): Endpoint identifier
        role_arn (str): IAM role ARN for S3 access
        bucket_name (str): S3 bucket name
        bucket_folder (str): S3 bucket folder
        region (str): AWS region
    
    Returns:
        str: Target endpoint ARN
    """
    dms_client = boto3.client('dms', region_name=region)
    
    try:
        # Check if endpoint already exists
        try:
            response = dms_client.describe_endpoints(
                Filters=[
                    {
                        'Name': 'endpoint-id',
                        'Values': [endpoint_id]
                    }
                ]
            )
            if response['Endpoints']:
                endpoint = response['Endpoints'][0]
                logger.info(f"Target endpoint {endpoint_id} already exists")
                return endpoint['EndpointArn']
        except ClientError as e:
            logger.debug(f"Target endpoint check error: {e}")
        
        # S3 settings
        s3_settings = {
            'ServiceAccessRoleArn': role_arn,
            'BucketName': bucket_name,
            'BucketFolder': bucket_folder,
            'CompressionType': 'NONE',
            'CsvDelimiter': ',',
            'CsvRowDelimiter': '\n',
            'DataFormat': 'csv',
            'IncludeOpForFullLoad': True,
            'TimestampColumnName': 'dms_timestamp'
        }
        
        # Create target endpoint
        logger.info(f"Creating DMS target endpoint: {endpoint_id}")
        response = dms_client.create_endpoint(
            EndpointIdentifier=endpoint_id,
            EndpointType='target',
            EngineName='s3',
            S3Settings=s3_settings,
            Tags=[
                {
                    'Key': 'Project',
                    'Value': 'AWS-DMS-Data-Ingestion'
                },
                {
                    'Key': 'Purpose',
                    'Value': 'Target-Endpoint'
                }
            ]
        )
        
        endpoint_arn = response['Endpoint']['EndpointArn']
        
        logger.info(f"Target endpoint created successfully: {endpoint_arn}")
        return endpoint_arn
        
    except ClientError as e:
        logger.error(f"Error creating target endpoint: {e}")
        raise

def create_migration_task(task_id, replication_instance_arn, source_endpoint_arn, 
                         target_endpoint_arn, table_name, region='us-east-1'):
    """
    Create DMS migration task.
    
    Args:
        task_id (str): Migration task identifier
        replication_instance_arn (str): Replication instance ARN
        source_endpoint_arn (str): Source endpoint ARN
        target_endpoint_arn (str): Target endpoint ARN
        table_name (str): Table name to migrate
        region (str): AWS region
    
    Returns:
        str: Migration task ARN
    """
    dms_client = boto3.client('dms', region_name=region)
    
    # Table mappings configuration
    table_mappings = {
        "rules": [
            {
                "rule-type": "selection",
                "rule-id": "1",
                "rule-name": "1",
                "object-locator": {
                    "schema-name": "dbo",
                    "table-name": table_name
                },
                "rule-action": "include"
            }
        ]
    }
    
    try:
        # Check if task already exists
        try:
            response = dms_client.describe_replication_tasks(
                Filters=[
                    {
                        'Name': 'replication-task-id',
                        'Values': [task_id]
                    }
                ]
            )
            if response['ReplicationTasks']:
                task = response['ReplicationTasks'][0]
                logger.info(f"Migration task {task_id} already exists")
                return task['ReplicationTaskArn']
        except ClientError as e:
            logger.debug(f"Migration task check error: {e}")
        
        # Create migration task
        logger.info(f"Creating DMS migration task: {task_id}")
        response = dms_client.create_replication_task(
            ReplicationTaskIdentifier=task_id,
            SourceEndpointArn=source_endpoint_arn,
            TargetEndpointArn=target_endpoint_arn,
            ReplicationInstanceArn=replication_instance_arn,
            MigrationType='full-load',
            TableMappings=json.dumps(table_mappings),
            Tags=[
                {
                    'Key': 'Project',
                    'Value': 'AWS-DMS-Data-Ingestion'
                },
                {
                    'Key': 'Purpose',
                    'Value': 'Data-Migration'
                }
            ]
        )
        
        task_arn = response['ReplicationTask']['ReplicationTaskArn']
        
        logger.info(f"Migration task created successfully: {task_arn}")
        return task_arn
        
    except ClientError as e:
        logger.error(f"Error creating migration task: {e}")
        raise

def start_migration(task_id, region='us-east-1'):
    """
    Start migration task and wait for completion.
    
    Args:
        task_id (str): Migration task identifier
        region (str): AWS region
    
    Returns:
        bool: True if migration completed successfully
    """
    dms_client = boto3.client('dms', region_name=region)
    
    try:
        # Get task details
        response = dms_client.describe_replication_tasks(
            Filters=[
                {
                    'Name': 'replication-task-id',
                    'Values': [task_id]
                }
            ]
        )
        
        if not response['ReplicationTasks']:
            raise ValueError(f"Migration task {task_id} not found")
        
        task = response['ReplicationTasks'][0]
        task_arn = task['ReplicationTaskArn']
        task_status = task['Status']
        
        if task_status == 'running':
            logger.info(f"Migration task {task_id} is already running")
        elif task_status in ['ready', 'stopped']:
            # Start the migration task
            logger.info(f"Starting migration task: {task_id}")
            dms_client.start_replication_task(
                ReplicationTaskArn=task_arn,
                StartReplicationTaskType='start-replication'
            )
        elif task_status == 'failed':
            logger.warning(f"Migration task {task_id} is in failed state, restarting...")
            dms_client.start_replication_task(
                ReplicationTaskArn=task_arn,
                StartReplicationTaskType='start-replication'
            )
        
        # Monitor task progress
        logger.info("Monitoring migration progress...")
        start_time = time.time()
        max_wait_time = 3600  # 1 hour maximum wait time
        
        while time.time() - start_time < max_wait_time:
            response = dms_client.describe_replication_tasks(
                Filters=[
                    {
                        'Name': 'replication-task-id',
                        'Values': [task_id]
                    }
                ]
            )
            
            task = response['ReplicationTasks'][0]
            status = task['Status']
            
            if status == 'stopped':
                # Check stop reason
                stop_reason = task.get('StopReason', 'Unknown')
                if 'Full load completed' in stop_reason or 'FULL_LOAD_COMPLETED' in stop_reason:
                    logger.info(f"Migration completed successfully: {stop_reason}")
                    
                    # Get task statistics
                    stats = task.get('ReplicationTaskStats', {})
                    logger.info(f"Migration statistics:")
                    logger.info(f"  - Full load rows: {stats.get('FullLoadRows', 0)}")
                    logger.info(f"  - Tables loaded: {stats.get('TablesLoaded', 0)}")
                    logger.info(f"  - Tables loading: {stats.get('TablesLoading', 0)}")
                    logger.info(f"  - Tables errored: {stats.get('TablesErrored', 0)}")
                    
                    return True
                else:
                    logger.error(f"Migration stopped with error: {stop_reason}")
                    return False
                    
            elif status == 'failed':
                logger.error("Migration task failed")
                return False
                
            elif status == 'running':
                # Log progress
                stats = task.get('ReplicationTaskStats', {})
                progress = f"Tables loading: {stats.get('TablesLoading', 0)}, loaded: {stats.get('TablesLoaded', 0)}"
                logger.info(f"Migration in progress - {progress}")
                
            time.sleep(30)  # Wait 30 seconds before next check
        
        logger.error("Migration task timed out")
        return False
        
    except ClientError as e:
        logger.error(f"Error starting/monitoring migration: {e}")
        raise

def get_migration_status(task_id, region='us-east-1'):
    """
    Get detailed migration task status and statistics.
    
    Args:
        task_id (str): Migration task identifier
        region (str): AWS region
    
    Returns:
        dict: Migration status and statistics
    """
    dms_client = boto3.client('dms', region_name=region)
    
    try:
        response = dms_client.describe_replication_tasks(
            Filters=[
                {
                    'Name': 'replication-task-id',
                    'Values': [task_id]
                }
            ]
        )
        
        if not response['ReplicationTasks']:
            return {'error': f'Task {task_id} not found'}
        
        task = response['ReplicationTasks'][0]
        
        status_info = {
            'task_id': task_id,
            'status': task['Status'],
            'creation_date': task.get('ReplicationTaskCreationDate'),
            'start_date': task.get('ReplicationTaskStartDate'),
            'stop_reason': task.get('StopReason'),
            'statistics': task.get('ReplicationTaskStats', {}),
            'settings': {
                'migration_type': task.get('MigrationType'),
                'source_endpoint': task.get('SourceEndpointArn'),
                'target_endpoint': task.get('TargetEndpointArn')
            }
        }
        
        return status_info
        
    except ClientError as e:
        logger.error(f"Error getting migration status: {e}")
        return {'error': str(e)}

def cleanup_dms_resources(replication_instance_id, source_endpoint_id, 
                         target_endpoint_id, task_id, region='us-east-1'):
    """
    Clean up DMS resources.
    
    Args:
        replication_instance_id (str): Replication instance identifier
        source_endpoint_id (str): Source endpoint identifier
        target_endpoint_id (str): Target endpoint identifier
        task_id (str): Migration task identifier
        region (str): AWS region
    """
    dms_client = boto3.client('dms', region_name=region)
    
    logger.info("Starting DMS resources cleanup...")
    
    # Stop and delete migration task
    try:
        response = dms_client.describe_replication_tasks(
            Filters=[{'Name': 'replication-task-id', 'Values': [task_id]}]
        )
        if response['ReplicationTasks']:
            task_arn = response['ReplicationTasks'][0]['ReplicationTaskArn']
            try:
                dms_client.stop_replication_task(ReplicationTaskArn=task_arn)
                logger.info(f"Stopped migration task: {task_id}")
                time.sleep(30)  # Wait for task to stop
            except ClientError:
                pass
            
            dms_client.delete_replication_task(ReplicationTaskArn=task_arn)
            logger.info(f"Deleted migration task: {task_id}")
    except ClientError as e:
        logger.warning(f"Error deleting migration task: {e}")
    
    # Delete endpoints
    for endpoint_id in [source_endpoint_id, target_endpoint_id]:
        try:
            response = dms_client.describe_endpoints(
                Filters=[{'Name': 'endpoint-id', 'Values': [endpoint_id]}]
            )
            if response['Endpoints']:
                endpoint_arn = response['Endpoints'][0]['EndpointArn']
                dms_client.delete_endpoint(EndpointArn=endpoint_arn)
                logger.info(f"Deleted endpoint: {endpoint_id}")
        except ClientError as e:
            logger.warning(f"Error deleting endpoint {endpoint_id}: {e}")
    
    # Delete replication instance
    try:
        response = dms_client.describe_replication_instances(
            Filters=[{'Name': 'replication-instance-id', 'Values': [replication_instance_id]}]
        )
        if response['ReplicationInstances']:
            instance_arn = response['ReplicationInstances'][0]['ReplicationInstanceArn']
            dms_client.delete_replication_instance(ReplicationInstanceArn=instance_arn)
            logger.info(f"Deleted replication instance: {replication_instance_id}")
    except ClientError as e:
        logger.warning(f"Error deleting replication instance: {e}")
    
    logger.info("DMS resources cleanup completed")

if __name__ == "__main__":
    # Test DMS setup
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    config = {
        'region': 'us-east-1',
        'account_id': os.getenv('AWS_ACCOUNT_ID'),
        'replication_instance_id': 'test-dms-replication',
        'source_endpoint_id': 'test-source-endpoint',
        'target_endpoint_id': 'test-target-endpoint',
        'migration_task_id': 'test-migration-task',
        'rds_endpoint': 'test-endpoint.region.rds.amazonaws.com',
        'password': os.getenv('AURORA_DB_PASSWORD'),
        'bucket_name': f"test-dms-bucket-{os.getenv('AWS_ACCOUNT_ID')}",
        'role_arn': f"arn:aws:iam::{os.getenv('AWS_ACCOUNT_ID')}:role/test-dms-role"
    }
    
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Create replication instance
        instance_arn = create_replication_instance(
            config['replication_instance_id'], 
            region=config['region']
        )
        print(f"Replication Instance ARN: {instance_arn}")
        
    except Exception as e:
        print(f"Error: {e}")