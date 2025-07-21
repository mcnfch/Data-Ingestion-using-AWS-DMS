"""
DMS configuration module for AWS DMS Data Ingestion Pipeline.
Handles DMS replication instance, endpoints, and migration tasks.
Follows AWS DMS best practices with proper error handling.
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
            'CsvRowDelimiter': '\\n',
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
    Create DMS migration task following AWS best practices.
    
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
    
    # Test endpoint connections first (AWS DMS requirement)
    logger.info("Testing endpoint connections...")
    try:
        dms_client.test_connection(
            ReplicationInstanceArn=replication_instance_arn,
            EndpointArn=source_endpoint_arn
        )
        dms_client.test_connection(
            ReplicationInstanceArn=replication_instance_arn,
            EndpointArn=target_endpoint_arn
        )
        logger.info("Connection tests initiated, waiting for completion...")
        
        # Wait for connections to succeed (required by AWS DMS)
        max_attempts = 20
        for attempt in range(max_attempts):
            time.sleep(15)
            connections = dms_client.describe_connections(
                Filters=[
                    {'Name': 'replication-instance-arn', 'Values': [replication_instance_arn]}
                ]
            )
            
            successful_count = 0
            for conn in connections['Connections']:
                if conn['Status'] == 'successful':
                    successful_count += 1
            
            if successful_count >= 2:
                logger.info("Both endpoint connections successful")
                break
        else:
            logger.warning("Connection tests may not have completed in time, continuing...")
            
    except ClientError as e:
        logger.warning(f"Connection test error: {e}")
    
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
            task_arn = task['ReplicationTaskArn']
            logger.info(f"Migration task {task_id} already exists")
            return task_arn
                
    except ClientError as e:
        logger.debug(f"Migration task check error: {e}")
    
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
    Start migration task following AWS DMS best practices.
    
    Args:
        task_id (str): Migration task identifier
        region (str): AWS region
    
    Returns:
        bool: True if migration completed successfully
    """
    dms_client = boto3.client('dms', region_name=region)
    
    try:
        # Get current task state
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
        
        logger.info(f"Current migration task status: {task_status}")
        
        # Handle different task states based on AWS DMS best practices
        if task_status == 'running':
            logger.info(f"Migration task {task_id} is already running")
            
        elif task_status == 'ready':
            logger.info(f"Starting migration task: {task_id}")
            dms_client.start_replication_task(
                ReplicationTaskArn=task_arn,
                StartReplicationTaskType='start-replication'
            )
            logger.info("Migration task started successfully")
            
        elif task_status == 'stopped':
            # Check if task completed successfully
            stop_reason = task.get('StopReason', '')
            if any(reason in stop_reason for reason in ['FULL_LOAD_ONLY_FINISHED', 'FULL_LOAD_COMPLETED']):
                logger.info(f"Migration already completed successfully: {stop_reason}")
                return True
            else:
                logger.info(f"Migration task stopped with reason: {stop_reason}, restarting...")
                dms_client.start_replication_task(
                    ReplicationTaskArn=task_arn,
                    StartReplicationTaskType='reload-target'
                )
                logger.info("Migration task restart initiated")
                
        elif task_status == 'failed':
            logger.info("Migration task failed, attempting restart...")
            dms_client.start_replication_task(
                ReplicationTaskArn=task_arn,
                StartReplicationTaskType='reload-target'
            )
            logger.info("Migration task restart initiated")
            
        elif task_status in ['creating', 'modifying']:
            logger.info(f"Migration task is {task_status}, waiting for ready state...")
            # Wait for task to be ready
            max_wait_time = 300  # 5 minutes
            wait_time = 0
            
            while wait_time < max_wait_time:
                time.sleep(15)
                wait_time += 15
                
                response = dms_client.describe_replication_tasks(
                    Filters=[{'Name': 'replication-task-id', 'Values': [task_id]}]
                )
                current_status = response['ReplicationTasks'][0]['Status']
                
                if current_status == 'ready':
                    logger.info("Task is now ready, starting migration...")
                    dms_client.start_replication_task(
                        ReplicationTaskArn=task_arn,
                        StartReplicationTaskType='start-replication'
                    )
                    break
                elif current_status == 'failed':
                    logger.error("Task failed while waiting for ready state")
                    return False
            
            if wait_time >= max_wait_time:
                logger.error("Timeout waiting for task to be ready")
                return False
        else:
            logger.error(f"Unknown task status: {task_status}")
            return False

        # Monitor migration progress
        return monitor_migration_completion(task_id, dms_client)
        
    except ClientError as e:
        logger.error(f"Error starting/monitoring migration: {e}")
        return False

def monitor_migration_completion(task_id, dms_client):
    """
    Monitor migration task until completion following AWS DMS best practices.
    
    Args:
        task_id (str): Migration task identifier
        dms_client: DMS client instance
    
    Returns:
        bool: True if migration completed successfully
    """
    logger.info("Monitoring migration progress...")
    start_time = time.time()
    max_wait_time = 3600  # 1 hour maximum wait time
    check_interval = 30  # Check every 30 seconds
    
    last_logged_progress = -1
    
    while time.time() - start_time < max_wait_time:
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
                logger.error("Migration task disappeared during monitoring")
                return False
                
            task = response['ReplicationTasks'][0]
            status = task['Status']
            stats = task.get('ReplicationTaskStats', {})
            stop_reason = task.get('StopReason', '')
            
            # Log progress if changed
            progress = stats.get('FullLoadProgressPercent', 0)
            if progress != last_logged_progress:
                logger.info(f"Migration progress: {progress}% complete")
                logger.info(f"Tables loaded: {stats.get('TablesLoaded', 0)}, "
                          f"loading: {stats.get('TablesLoading', 0)}, "
                          f"errored: {stats.get('TablesErrored', 0)}")
                last_logged_progress = progress
            
            # Check completion states based on AWS DMS documentation
            if status == 'stopped':
                # Check if task completed successfully
                if any(reason in stop_reason for reason in [
                    'FULL_LOAD_ONLY_FINISHED',
                    'FULL_LOAD_COMPLETED', 
                    'STOPPED_AFTER_FULL_LOAD',
                    'STOPPED_AFTER_CACHED_EVENTS'
                ]):
                    logger.info(f"Migration completed successfully: {stop_reason}")
                    logger.info(f"Final statistics:")
                    logger.info(f"  Progress: {stats.get('FullLoadProgressPercent', 0)}%")
                    logger.info(f"  Tables loaded: {stats.get('TablesLoaded', 0)}")
                    logger.info(f"  Tables errored: {stats.get('TablesErrored', 0)}")
                    return True
                else:
                    logger.error(f"Migration stopped with error: {stop_reason}")
                    return False
                    
            elif status == 'failed':
                logger.error("Migration task failed")
                return False
                
            elif status in ['running', 'starting']:
                # Task is running normally, continue monitoring
                pass
            else:
                logger.warning(f"Unexpected task status during monitoring: {status}")
                
        except ClientError as e:
            logger.error(f"Error during migration monitoring: {e}")
            return False
        
        time.sleep(check_interval)
    
    logger.error("Migration monitoring timed out after 1 hour")
    return False

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