"""
Validation and monitoring module for AWS DMS Data Ingestion Pipeline.
Handles data validation and CloudWatch monitoring setup.
"""

import boto3
import pyodbc
import pandas as pd
import logging
import json
from botocore.exceptions import ClientError
from io import StringIO

logger = logging.getLogger(__name__)

def validate_s3_data(bucket_name, bucket_folder, rds_endpoint, password, 
                    db_name='SRC_DB', table_name='raw_src', username='admin', port=1433):
    """
    Validate migrated data in S3 against source database.
    
    Args:
        bucket_name (str): S3 bucket name
        bucket_folder (str): S3 bucket folder
        rds_endpoint (str): RDS endpoint
        password (str): Database password
        db_name (str): Database name
        table_name (str): Table name
        username (str): Database username
        port (int): Database port
    
    Returns:
        bool: True if validation passes
    """
    s3_client = boto3.client('s3')
    
    try:
        # Get source database row count
        logger.info("Getting source database row count...")
        source_count = get_source_row_count(rds_endpoint, password, db_name, table_name, username, port)
        logger.info(f"Source database row count: {source_count}")
        
        # Get S3 data files
        logger.info(f"Checking S3 bucket: {bucket_name}/{bucket_folder}")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"{bucket_folder}/"
        )
        
        if 'Contents' not in response:
            logger.error("No files found in S3 bucket")
            return False
        
        # Find data files (exclude folder markers)
        data_files = [obj for obj in response['Contents'] if not obj['Key'].endswith('/')]
        logger.info(f"Found {len(data_files)} files in S3")
        
        if not data_files:
            logger.error("No data files found in S3")
            return False
        
        # Count total rows in S3 files
        total_s3_rows = 0
        sample_data = []
        
        for file_obj in data_files:
            file_key = file_obj['Key']
            logger.info(f"Processing file: {file_key}")
            
            try:
                # Get file content
                file_response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
                file_content = file_response['Body'].read().decode('utf-8')
                
                # Count rows (DMS CSV has no header, just data rows)
                rows = file_content.strip().split('\n')
                if rows and rows[0]:  # Check if file has content
                    file_row_count = len(rows)  # DMS CSV files have no header row
                    total_s3_rows += file_row_count
                    logger.info(f"File {file_key}: {file_row_count} rows")
                    
                    # Store sample data from first file
                    if not sample_data and len(rows) > 1:
                        sample_data = rows[:6]  # Header + 5 data rows
                
            except Exception as e:
                logger.error(f"Error processing file {file_key}: {e}")
                continue
        
        logger.info(f"Total S3 rows: {total_s3_rows}")
        
        # Compare counts
        if source_count == total_s3_rows:
            logger.info("✅ Data validation PASSED - Row counts match")
            
            # Display sample data
            if sample_data:
                logger.info("Sample migrated data:")
                for i, row in enumerate(sample_data):
                    logger.info(f"  {i}: {row[:100]}...")  # Truncate long rows
            
            return True
        else:
            logger.error(f"❌ Data validation FAILED - Source: {source_count}, S3: {total_s3_rows}")
            return False
            
    except Exception as e:
        logger.error(f"Error validating S3 data: {e}")
        return False

def get_source_row_count(endpoint, password, db_name, table_name, username='admin', port=1433):
    """
    Get row count from source database.
    
    Args:
        endpoint (str): RDS endpoint
        password (str): Database password
        db_name (str): Database name
        table_name (str): Table name
        username (str): Database username
        port (int): Database port
    
    Returns:
        int: Row count
    """
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={endpoint},{port};"
        f"DATABASE={db_name};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
    )
    
    try:
        with pyodbc.connect(connection_string, timeout=30) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
            row_count = cursor.fetchone()[0]
            return row_count
            
    except Exception as e:
        logger.error(f"Error getting source row count: {e}")
        raise

def setup_monitoring(task_arn, region='us-east-1', notification_email=None):
    """
    Setup CloudWatch monitoring and alarms for DMS task.
    
    Args:
        task_arn (str): DMS task ARN
        region (str): AWS region
        notification_email (str): Email for notifications (optional)
    
    Returns:
        dict: Created monitoring resources
    """
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    sns = boto3.client('sns', region_name=region)
    
    # Extract task ID from ARN
    task_id = task_arn.split(':')[-1]
    
    monitoring_resources = {
        'alarms': [],
        'sns_topic_arn': None
    }
    
    try:
        # Create SNS topic for notifications (if email provided)
        topic_arn = None
        if notification_email:
            logger.info("Creating SNS topic for notifications...")
            topic_response = sns.create_topic(Name=f'dms-alerts-{task_id}')
            topic_arn = topic_response['TopicArn']
            
            # Subscribe email to topic
            sns.subscribe(
                TopicArn=topic_arn,
                Protocol='email',
                Endpoint=notification_email
            )
            
            monitoring_resources['sns_topic_arn'] = topic_arn
            logger.info(f"SNS topic created: {topic_arn}")
        
        # Define CloudWatch alarms
        alarms_config = [
            {
                'name': f'DMS-Task-Failure-{task_id}',
                'description': 'Alert when DMS task fails',
                'metric_name': 'ReplicationTaskStatus',
                'statistic': 'Maximum',
                'comparison_operator': 'GreaterThanThreshold',
                'threshold': 0,
                'evaluation_periods': 1,
                'period': 300,
                'treat_missing_data': 'breaching'
            },
            {
                'name': f'DMS-High-Replication-Lag-{task_id}',
                'description': 'Alert when replication lag is high',
                'metric_name': 'CDCLatencyTarget',
                'statistic': 'Average',
                'comparison_operator': 'GreaterThanThreshold',
                'threshold': 300,  # 5 minutes
                'evaluation_periods': 2,
                'period': 300,
                'treat_missing_data': 'notBreaching'
            },
            {
                'name': f'DMS-Low-Throughput-{task_id}',
                'description': 'Alert when throughput is low',
                'metric_name': 'CDCThroughputRowsTarget',
                'statistic': 'Average',
                'comparison_operator': 'LessThanThreshold',
                'threshold': 10,
                'evaluation_periods': 3,
                'period': 300,
                'treat_missing_data': 'notBreaching'
            }
        ]
        
        # Create CloudWatch alarms
        for alarm_config in alarms_config:
            logger.info(f"Creating CloudWatch alarm: {alarm_config['name']}")
            
            alarm_actions = [topic_arn] if topic_arn else []
            
            cloudwatch.put_metric_alarm(
                AlarmName=alarm_config['name'],
                ComparisonOperator=alarm_config['comparison_operator'],
                EvaluationPeriods=alarm_config['evaluation_periods'],
                MetricName=alarm_config['metric_name'],
                Namespace='AWS/DMS',
                Period=alarm_config['period'],
                Statistic=alarm_config['statistic'],
                Threshold=alarm_config['threshold'],
                ActionsEnabled=True,
                AlarmActions=alarm_actions,
                AlarmDescription=alarm_config['description'],
                Dimensions=[
                    {
                        'Name': 'ReplicationTaskArn',
                        'Value': task_arn
                    }
                ],
                TreatMissingData=alarm_config['treat_missing_data'],
                Tags=[
                    {
                        'Key': 'Project',
                        'Value': 'AWS-DMS-Data-Ingestion'
                    },
                    {
                        'Key': 'Purpose',
                        'Value': 'Monitoring'
                    }
                ]
            )
            
            monitoring_resources['alarms'].append(alarm_config['name'])
        
        logger.info(f"Created {len(alarms_config)} CloudWatch alarms")
        return monitoring_resources
        
    except ClientError as e:
        logger.error(f"Error setting up monitoring: {e}")
        raise

def create_custom_dashboard(task_arn, region='us-east-1'):
    """
    Create CloudWatch dashboard for DMS monitoring.
    
    Args:
        task_arn (str): DMS task ARN
        region (str): AWS region
    
    Returns:
        str: Dashboard URL
    """
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    
    task_id = task_arn.split(':')[-1]
    dashboard_name = f'DMS-Migration-{task_id}'
    
    # Dashboard body configuration
    dashboard_body = {
        "widgets": [
            {
                "type": "metric",
                "x": 0,
                "y": 0,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["AWS/DMS", "FullLoadThroughputBandwidthSource", "ReplicationTaskArn", task_arn],
                        [".", "FullLoadThroughputRowsSource", ".", "."],
                        [".", "CDCThroughputBandwidthTarget", ".", "."],
                        [".", "CDCThroughputRowsTarget", ".", "."]
                    ],
                    "period": 300,
                    "stat": "Average",
                    "region": region,
                    "title": "DMS Throughput Metrics"
                }
            },
            {
                "type": "metric",
                "x": 12,
                "y": 0,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["AWS/DMS", "CDCLatencySource", "ReplicationTaskArn", task_arn],
                        [".", "CDCLatencyTarget", ".", "."]
                    ],
                    "period": 300,
                    "stat": "Average",
                    "region": region,
                    "title": "DMS Latency Metrics"
                }
            },
            {
                "type": "metric",
                "x": 0,
                "y": 6,
                "width": 24,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["AWS/DMS", "FreeMemory", "ReplicationInstanceArn", task_arn.replace(":task:", ":rep:")],
                        [".", "CPUUtilization", ".", "."],
                        [".", "FreeStorageSpace", ".", "."]
                    ],
                    "period": 300,
                    "stat": "Average",
                    "region": region,
                    "title": "Replication Instance Metrics"
                }
            }
        ]
    }
    
    try:
        logger.info(f"Creating CloudWatch dashboard: {dashboard_name}")
        
        cloudwatch.put_dashboard(
            DashboardName=dashboard_name,
            DashboardBody=json.dumps(dashboard_body)
        )
        
        dashboard_url = f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}#dashboards:name={dashboard_name}"
        logger.info(f"Dashboard created: {dashboard_url}")
        
        return dashboard_url
        
    except ClientError as e:
        logger.error(f"Error creating dashboard: {e}")
        raise

def generate_validation_report(bucket_name, bucket_folder, rds_endpoint, password, 
                             db_name='SRC_DB', table_name='raw_src'):
    """
    Generate a comprehensive validation report.
    
    Args:
        bucket_name (str): S3 bucket name
        bucket_folder (str): S3 bucket folder
        rds_endpoint (str): RDS endpoint
        password (str): Database password
        db_name (str): Database name
        table_name (str): Table name
    
    Returns:
        dict: Validation report
    """
    report = {
        'timestamp': pd.Timestamp.now().isoformat(),
        'source_database': {
            'endpoint': rds_endpoint,
            'database': db_name,
            'table': table_name
        },
        'target_storage': {
            'bucket': bucket_name,
            'folder': bucket_folder
        },
        'validation_results': {}
    }
    
    try:
        # Source validation
        logger.info("Analyzing source database...")
        source_count = get_source_row_count(rds_endpoint, password, db_name, table_name)
        report['source_database']['row_count'] = source_count
        
        # S3 validation
        logger.info("Analyzing S3 data...")
        s3_client = boto3.client('s3')
        
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"{bucket_folder}/"
        )
        
        files = [obj for obj in response.get('Contents', []) if not obj['Key'].endswith('/')]
        total_size = sum(obj['Size'] for obj in files)
        
        # Count S3 rows
        total_s3_rows = 0
        for file_obj in files:
            try:
                file_response = s3_client.get_object(Bucket=bucket_name, Key=file_obj['Key'])
                content = file_response['Body'].read().decode('utf-8')
                rows = content.strip().split('\n')
                if rows and rows[0]:
                    file_row_count = len(rows) - 1 if ',' in rows[0] else len(rows)
                    total_s3_rows += file_row_count
            except Exception as e:
                logger.warning(f"Error processing file {file_obj['Key']}: {e}")
        
        report['target_storage'].update({
            'file_count': len(files),
            'total_size_bytes': total_size,
            'row_count': total_s3_rows
        })
        
        # Validation results
        validation_passed = source_count == total_s3_rows
        report['validation_results'] = {
            'status': 'PASSED' if validation_passed else 'FAILED',
            'row_count_match': validation_passed,
            'source_rows': source_count,
            'target_rows': total_s3_rows,
            'row_difference': abs(source_count - total_s3_rows) if not validation_passed else 0,
            'data_completeness_percentage': (total_s3_rows / source_count * 100) if source_count > 0 else 0
        }
        
        logger.info(f"Validation report generated: {report['validation_results']['status']}")
        return report
        
    except Exception as e:
        logger.error(f"Error generating validation report: {e}")
        report['validation_results'] = {
            'status': 'ERROR',
            'error_message': str(e)
        }
        return report

def cleanup_monitoring_resources(task_id, region='us-east-1'):
    """
    Clean up monitoring resources (alarms, topics, dashboard).
    
    Args:
        task_id (str): DMS task identifier
        region (str): AWS region
    """
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    sns = boto3.client('sns', region_name=region)
    
    logger.info("Cleaning up monitoring resources...")
    
    # Delete CloudWatch alarms
    alarm_names = [
        f'DMS-Task-Failure-{task_id}',
        f'DMS-High-Replication-Lag-{task_id}',
        f'DMS-Low-Throughput-{task_id}'
    ]
    
    try:
        cloudwatch.delete_alarms(AlarmNames=alarm_names)
        logger.info(f"Deleted {len(alarm_names)} CloudWatch alarms")
    except ClientError as e:
        logger.warning(f"Error deleting alarms: {e}")
    
    # Delete dashboard
    try:
        dashboard_name = f'DMS-Migration-{task_id}'
        cloudwatch.delete_dashboards(DashboardNames=[dashboard_name])
        logger.info(f"Deleted dashboard: {dashboard_name}")
    except ClientError as e:
        logger.warning(f"Error deleting dashboard: {e}")
    
    # Delete SNS topic
    try:
        topic_name = f'dms-alerts-{task_id}'
        topics_response = sns.list_topics()
        for topic in topics_response['Topics']:
            if topic_name in topic['TopicArn']:
                sns.delete_topic(TopicArn=topic['TopicArn'])
                logger.info(f"Deleted SNS topic: {topic['TopicArn']}")
                break
    except ClientError as e:
        logger.warning(f"Error deleting SNS topic: {e}")
    
    logger.info("Monitoring cleanup completed")

if __name__ == "__main__":
    # Test validation and monitoring
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    config = {
        'bucket_name': f"test-dms-bucket-{os.getenv('AWS_ACCOUNT_ID')}",
        'bucket_folder': 'test-data',
        'rds_endpoint': 'test-endpoint.region.rds.amazonaws.com',
        'password': os.getenv('AURORA_DB_PASSWORD'),
        'task_arn': f"arn:aws:dms:us-east-1:{os.getenv('AWS_ACCOUNT_ID')}:task:test-task",
        'region': 'us-east-1'
    }
    
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Test validation
        validation_result = validate_s3_data(
            config['bucket_name'],
            config['bucket_folder'],
            config['rds_endpoint'],
            config['password']
        )
        
        print(f"Validation Result: {validation_result}")
        
    except Exception as e:
        print(f"Error: {e}")