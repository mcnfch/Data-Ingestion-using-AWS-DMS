"""
Infrastructure setup module for AWS DMS Data Ingestion Pipeline.
Handles RDS instance, S3 bucket, and IAM role creation.
"""

import boto3
import json
import time
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

def create_security_group_for_sqlserver(instance_id, region='us-east-1'):
    """
    Create security group for SQL Server RDS instance.
    
    Args:
        instance_id (str): RDS instance identifier (used for naming)
        region (str): AWS region
    
    Returns:
        str: Security group ID
    """
    ec2_client = boto3.client('ec2', region_name=region)
    
    security_group_name = f"{instance_id}-sg"
    
    try:
        # Check if security group already exists
        try:
            response = ec2_client.describe_security_groups(
                Filters=[
                    {'Name': 'group-name', 'Values': [security_group_name]}
                ]
            )
            if response['SecurityGroups']:
                sg_id = response['SecurityGroups'][0]['GroupId']
                logger.info(f"Security group {security_group_name} already exists: {sg_id}")
                return sg_id
        except ClientError:
            pass
        
        # Get default VPC
        vpc_response = ec2_client.describe_vpcs(
            Filters=[{'Name': 'isDefault', 'Values': ['true']}]
        )
        if not vpc_response['Vpcs']:
            raise Exception("No default VPC found")
        
        vpc_id = vpc_response['Vpcs'][0]['VpcId']
        
        # Get current public IP
        import requests
        try:
            current_ip = requests.get('https://checkip.amazonaws.com', timeout=10).text.strip()
            cidr_block = f"{current_ip}/32"
        except Exception:
            # Fallback to a more open rule if IP detection fails
            logger.warning("Could not detect current IP, using 0.0.0.0/0 (not recommended for production)")
            cidr_block = "0.0.0.0/0"
        
        # Create security group
        logger.info(f"Creating security group: {security_group_name}")
        sg_response = ec2_client.create_security_group(
            GroupName=security_group_name,
            Description=f'Security group for SQL Server RDS instance {instance_id}',
            VpcId=vpc_id,
            TagSpecifications=[
                {
                    'ResourceType': 'security-group',
                    'Tags': [
                        {'Key': 'Name', 'Value': security_group_name},
                        {'Key': 'Project', 'Value': 'AWS-DMS-Data-Ingestion'},
                        {'Key': 'Purpose', 'Value': 'SQL-Server-RDS'}
                    ]
                }
            ]
        )
        
        security_group_id = sg_response['GroupId']
        
        # Add inbound rule for SQL Server (port 1433)
        logger.info(f"Adding SQL Server port 1433 access for {current_ip}")
        ec2_client.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 1433,
                    'ToPort': 1433,
                    'IpRanges': [{'CidrIp': cidr_block, 'Description': 'SQL Server access'}]
                }
            ]
        )
        
        logger.info(f"Security group created successfully: {security_group_id}")
        return security_group_id
        
    except ClientError as e:
        logger.error(f"Error creating security group: {e}")
        raise

def ensure_sqlserver_access(security_group_id, region='us-east-1'):
    """
    Ensure security group has SQL Server port 1433 access.
    
    Args:
        security_group_id (str): Security group ID to check/update
        region (str): AWS region
    """
    ec2_client = boto3.client('ec2', region_name=region)
    
    try:
        # Check current rules
        response = ec2_client.describe_security_groups(GroupIds=[security_group_id])
        security_group = response['SecurityGroups'][0]
        
        # Check if port 1433 is already open
        has_sqlserver_access = False
        for rule in security_group['IpPermissions']:
            if (rule.get('IpProtocol') == 'tcp' and 
                rule.get('FromPort') == 1433 and 
                rule.get('ToPort') == 1433):
                has_sqlserver_access = True
                break
        
        if has_sqlserver_access:
            logger.info("Security group already has SQL Server port 1433 access")
            return
        
        # Get current public IP
        import requests
        try:
            current_ip = requests.get('https://checkip.amazonaws.com', timeout=10).text.strip()
            cidr_block = f"{current_ip}/32"
        except Exception:
            logger.warning("Could not detect current IP, using 0.0.0.0/0 (not recommended for production)")
            cidr_block = "0.0.0.0/0"
        
        # Add SQL Server port rule
        logger.info(f"Adding SQL Server port 1433 access for {current_ip} to security group {security_group_id}")
        ec2_client.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 1433,
                    'ToPort': 1433,
                    'IpRanges': [{'CidrIp': cidr_block, 'Description': 'SQL Server access (auto-added)'}]
                }
            ]
        )
        logger.info("SQL Server port 1433 access added successfully")
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            logger.info("SQL Server port 1433 rule already exists")
        else:
            logger.error(f"Error ensuring SQL Server access: {e}")
            raise

def setup_rds(instance_id, password, region='us-east-1'):
    """
    Create RDS SQL Server instance for source data.
    
    Args:
        instance_id (str): RDS instance identifier
        password (str): Master password for RDS instance
        region (str): AWS region
    
    Returns:
        tuple: (RDS endpoint, Security group ID)
    """
    rds_client = boto3.client('rds', region_name=region)
    
    try:
        # Check if instance already exists
        try:
            response = rds_client.describe_db_instances(DBInstanceIdentifier=instance_id)
            instance = response['DBInstances'][0]
            if instance['DBInstanceStatus'] == 'available':
                logger.info(f"RDS instance {instance_id} already exists and is available")
                endpoint = instance['Endpoint']['Address']
                security_group_id = instance['VpcSecurityGroups'][0]['VpcSecurityGroupId'] if instance['VpcSecurityGroups'] else None
                
                # Check if security group has SQL Server access (port 1433)
                if security_group_id:
                    logger.info(f"Checking security group {security_group_id} for SQL Server access...")
                    ensure_sqlserver_access(security_group_id, region)
                
                return endpoint, security_group_id
        except ClientError as e:
            if e.response['Error']['Code'] != 'DBInstanceNotFound':
                raise
        
        # Create security group for SQL Server
        logger.info(f"Creating security group for SQL Server access...")
        security_group_id = create_security_group_for_sqlserver(instance_id, region)
        
        # Create RDS instance with fastest provisioning settings
        logger.info(f"Creating RDS SQL Server instance: {instance_id}")
        response = rds_client.create_db_instance(
            DBInstanceIdentifier=instance_id,
            DBInstanceClass='db.t3.micro',  # Smallest instance for fastest provisioning
            Engine='sqlserver-ex',  # SQL Server Express (fastest provisioning)
            MasterUsername='admin',
            MasterUserPassword=password,
            AllocatedStorage=20,  # Minimum storage
            StorageType='gp2',  # General Purpose SSD (faster than magnetic)
            VpcSecurityGroupIds=[security_group_id],
            PubliclyAccessible=True,
            BackupRetentionPeriod=0,  # No backups for fastest provisioning
            StorageEncrypted=False,  # No encryption for speed
            DeletionProtection=False,
            MultiAZ=False,  # Single AZ for speed
            AutoMinorVersionUpgrade=False,  # Skip version upgrades during creation
            CopyTagsToSnapshot=False,  # Skip snapshot tagging
            EnablePerformanceInsights=False,  # Disable performance insights
            MonitoringInterval=0,  # No enhanced monitoring
            Tags=[
                {
                    'Key': 'Project',
                    'Value': 'AWS-DMS-Data-Ingestion'
                },
                {
                    'Key': 'Purpose',
                    'Value': 'Source-Database'
                }
            ]
        )
        
        # Wait for instance to be available
        logger.info("Waiting for RDS instance to become available...")
        waiter = rds_client.get_waiter('db_instance_available')
        waiter.wait(
            DBInstanceIdentifier=instance_id,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 40
            }
        )
        
        # Get the endpoint
        response = rds_client.describe_db_instances(DBInstanceIdentifier=instance_id)
        instance = response['DBInstances'][0]
        endpoint = instance['Endpoint']['Address']
        
        logger.info(f"RDS instance created successfully: {endpoint}")
        return endpoint, security_group_id
        
    except ClientError as e:
        logger.error(f"Error creating RDS instance: {e}")
        raise

def setup_s3_bucket(bucket_name, folder_name, region='us-east-1'):
    """
    Create S3 bucket and folder for DMS target.
    
    Args:
        bucket_name (str): S3 bucket name
        folder_name (str): Folder name within bucket
        region (str): AWS region
    
    Returns:
        str: S3 bucket ARN
    """
    s3_client = boto3.client('s3', region_name=region)
    
    try:
        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"S3 bucket {bucket_name} already exists")
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                # Create bucket
                logger.info(f"Creating S3 bucket: {bucket_name}")
                if region == 'us-east-1':
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                
                # Add bucket tags
                s3_client.put_bucket_tagging(
                    Bucket=bucket_name,
                    Tagging={
                        'TagSet': [
                            {
                                'Key': 'Project',
                                'Value': 'AWS-DMS-Data-Ingestion'
                            },
                            {
                                'Key': 'Purpose',
                                'Value': 'DMS-Target'
                            }
                        ]
                    }
                )
                logger.info(f"S3 bucket {bucket_name} created successfully")
            else:
                raise
        
        # Create folder (by putting an empty object with trailing slash)
        folder_key = f"{folder_name}/"
        try:
            s3_client.head_object(Bucket=bucket_name, Key=folder_key)
            logger.info(f"Folder {folder_name} already exists in bucket")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.info(f"Creating folder: {folder_name}")
                s3_client.put_object(Bucket=bucket_name, Key=folder_key)
                logger.info(f"Folder {folder_name} created successfully")
            else:
                raise
        
        # Return bucket ARN
        bucket_arn = f"arn:aws:s3:::{bucket_name}"
        return bucket_arn
        
    except ClientError as e:
        logger.error(f"Error setting up S3 bucket: {e}")
        raise

def setup_iam_role(role_name, bucket_name):
    """
    Create IAM role for DMS to access S3.
    
    Args:
        role_name (str): IAM role name
        bucket_name (str): S3 bucket name for policy
    
    Returns:
        str: IAM role ARN
    """
    iam_client = boto3.client('iam')
    
    # Trust policy for DMS service
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "dms.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    # Permission policy for S3 access
    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:DeleteObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}/*"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}"
                ]
            }
        ]
    }
    
    try:
        # Check if role exists
        try:
            response = iam_client.get_role(RoleName=role_name)
            role_arn = response['Role']['Arn']
            logger.info(f"IAM role {role_name} already exists: {role_arn}")
            return role_arn
        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchEntity':
                raise
        
        # Create IAM role
        logger.info(f"Creating IAM role: {role_name}")
        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='IAM role for DMS to access S3 bucket',
            Tags=[
                {
                    'Key': 'Project',
                    'Value': 'AWS-DMS-Data-Ingestion'
                },
                {
                    'Key': 'Purpose',
                    'Value': 'DMS-S3-Access'
                }
            ]
        )
        
        role_arn = response['Role']['Arn']
        
        # Create and attach inline policy
        policy_name = f"{role_name}-s3-policy"
        logger.info(f"Attaching S3 policy to role: {policy_name}")
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
            PolicyDocument=json.dumps(permission_policy)
        )
        
        # Wait for role propagation
        logger.info("Waiting for IAM role to propagate...")
        time.sleep(10)
        
        logger.info(f"IAM role created successfully: {role_arn}")
        return role_arn
        
    except ClientError as e:
        logger.error(f"Error creating IAM role: {e}")
        raise

def setup_dms_vpc_role():
    """
    Create the DMS VPC role required for replication instances.
    
    Returns:
        str: DMS VPC role ARN
    """
    iam_client = boto3.client('iam')
    
    # DMS VPC role trust policy
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "dms.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    try:
        # Check if dms-vpc-role already exists
        try:
            response = iam_client.get_role(RoleName='dms-vpc-role')
            role_arn = response['Role']['Arn']
            logger.info(f"DMS VPC role already exists: {role_arn}")
            return role_arn
        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchEntity':
                raise
        
        # Create dms-vpc-role
        logger.info("Creating dms-vpc-role...")
        response = iam_client.create_role(
            RoleName='dms-vpc-role',
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='Role for DMS to access VPC resources',
            Tags=[
                {
                    'Key': 'Project',
                    'Value': 'AWS-DMS-Data-Ingestion'
                },
                {
                    'Key': 'Purpose',
                    'Value': 'DMS-VPC-Access'
                }
            ]
        )
        
        role_arn = response['Role']['Arn']
        
        # Attach the AWS managed policy for DMS VPC role
        logger.info("Attaching DMS VPC policy...")
        iam_client.attach_role_policy(
            RoleName='dms-vpc-role',
            PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole'
        )
        
        # Wait for role propagation
        logger.info("Waiting for DMS VPC role to propagate...")
        time.sleep(10)
        
        logger.info(f"DMS VPC role created successfully: {role_arn}")
        return role_arn
        
    except ClientError as e:
        logger.error(f"Error creating DMS VPC role: {e}")
        raise

def cleanup_infrastructure(instance_id, bucket_name, role_name, region='us-east-1'):
    """
    Clean up created infrastructure resources.
    
    Args:
        instance_id (str): RDS instance identifier
        bucket_name (str): S3 bucket name
        role_name (str): IAM role name
        region (str): AWS region
    """
    logger.info("Starting infrastructure cleanup...")
    
    # Cleanup RDS instance
    try:
        rds_client = boto3.client('rds', region_name=region)
        rds_client.delete_db_instance(
            DBInstanceIdentifier=instance_id,
            SkipFinalSnapshot=True,
            DeleteAutomatedBackups=True
        )
        logger.info(f"RDS instance {instance_id} deletion initiated")
    except ClientError as e:
        if e.response['Error']['Code'] != 'DBInstanceNotFound':
            logger.error(f"Error deleting RDS instance: {e}")
    
    # Cleanup S3 bucket (empty first, then delete)
    try:
        s3_client = boto3.client('s3', region_name=region)
        
        # Empty bucket
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' in page:
                objects = [{'Key': obj['Key']} for obj in page['Contents']]
                s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': objects}
                )
        
        # Delete bucket
        s3_client.delete_bucket(Bucket=bucket_name)
        logger.info(f"S3 bucket {bucket_name} deleted")
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchBucket':
            logger.error(f"Error deleting S3 bucket: {e}")
    
    # Cleanup IAM role
    try:
        iam_client = boto3.client('iam')
        
        # Delete inline policies
        try:
            policies = iam_client.list_role_policies(RoleName=role_name)
            for policy_name in policies['PolicyNames']:
                iam_client.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
        except ClientError:
            pass
        
        # Delete role
        iam_client.delete_role(RoleName=role_name)
        logger.info(f"IAM role {role_name} deleted")
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchEntity':
            logger.error(f"Error deleting IAM role: {e}")
    
    logger.info("Infrastructure cleanup completed")

if __name__ == "__main__":
    # Test infrastructure setup
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    config = {
        'instance_id': 'test-dms-source',
        'password': os.getenv('AURORA_DB_PASSWORD'),
        'bucket_name': f"test-dms-bucket-{os.getenv('AWS_ACCOUNT_ID')}",
        'folder_name': 'test-data',
        'role_name': 'test-dms-role',
        'region': 'us-east-1'
    }
    
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Setup infrastructure
        rds_endpoint = setup_rds(config['instance_id'], config['password'], config['region'])
        print(f"RDS Endpoint: {rds_endpoint}")
        
        bucket_arn = setup_s3_bucket(config['bucket_name'], config['folder_name'], config['region'])
        print(f"S3 Bucket ARN: {bucket_arn}")
        
        role_arn = setup_iam_role(config['role_name'], config['bucket_name'])
        print(f"IAM Role ARN: {role_arn}")
        
    except Exception as e:
        print(f"Error: {e}")