#!/usr/bin/env python3
"""
Script to delete all general purpose S3 buckets.
Excludes system buckets like CDK, Glue, Athena.
"""

import boto3
import sys
from botocore.exceptions import ClientError

def delete_all_bucket_contents(bucket_name, s3_client):
    """Delete all objects and versions in a bucket."""
    try:
        # Delete all objects
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' in page:
                objects = [{'Key': obj['Key']} for obj in page['Contents']]
                if objects:
                    s3_client.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': objects}
                    )
                    print(f"  Deleted {len(objects)} objects")
        
        # Delete all versions (for versioned buckets)
        paginator = s3_client.get_paginator('list_object_versions')
        for page in paginator.paginate(Bucket=bucket_name):
            delete_list = []
            
            # Add versions
            if 'Versions' in page:
                delete_list.extend([
                    {'Key': version['Key'], 'VersionId': version['VersionId']}
                    for version in page['Versions']
                ])
            
            # Add delete markers
            if 'DeleteMarkers' in page:
                delete_list.extend([
                    {'Key': marker['Key'], 'VersionId': marker['VersionId']}
                    for marker in page['DeleteMarkers']
                ])
            
            if delete_list:
                s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': delete_list}
                )
                print(f"  Deleted {len(delete_list)} versions/markers")
                
    except ClientError as e:
        print(f"  Error deleting contents: {e}")
        return False
    return True

def main():
    s3_client = boto3.client('s3')
    
    # Get all buckets
    try:
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
    except ClientError as e:
        print(f"Error listing buckets: {e}")
        return
    
    # Filter out system buckets to keep
    system_prefixes = [
        'aws-athena-query-results',
        'aws-glue-scripts', 
        'aws-glue-temporary',
        'cdk-hnb659fds-assets'
    ]
    
    buckets_to_delete = []
    for bucket in buckets:
        keep_bucket = any(bucket.startswith(prefix) for prefix in system_prefixes)
        if not keep_bucket:
            buckets_to_delete.append(bucket)
    
    if not buckets_to_delete:
        print("No general purpose buckets found to delete.")
        return
    
    print(f"Found {len(buckets_to_delete)} general purpose buckets to delete:")
    for bucket in buckets_to_delete:
        print(f"  - {bucket}")
    
    # Auto-confirm deletion for cleanup
    print("\n🗑️  Proceeding with deletion...")
    
    # Delete each bucket
    success_count = 0
    for bucket_name in buckets_to_delete:
        print(f"\nDeleting bucket: {bucket_name}")
        
        try:
            # First, delete all contents
            if delete_all_bucket_contents(bucket_name, s3_client):
                # Then delete the bucket
                s3_client.delete_bucket(Bucket=bucket_name)
                print(f"  ✅ Deleted bucket: {bucket_name}")
                success_count += 1
            else:
                print(f"  ❌ Failed to delete contents of: {bucket_name}")
                
        except ClientError as e:
            print(f"  ❌ Error deleting bucket {bucket_name}: {e}")
    
    print(f"\n🎉 Successfully deleted {success_count} out of {len(buckets_to_delete)} buckets.")

if __name__ == "__main__":
    main()