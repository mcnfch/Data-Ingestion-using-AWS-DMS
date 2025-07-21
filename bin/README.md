# AWS DMS Data Ingestion Deployment Scripts

Automated deployment scripts for AWS Database Migration Service (DMS) data ingestion pipeline.

## Overview

This project automates the deployment of an end-to-end data ingestion pipeline using AWS DMS to migrate data from SQL Server on RDS to Amazon S3.

## Architecture

```
SQL Server (RDS) → DMS Replication → S3 Bucket
```

## Prerequisites

1. **AWS Account** with appropriate permissions
2. **Python 3.8+** installed
3. **SQL Server ODBC Driver 17** installed
4. **AWS CLI** configured (optional but recommended)

### Installing SQL Server ODBC Driver

**macOS:**
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
brew install msodbcsql17 mssql-tools
```

**Ubuntu/Debian:**
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql17
```

## Setup

1. **Clone or navigate to the project directory:**
   ```bash
   cd "01 Data Ingestion using AWS DMS"
   ```

2. **Install Python dependencies:**
   ```bash
   cd bin
   pip install -r requirements.txt
   ```

3. **Configure environment variables:**
   
   Ensure your `bin/.env` file contains:
   ```bash
   AWS_ACCESS_KEY_ID=your_access_key
   AWS_SECRET_ACCESS_KEY=your_secret_key
   AWS_DEFAULT_REGION=us-east-1
   AWS_PROFILE=your_profile
   AURORA_DB_PASSWORD=your_db_password
   AWS_ACCOUNT_ID=your_account_id
   ```

## Usage

### Full Deployment

Deploy the complete pipeline:

```bash
python deploy.py --phase all
```

### Phase-by-Phase Deployment

Deploy specific phases:

```bash
# Phase 1: Infrastructure (RDS, S3, IAM)
python deploy.py --phase infra

# Phase 2: Database setup
python deploy.py --phase db

# Phase 3: DMS configuration and migration
python deploy.py --phase dms

# Phase 4: Validation and monitoring
python deploy.py --phase validate
```

### Skip Validation

Deploy without validation and monitoring:

```bash
python deploy.py --skip-validation
```

## Script Modules

### 1. `deploy.py` - Main Orchestration Script

**Purpose:** Coordinates the entire deployment process

**Usage:**
```bash
python deploy.py [--phase {infra,db,dms,validate,all}] [--skip-validation]
```

**Features:**
- Phase-based deployment
- Comprehensive logging
- Error handling and rollback
- Configuration validation

### 2. `infra.py` - Infrastructure Setup

**Responsibilities:**
- RDS SQL Server instance creation
- S3 bucket and folder setup  
- IAM role creation for DMS

**Functions:**
- `setup_rds()` - Creates RDS SQL Server instance
- `setup_s3_bucket()` - Creates S3 bucket and folder
- `setup_iam_role()` - Creates IAM role for DMS S3 access

### 3. `db_init.py` - Database Initialization

**Responsibilities:**
- Source database and table creation
- Sample data insertion
- Database validation

**Functions:**
- `setup_source_db()` - Creates database and table with sample data
- `verify_database_setup()` - Validates database setup
- `add_test_data()` - Adds additional test data

### 4. `dms.py` - DMS Configuration

**Responsibilities:**
- DMS replication instance creation
- Source and target endpoint setup
- Migration task creation and execution

**Functions:**
- `create_replication_instance()` - Creates DMS replication instance
- `create_source_endpoint()` - Creates SQL Server source endpoint
- `create_target_endpoint()` - Creates S3 target endpoint  
- `create_migration_task()` - Creates and configures migration task
- `start_migration()` - Starts migration and monitors progress

### 5. `validate_and_monitor.py` - Validation and Monitoring

**Responsibilities:**
- Data validation between source and target
- CloudWatch monitoring setup
- Performance dashboards

**Functions:**
- `validate_s3_data()` - Validates migrated data integrity
- `setup_monitoring()` - Creates CloudWatch alarms and notifications
- `create_custom_dashboard()` - Creates monitoring dashboard
- `generate_validation_report()` - Creates comprehensive validation report

## Configuration

### Default Configuration

The deployment uses these default values:

- **RDS Instance ID:** `dms-source-sqlserver`
- **Database Name:** `SRC_DB`
- **Table Name:** `raw_src`
- **S3 Bucket:** `dms-data-ingestion-{AWS_ACCOUNT_ID}`
- **S3 Folder:** `dms-sql-data`
- **Replication Instance:** `dms-replication-instance`
- **IAM Role:** `dms-s3-access-role`

### Customization

You can customize the deployment by modifying the configuration in `deploy.py`:

```python
config = {
    'db_instance_id': 'my-custom-rds-instance',
    'bucket_name': 'my-custom-bucket',
    # ... other configurations
}
```

## Monitoring

After deployment, monitoring resources include:

1. **CloudWatch Alarms:**
   - DMS task failure detection
   - High replication lag alerts  
   - Low throughput warnings

2. **CloudWatch Dashboard:**
   - Real-time migration metrics
   - Performance visualization
   - Resource utilization tracking

3. **SNS Notifications** (if email configured):
   - Alert notifications
   - Task status updates

## Data Validation

The validation process includes:

- **Row Count Verification:** Compares source and target row counts
- **File Integrity Check:** Validates S3 file structure and content
- **Sample Data Review:** Displays sample migrated records
- **Comprehensive Reporting:** Generates detailed validation reports

## Troubleshooting

### Common Issues

1. **ODBC Driver Error:**
   ```
   Error: ('01000', "[01000] [unixODBC][Driver Manager]Can't open lib")
   ```
   **Solution:** Install SQL Server ODBC Driver 17

2. **RDS Connection Timeout:**
   ```
   Error: ('08001', '[08001] [Microsoft][ODBC Driver 17 for SQL Server]')
   ```
   **Solution:** Check RDS security groups and network connectivity

3. **S3 Access Denied:**
   ```
   Error: botocore.exceptions.ClientError: An error occurred (AccessDenied)
   ```
   **Solution:** Verify IAM permissions for S3 access

4. **DMS Task Failed:**
   ```
   Error: Migration task failed
   ```
   **Solution:** Check DMS task logs in CloudWatch for detailed errors

### Logging

Deployment logs are written to:
- Console output (real-time)
- `deployment.log` file (persistent)

Log levels:
- `INFO`: General deployment progress
- `WARNING`: Non-critical issues  
- `ERROR`: Critical errors requiring attention

### Getting Help

1. Check the deployment logs for detailed error messages
2. Verify AWS permissions and quotas
3. Ensure all prerequisites are properly installed
4. Review the AWS DMS documentation for service-specific issues

## Cleanup

To remove all deployed resources:

```bash
# Note: Cleanup functions are available in each module
# Manual cleanup may be required for safety

python -c "
from infra import cleanup_infrastructure
from dms import cleanup_dms_resources  
from validate_and_monitor import cleanup_monitoring_resources

# Call cleanup functions with your resource IDs
"
```

⚠️ **Warning:** Cleanup operations will permanently delete resources and data.

## Cost Considerations

Estimated monthly costs (us-east-1):
- **RDS SQL Server (db.t3.micro):** ~$13-15
- **DMS Replication Instance (dms.t3.micro):** ~$13-15
- **S3 Storage:** ~$0.023 per GB
- **Data Transfer:** Varies by volume

💡 **Tip:** Use `db.t3.micro` and `dms.t3.micro` for development/testing to minimize costs.

## Security

- All credentials are stored in environment variables
- IAM roles follow least-privilege principle
- RDS instances use encrypted connections
- S3 buckets can be configured with encryption

## Support

For issues with this deployment script:
1. Check the logs and troubleshooting section
2. Review AWS service limits and permissions
3. Consult AWS DMS documentation
4. Contact your AWS support team for service-specific issues