#!/usr/bin/env python3
"""
AWS DMS Data Ingestion Deployment Script
Main orchestration script with terminal panel interface.
"""

import os
import sys
import time
import threading
import json
import logging
import traceback
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Optional

# Rich imports for terminal UI
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.align import Align
from rich.columns import Columns

# Add the bin directory to Python path for imports
sys.path.append(str(Path(__file__).parent))

from infra import setup_rds, setup_s3_bucket, setup_iam_role, setup_dms_vpc_role
from db_init import setup_source_db
from dms import create_replication_instance, create_source_endpoint, create_target_endpoint, create_migration_task, start_migration
from validate_and_monitor import validate_s3_data, setup_monitoring

# Load environment variables
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / '.env')

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running" 
    SUCCESS = "success"
    FAILED = "failure"  # Match continuity.json format
    SKIPPED = "skipped"
    WAITING = "waiting"  # Add waiting status from continuity.json

@dataclass
class TaskInfo:
    name: str
    display_name: str
    status: TaskStatus
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None

class DeploymentUI:
    def __init__(self):
        self.console = Console()
        self.tasks: Dict[str, TaskInfo] = {
            'infra': TaskInfo('infra', 'infra', TaskStatus.PENDING),
            'db_init': TaskInfo('db_init', 'db_init', TaskStatus.PENDING),
            'dms': TaskInfo('dms', 'dms', TaskStatus.PENDING),
            'validate': TaskInfo('validate', 'validate', TaskStatus.PENDING),
        }
        self.log_messages: List[str] = []
        self.start_time = datetime.now()
        
        # Status emojis
        self.status_emojis = {
            TaskStatus.PENDING: "⏳",
            TaskStatus.RUNNING: "🚀", 
            TaskStatus.SUCCESS: "✅",
            TaskStatus.FAILED: "❌",
            TaskStatus.SKIPPED: "⏭️",
            TaskStatus.WAITING: "⏸️"
        }
        
        # Path to files
        self.continuity_file = Path(__file__).parent / 'continuity.json'
        self.working_parameters_file = Path(__file__).parent / 'working-parameters.json'
        self.log_file = Path(__file__).parent / 'deployment.log'
        
        # Setup file logging
        self.setup_logging()
        
        # Load existing continuity state and working parameters
        self.load_continuity_state()
        self.load_working_parameters()
    
    def setup_logging(self):
        """Setup file logging for deployment tracking."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file, mode='a'),
                logging.StreamHandler()  # Enable console output for web streaming
            ]
        )
        self.file_logger = logging.getLogger('deployment')
        
        # Log deployment start
        self.file_logger.info("="*60)
        self.file_logger.info("NEW DEPLOYMENT SESSION STARTED")
        self.file_logger.info("="*60)
    
    def log(self, message: str, task_name: str = None, level: str = "INFO"):
        """Add a log message with timestamp to both UI and file."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {message}"
        self.log_messages.append(formatted_message)
        
        # Also log to file with more details
        full_message = f"[{task_name}] {message}" if task_name else message
        if level == "ERROR":
            self.file_logger.error(full_message)
        elif level == "WARNING":
            self.file_logger.warning(full_message)
        else:
            self.file_logger.info(full_message)
        
        # Keep only last 50 messages to prevent memory issues
        if len(self.log_messages) > 50:
            self.log_messages = self.log_messages[-50:]
    
    def log_error(self, message: str, error: Exception, task_name: str = None):
        """Log an error with full traceback to file."""
        error_msg = f"ERROR: {message} - {str(error)}"
        self.log(error_msg, task_name, "ERROR")
        
        # Log full traceback to file only
        self.file_logger.error("Full traceback:")
        self.file_logger.error(traceback.format_exc())
    
    def load_continuity_state(self):
        """Load task states from continuity.json file."""
        try:
            if self.continuity_file.exists():
                with open(self.continuity_file, 'r') as f:
                    continuity_data = json.load(f)
                
                # Map continuity task names to our internal names
                task_mapping = {
                    'setup_infra': 'infra',
                    'init_db': 'db_init',
                    'run_dms': 'dms',
                    'validate': 'validate'
                }
                
                # Update task statuses from continuity file
                for continuity_task, internal_task in task_mapping.items():
                    if continuity_task in continuity_data.get('tasks', {}):
                        status_str = continuity_data['tasks'][continuity_task]
                        try:
                            # Convert string to TaskStatus enum
                            status = TaskStatus(status_str)
                            if internal_task in self.tasks:
                                self.tasks[internal_task].status = status
                        except ValueError:
                            # If status not recognized, keep as pending
                            pass
                
                self.log(f"Loaded continuity state from {self.continuity_file.name}")
                
                # Log which tasks are already completed
                completed_tasks = [name for name, task in self.tasks.items() 
                                 if task.status == TaskStatus.SUCCESS]
                if completed_tasks:
                    self.log(f"Previously completed tasks: {', '.join(completed_tasks)}")
                    
        except Exception as e:
            self.log(f"Warning: Could not load continuity state: {e}")
    
    def save_continuity_state(self, region: str = "us-east-1"):
        """Save current task states to continuity.json file."""
        try:
            # Map internal task names back to continuity format
            task_mapping = {
                'infra': 'setup_infra',
                'db_init': 'init_db',
                'dms': 'run_dms',
                'validate': 'validate'
            }
            
            continuity_data = {
                "project_name": "AWS-DMS-Data-Ingestion",
                "last_run": datetime.now().isoformat(),
                "region": region,
                "tasks": {}
            }
            
            # Convert our task statuses to continuity format
            for internal_task, continuity_task in task_mapping.items():
                if internal_task in self.tasks:
                    continuity_data["tasks"][continuity_task] = self.tasks[internal_task].status.value
            
            # Add cleanup task (always pending unless explicitly set)
            continuity_data["tasks"]["cleanup"] = "pending"
            
            # Write to file
            with open(self.continuity_file, 'w') as f:
                json.dump(continuity_data, f, indent=4)
                
            self.log(f"Saved continuity state to {self.continuity_file.name}")
            
        except Exception as e:
            self.log(f"Warning: Could not save continuity state: {e}")
    
    def should_skip_task(self, task_name: str) -> bool:
        """Check if task should be skipped based on continuity state."""
        if task_name in self.tasks:
            return self.tasks[task_name].status == TaskStatus.SUCCESS
        return False
    
    def load_working_parameters(self):
        """Load working parameters from working-parameters.json file."""
        try:
            if self.working_parameters_file.exists():
                with open(self.working_parameters_file, 'r') as f:
                    self.working_parameters = json.load(f)
                self.log(f"Loaded working parameters from {self.working_parameters_file.name}")
                
                # Log key parameters for visibility
                created_resources = self.working_parameters.get('created_resources', {})
                if created_resources:
                    resource_summary = []
                    for key, value in created_resources.items():
                        if value:  # Only show non-empty values
                            resource_summary.append(f"{key}: {value}")
                    if resource_summary:
                        self.log(f"Found existing resources: {', '.join(resource_summary[:3])}...")
            else:
                # Initialize empty working parameters
                self.working_parameters = {
                    "project_name": "AWS-DMS-Data-Ingestion",
                    "deployment_id": self._generate_deployment_id(),
                    "created_resources": {},
                    "configuration": {},
                    "last_updated": datetime.now().isoformat()
                }
                self.save_working_parameters()
                
        except Exception as e:
            self.log(f"Warning: Could not load working parameters: {e}")
            # Initialize empty parameters as fallback
            self.working_parameters = {
                "project_name": "AWS-DMS-Data-Ingestion",
                "deployment_id": self._generate_deployment_id(),
                "created_resources": {},
                "configuration": {},
                "last_updated": datetime.now().isoformat()
            }
    
    def _generate_deployment_id(self) -> str:
        """Generate a unique deployment ID."""
        import uuid
        return str(uuid.uuid4())[:8]
    
    def save_working_parameters(self):
        """Save current working parameters to working-parameters.json file."""
        try:
            self.working_parameters["last_updated"] = datetime.now().isoformat()
            
            with open(self.working_parameters_file, 'w') as f:
                json.dump(self.working_parameters, f, indent=4)
                
        except Exception as e:
            self.log(f"Warning: Could not save working parameters: {e}")
    
    def set_parameter(self, category: str, key: str, value: str):
        """Set a parameter in the working parameters file."""
        try:
            if category not in self.working_parameters:
                self.working_parameters[category] = {}
            
            self.working_parameters[category][key] = value
            self.save_working_parameters()
            self.log(f"Saved parameter: {category}.{key} = {value}")
            
        except Exception as e:
            self.log(f"Warning: Could not set parameter {category}.{key}: {e}")
    
    def get_parameter(self, category: str, key: str, default=None):
        """Get a parameter from the working parameters."""
        try:
            return self.working_parameters.get(category, {}).get(key, default)
        except Exception:
            return default
    
    def get_created_resource(self, resource_name: str, default=None):
        """Get a created resource parameter."""
        return self.get_parameter('created_resources', resource_name, default)
    
    def set_created_resource(self, resource_name: str, resource_value: str):
        """Set a created resource parameter."""
        self.set_parameter('created_resources', resource_name, resource_value)
    
    def update_task_status(self, task_name: str, status: TaskStatus, error_message: str = None, region: str = "us-east-1"):
        """Update the status of a specific task and save to continuity file."""
        if task_name in self.tasks:
            task = self.tasks[task_name]
            task.status = status
            task.error_message = error_message
            
            if status == TaskStatus.RUNNING and not task.start_time:
                task.start_time = datetime.now()
            elif status in [TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.SKIPPED]:
                task.end_time = datetime.now()
            
            # Save state to continuity file after each status update
            self.save_continuity_state(region)
    
    def get_summary_stats(self) -> Dict[str, int]:
        """Get summary statistics for all tasks."""
        stats = {
            'completed': 0,
            'failed': 0,
            'skipped': 0,
            'pending': 0
        }
        
        for task in self.tasks.values():
            if task.status == TaskStatus.SUCCESS:
                stats['completed'] += 1
            elif task.status == TaskStatus.FAILED:
                stats['failed'] += 1
            elif task.status == TaskStatus.SKIPPED:
                stats['skipped'] += 1
            elif task.status == TaskStatus.PENDING:
                stats['pending'] += 1
        
        return stats
    
    def get_total_duration(self) -> str:
        """Get formatted total duration."""
        duration = datetime.now() - self.start_time
        total_seconds = int(duration.total_seconds())
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        
        if minutes > 0:
            return f"{minutes}.{seconds//6}m"
        else:
            return f"{seconds}s"
    
    def create_status_panel(self) -> Panel:
        """Create the left status panel showing task statuses."""
        status_table = Table.grid(padding=0)
        status_table.add_column(justify="left")
        status_table.add_column(justify="left", width=6)
        
        for task_name, task in self.tasks.items():
            emoji = self.status_emojis[task.status]
            task_text = Text(f"{task.display_name:<10}", style="bold")
            status_text = Text(f"{emoji}", style="bold")
            status_table.add_row(task_text, status_text)
        
        return Panel(
            status_table,
            title="[bold blue]MODULES[/bold blue]",
            border_style="blue",
            width=20,
            height=8
        )
    
    def create_summary_panel(self) -> Panel:
        """Create the right summary panel showing statistics."""
        stats = self.get_summary_stats()
        duration = self.get_total_duration()
        
        summary_content = Text()
        summary_content.append("DEPLOYMENT SUMMARY", style="bold")
        summary_content.append("\n" + "=" * 45 + "\n")
        summary_content.append(f"✅ Completed: {stats['completed']:<10}\n", style="green")
        summary_content.append(f"❌ Failed:    {stats['failed']:<10}\n", style="red") 
        summary_content.append(f"⏭️ Skipped:   {stats['skipped']:<10}\n", style="yellow")
        summary_content.append(f"⏳ Pending:   {stats['pending']:<10}\n", style="blue")
        summary_content.append(f"⏱️ Total Duration: {duration:<10}\n", style="cyan")
        summary_content.append("=" * 45)
        
        return Panel(
            Align.left(summary_content),
            border_style="blue",
            width=55,
            height=8
        )
    
    def create_log_panel(self) -> Panel:
        """Create the bottom log panel showing recent messages."""
        log_content = "\n".join(self.log_messages[-15:])  # Show last 15 messages
        
        return Panel(
            Text(log_content, style="white"),
            title="[bold green]DEPLOYMENT LOG[/bold green]",
            border_style="green",
            height=10
        )
    
    def create_layout(self) -> Layout:
        """Create the main layout with panels."""
        # Create panels first
        status_panel = self.create_status_panel()
        summary_panel = self.create_summary_panel()
        log_panel = self.create_log_panel()
        
        # Create layout structure
        top_layout = Layout()
        top_layout.split_row(
            Layout(status_panel, name="status"),
            Layout(summary_panel, name="summary")
        )
        
        main_layout = Layout()
        main_layout.split_column(
            Layout(top_layout, name="top", ratio=1),
            Layout(log_panel, name="bottom", ratio=1)
        )
        
        return main_layout

class DeploymentManager:
    def __init__(self, ui: DeploymentUI):
        self.ui = ui
        self.config = self.load_config()
        self.load_runtime_config()
    
    def load_config(self):
        """Load configuration from environment variables."""
        config = {
            'aws_account_id': os.getenv('AWS_ACCOUNT_ID'),
            'aws_region': os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
            'aws_profile': os.getenv('AWS_PROFILE'),
            'aurora_password': os.getenv('AURORA_DB_PASSWORD'),
            'bucket_name': f"dms-data-ingestion-{os.getenv('AWS_ACCOUNT_ID')}",
            'bucket_folder': 'dms-sql-data',
            'db_instance_id': 'dms-source-sqlserver',
            'db_name': 'SRC_DB',
            'table_name': 'raw_src',
            'replication_instance_id': 'dms-replication-instance',
            'source_endpoint_id': 'sqlserver-source',
            'target_endpoint_id': 's3-target',
            'migration_task_id': 'sqlserver-to-s3-migration',
            'iam_role_name': 'dms-s3-access-role'
        }
        
        # Validate required environment variables
        required_vars = ['aws_account_id', 'aurora_password']
        missing_vars = [var for var in required_vars if not config[var]]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        return config
    
    def load_runtime_config(self):
        """Load runtime configuration from working parameters."""
        try:
            # Load parameters from working-parameters.json instead of querying AWS
            self.config['rds_endpoint'] = self.ui.get_created_resource('rds_endpoint')
            self.config['iam_role_arn'] = self.ui.get_created_resource('iam_role_arn')
            self.config['security_group_id'] = self.ui.get_created_resource('security_group_id')
            self.config['bucket_arn'] = self.ui.get_created_resource('s3_bucket_arn')
            
            # Log loaded parameters
            if self.config['rds_endpoint']:
                self.ui.log(f"Loaded RDS endpoint from parameters: {self.config['rds_endpoint']}")
            if self.config['iam_role_arn']:
                self.ui.log(f"Loaded IAM role ARN from parameters: {self.config['iam_role_arn']}")
                
            # Store configuration parameters for reference
            self.ui.set_parameter('configuration', 'region', self.config['aws_region'])
            self.ui.set_parameter('configuration', 'account_id', self.config['aws_account_id'])
                    
        except Exception as e:
            self.ui.log(f"Warning: Could not load runtime configuration: {e}")
    
    def deploy_infrastructure(self):
        """Deploy AWS infrastructure components."""
        # Check if task should be skipped
        if self.ui.should_skip_task('infra'):
            self.ui.log("Infrastructure already completed - skipping...")
            return
            
        self.ui.update_task_status('infra', TaskStatus.RUNNING, region=self.config['aws_region'])
        self.ui.log("Starting infrastructure deployment...")
        
        try:
            # Setup RDS instance
            self.ui.log("Creating RDS SQL Server instance...")
            rds_endpoint, security_group_id = setup_rds(
                instance_id=self.config['db_instance_id'],
                password=self.config['aurora_password'],
                region=self.config['aws_region']
            )
            self.config['rds_endpoint'] = rds_endpoint
            self.config['security_group_id'] = security_group_id
            self.ui.set_created_resource('rds_endpoint', rds_endpoint)
            self.ui.set_created_resource('rds_instance_id', self.config['db_instance_id'])
            self.ui.set_created_resource('security_group_id', security_group_id)
            self.ui.log(f"RDS instance ready: {rds_endpoint}")
            
            # Setup S3 bucket
            self.ui.log("Setting up S3 bucket and folder...")
            bucket_arn = setup_s3_bucket(
                bucket_name=self.config['bucket_name'],
                folder_name=self.config['bucket_folder'],
                region=self.config['aws_region']
            )
            self.config['bucket_arn'] = bucket_arn
            self.ui.set_created_resource('s3_bucket_name', self.config['bucket_name'])
            self.ui.set_created_resource('s3_bucket_arn', bucket_arn)
            self.ui.set_created_resource('s3_bucket_folder', self.config['bucket_folder'])
            self.ui.log(f"S3 bucket created: {self.config['bucket_name']}")
            
            # Setup IAM role
            self.ui.log("Creating IAM role for DMS...")
            role_arn = setup_iam_role(
                role_name=self.config['iam_role_name'],
                bucket_name=self.config['bucket_name']
            )
            self.config['iam_role_arn'] = role_arn
            self.ui.set_created_resource('iam_role_arn', role_arn)
            self.ui.set_created_resource('iam_role_name', self.config['iam_role_name'])
            self.ui.log(f"IAM role created successfully")
            
            # Setup DMS VPC role
            self.ui.log("Creating DMS VPC service-linked role...")
            dms_vpc_role_arn = setup_dms_vpc_role()
            self.ui.set_created_resource('dms_vpc_role_arn', dms_vpc_role_arn)
            self.ui.log("DMS VPC role ready")
            
            self.ui.update_task_status('infra', TaskStatus.SUCCESS, region=self.config['aws_region'])
            self.ui.log("Infrastructure deployment completed!")
            
        except Exception as e:
            self.ui.update_task_status('infra', TaskStatus.FAILED, str(e), region=self.config['aws_region'])
            self.ui.log(f"Infrastructure deployment failed: {str(e)}")
            raise
    
    def initialize_database(self):
        """Initialize and seed the source database."""
        # Check if task should be skipped
        if self.ui.should_skip_task('db_init'):
            self.ui.log("Database initialization already completed - skipping...")
            return
            
        self.ui.update_task_status('db_init', TaskStatus.RUNNING, region=self.config['aws_region'])
        self.ui.log("Starting database initialization...")
        
        try:
            self.ui.log("Creating source database and table...")
            setup_source_db(
                endpoint=self.config['rds_endpoint'],
                password=self.config['aurora_password'],
                db_name=self.config['db_name'],
                table_name=self.config['table_name']
            )
            
            self.ui.update_task_status('db_init', TaskStatus.SUCCESS, region=self.config['aws_region'])
            self.ui.log("Database initialization completed!")
            
        except Exception as e:
            self.ui.update_task_status('db_init', TaskStatus.FAILED, str(e), region=self.config['aws_region'])
            self.ui.log(f"Database initialization failed: {str(e)}")
            raise
    
    def setup_dms_migration(self):
        """Setup DMS components and run migration."""
        # Check if task should be skipped
        if self.ui.should_skip_task('dms'):
            self.ui.log("DMS migration already completed - skipping...")
            return
            
        self.ui.update_task_status('dms', TaskStatus.RUNNING, region=self.config['aws_region'])
        self.ui.log("Starting DMS migration setup...")
        
        try:
            # Create replication instance
            self.ui.log("Creating DMS replication instance...")
            replication_instance_arn = create_replication_instance(
                instance_id=self.config['replication_instance_id'],
                region=self.config['aws_region']
            )
            self.ui.set_created_resource('replication_instance_arn', replication_instance_arn)
            self.ui.set_created_resource('replication_instance_id', self.config['replication_instance_id'])
            self.ui.log("DMS replication instance created")
            
            # Create source endpoint
            self.ui.log("Creating source endpoint...")
            source_endpoint_arn = create_source_endpoint(
                endpoint_id=self.config['source_endpoint_id'],
                server_name=self.config['rds_endpoint'],
                password=self.config['aurora_password'],
                database_name=self.config['db_name'],
                replication_instance_arn=replication_instance_arn
            )
            self.ui.set_created_resource('source_endpoint_arn', source_endpoint_arn)
            self.ui.set_created_resource('source_endpoint_id', self.config['source_endpoint_id'])
            self.ui.log("Source endpoint created")
            
            # Create target endpoint
            self.ui.log("Creating target endpoint...")
            target_endpoint_arn = create_target_endpoint(
                endpoint_id=self.config['target_endpoint_id'],
                role_arn=self.config['iam_role_arn'],
                bucket_name=self.config['bucket_name'],
                bucket_folder=self.config['bucket_folder']
            )
            self.ui.set_created_resource('target_endpoint_arn', target_endpoint_arn)
            self.ui.set_created_resource('target_endpoint_id', self.config['target_endpoint_id'])
            self.ui.log("Target endpoint created")
            
            # Create migration task
            self.ui.log("Creating migration task...")
            create_migration_task(
                task_id=self.config['migration_task_id'],
                replication_instance_arn=replication_instance_arn,
                source_endpoint_arn=source_endpoint_arn,
                target_endpoint_arn=target_endpoint_arn,
                table_name=self.config['table_name']
            )
            migration_task_arn = f"arn:aws:dms:{self.config['aws_region']}:{self.config['aws_account_id']}:task:{self.config['migration_task_id']}"
            self.ui.set_created_resource('migration_task_arn', migration_task_arn)
            self.ui.set_created_resource('migration_task_id', self.config['migration_task_id'])
            self.ui.log("Migration task created")
            
            # Start migration
            self.ui.log("Starting data migration...")
            migration_success = start_migration(self.config['migration_task_id'], self.config['aws_region'])
            
            if not migration_success:
                raise Exception("Migration task failed to complete successfully")
            
            self.ui.update_task_status('dms', TaskStatus.SUCCESS, region=self.config['aws_region'])
            self.ui.log("DMS migration completed successfully!")
            
        except Exception as e:
            self.ui.update_task_status('dms', TaskStatus.FAILED, str(e), region=self.config['aws_region'])
            self.ui.log(f"DMS migration failed: {str(e)}")
            raise
    
    def validate_deployment(self):
        """Validate the migration and setup monitoring."""
        # Validation always runs regardless of previous state
            
        self.ui.update_task_status('validate', TaskStatus.RUNNING, region=self.config['aws_region'])
        self.ui.log("Starting validation and monitoring setup...")
        
        try:
            # Validate S3 data
            self.ui.log("Validating migrated data in S3...")
            validation_result = validate_s3_data(
                bucket_name=self.config['bucket_name'],
                bucket_folder=self.config['bucket_folder'],
                rds_endpoint=self.config['rds_endpoint'],
                password=self.config['aurora_password'],
                db_name=self.config['db_name'],
                table_name=self.config['table_name']
            )
            
            if validation_result:
                self.ui.log("Data validation successful!")
            else:
                raise Exception("Data validation failed - row counts don't match")
            
            # Setup monitoring
            self.ui.log("Setting up CloudWatch monitoring...")
            migration_task_arn = self.ui.get_created_resource('migration_task_arn')
            if not migration_task_arn:
                migration_task_arn = f"arn:aws:dms:{self.config['aws_region']}:{self.config['aws_account_id']}:task:{self.config['migration_task_id']}"
            
            setup_monitoring(
                task_arn=migration_task_arn,
                region=self.config['aws_region']
            )
            self.ui.log("Monitoring setup completed")
            
            self.ui.update_task_status('validate', TaskStatus.SUCCESS, region=self.config['aws_region'])
            self.ui.log("Validation and monitoring completed!")
            
        except Exception as e:
            self.ui.update_task_status('validate', TaskStatus.FAILED, str(e), region=self.config['aws_region'])
            self.ui.log(f"Validation failed: {str(e)}")
            raise

def run_deployment_with_ui():
    """Run deployment with terminal UI."""
    ui = DeploymentUI()
    manager = DeploymentManager(ui)
    
    success = True
    
    def deployment_worker():
        nonlocal success
        try:
            ui.log("Starting AWS DMS Data Ingestion Deployment")
            ui.log(f"Configuration loaded for AWS Account: {manager.config['aws_account_id']}")
            
            manager.deploy_infrastructure()
            time.sleep(1)  # Brief pause for UI update
            
            manager.initialize_database()
            time.sleep(1)
            
            manager.setup_dms_migration()
            time.sleep(1)
            
            manager.validate_deployment()
            time.sleep(1)
            
            ui.log("🎉 Deployment completed successfully!")
            ui.log("Check the S3 bucket for migrated data files.")
            
            # Display connection string
            if manager.config.get('rds_endpoint'):
                connection_string = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={manager.config['rds_endpoint']},1433;"
                    f"DATABASE={manager.config.get('db_name', 'SRC_DB')};"
                    f"UID=admin;"
                    f"PWD={manager.config.get('aurora_password', '[PASSWORD]')};"
                    f"TrustServerCertificate=yes;"
                )
                ui.log("📊 Database Connection String:")
                ui.log(f"   {connection_string}")
            
        except Exception as e:
            success = False
            ui.log_error("Deployment failed", e)
    
    # Run deployment in a separate thread
    deployment_thread = threading.Thread(target=deployment_worker)
    deployment_thread.daemon = True
    deployment_thread.start()
    
    # Run the UI
    with Live(ui.create_layout(), refresh_per_second=4, screen=True) as live:
        while deployment_thread.is_alive():
            live.update(ui.create_layout())
            time.sleep(0.25)
        
        # Show final state for a few seconds
        live.update(ui.create_layout())
        time.sleep(3)
    
    return success

def main():
    """Main deployment function."""
    # Setup basic logging for main function errors
    log_file = Path(__file__).parent / 'deployment.log'
    main_logger = logging.getLogger('main')
    
    try:
        success = run_deployment_with_ui()
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n🛑 Deployment cancelled by user")
        main_logger.info("Deployment cancelled by user (KeyboardInterrupt)")
        sys.exit(1)
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(f"\n💥 {error_msg}")
        
        # Log full error details to file
        main_logger.error(error_msg)
        main_logger.error("Full traceback:")
        main_logger.error(traceback.format_exc())
        
        print(f"💡 Check {log_file} for detailed error information")
        sys.exit(1)

if __name__ == "__main__":
    main()