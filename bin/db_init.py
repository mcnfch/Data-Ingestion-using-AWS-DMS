"""
Database initialization module for AWS DMS Data Ingestion Pipeline.
Handles creation and seeding of the source SQL Server database.
"""

import pyodbc
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def setup_source_db(endpoint, password, db_name='SRC_DB', table_name='raw_src', username='admin', port=1433):
    """
    Create and seed the source SQL Server database and table.
    
    Args:
        endpoint (str): RDS SQL Server endpoint
        password (str): Database password
        db_name (str): Database name to create
        table_name (str): Table name to create
        username (str): Database username
        port (int): Database port
    """
    # Connection string for SQL Server
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={endpoint},{port};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
    )
    
    try:
        logger.info(f"Connecting to SQL Server at {endpoint}...")
        
        # First connect to master database to create our database
        master_conn_string = connection_string + "DATABASE=master;"
        
        with pyodbc.connect(master_conn_string, timeout=30) as conn:
            conn.autocommit = True
            cursor = conn.cursor()
            
            # Check if database exists
            cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{db_name}'")
            if cursor.fetchone():
                logger.info(f"Database {db_name} already exists")
            else:
                # Create database
                logger.info(f"Creating database: {db_name}")
                cursor.execute(f"CREATE DATABASE [{db_name}]")
                logger.info(f"Database {db_name} created successfully")
        
        # Connect to the created database
        db_conn_string = connection_string + f"DATABASE={db_name};"
        
        with pyodbc.connect(db_conn_string, timeout=30) as conn:
            conn.autocommit = True
            cursor = conn.cursor()
            
            # Check if table exists
            cursor.execute(f"""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = 'dbo'
            """)
            
            if cursor.fetchone():
                logger.info(f"Table {table_name} already exists")
                # Check if table has data
                cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
                row_count = cursor.fetchone()[0]
                logger.info(f"Table {table_name} has {row_count} rows")
                
                if row_count == 0:
                    logger.info("Table is empty, inserting sample data...")
                    insert_sample_data(cursor, table_name)
            else:
                # Create table
                logger.info(f"Creating table: {table_name}")
                create_table_sql = f"""
                CREATE TABLE dbo.{table_name} (
                    EMPID INTEGER,
                    NAME VARCHAR(50),
                    AGE INTEGER,
                    GENDER VARCHAR(10),
                    LOCATION VARCHAR(50),
                    DATE DATE,
                    SRC_DTS DATETIME
                )
                """
                cursor.execute(create_table_sql)
                logger.info(f"Table {table_name} created successfully")
                
                # Insert sample data
                logger.info("Inserting sample data...")
                insert_sample_data(cursor, table_name)
        
        logger.info("Database initialization completed successfully")
        
    except pyodbc.Error as e:
        logger.error(f"Database error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

def insert_sample_data(cursor, table_name):
    """
    Insert sample data into the table.
    
    Args:
        cursor: Database cursor
        table_name (str): Name of the table to insert data into
    """
    sample_data = [
        (101, 'Robert', 34, 'Male', 'Houston', '2023-04-05', '2023-06-16 00:00:00.000'),
        (102, 'Sam', 29, 'Male', 'Dallas', '2023-03-21', '2023-06-16 00:00:00.000'),
        (103, 'Smith', 25, 'Male', 'Texas', '2023-04-10', '2023-06-16 00:00:00.000'),
        (104, 'Dan', 31, 'Male', 'Florida', '2023-02-07', '2023-06-16 00:00:00.000'),
        (105, 'Lily', 27, 'Female', 'Cannes', '2023-01-30', '2023-06-16 00:00:00.000')
    ]
    
    insert_sql = f"""
    INSERT INTO dbo.{table_name} 
    (EMPID, NAME, AGE, GENDER, LOCATION, DATE, SRC_DTS)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    
    cursor.executemany(insert_sql, sample_data)
    logger.info(f"Inserted {len(sample_data)} sample records into {table_name}")

def verify_database_setup(endpoint, password, db_name='SRC_DB', table_name='raw_src', username='admin', port=1433):
    """
    Verify that the database and table were created correctly.
    
    Args:
        endpoint (str): RDS SQL Server endpoint
        password (str): Database password
        db_name (str): Database name
        table_name (str): Table name
        username (str): Database username
        port (int): Database port
    
    Returns:
        dict: Verification results
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
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
            row_count = cursor.fetchone()[0]
            
            # Get table schema
            cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = 'dbo'
                ORDER BY ORDINAL_POSITION
            """)
            columns = cursor.fetchall()
            
            # Get sample data
            cursor.execute(f"SELECT TOP 5 * FROM dbo.{table_name} ORDER BY id")
            sample_rows = cursor.fetchall()
            
            results = {
                'database_name': db_name,
                'table_name': table_name,
                'row_count': row_count,
                'columns': [{'name': col[0], 'type': col[1], 'nullable': col[2], 'length': col[3]} for col in columns],
                'sample_data': [dict(zip([desc[0] for desc in cursor.description], row)) for row in sample_rows]
            }
            
            logger.info(f"Database verification completed: {row_count} rows in {table_name}")
            return results
            
    except Exception as e:
        logger.error(f"Error verifying database setup: {e}")
        raise

def add_test_data(endpoint, password, db_name='SRC_DB', table_name='raw_src', username='admin', port=1433, num_records=10):
    """
    Add additional test data to the table for testing ongoing replication.
    
    Args:
        endpoint (str): RDS SQL Server endpoint
        password (str): Database password
        db_name (str): Database name
        table_name (str): Table name
        username (str): Database username
        port (int): Database port
        num_records (int): Number of test records to add
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
            conn.autocommit = True
            cursor = conn.cursor()
            
            # Generate test data
            departments = ['Engineering', 'Marketing', 'Sales', 'HR', 'Finance', 'Operations']
            base_names = ['Test User', 'Sample Employee', 'Demo Person', 'Trial Worker', 'Example Staff']
            
            test_data = []
            for i in range(num_records):
                first_name = f"Test{i+1}"
                last_name = f"User{i+1}"
                email = f"test.user{i+1}@company.com"
                phone = f"555-{1000+i:04d}"
                department = departments[i % len(departments)]
                salary = 60000.00 + (i * 1000)
                
                test_data.append((first_name, last_name, email, phone, department, '2023-10-01', salary, 1))
            
            insert_sql = f"""
            INSERT INTO dbo.{table_name} 
            (first_name, last_name, email, phone, department, hire_date, salary, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            cursor.executemany(insert_sql, test_data)
            logger.info(f"Added {num_records} test records to {table_name}")
            
    except Exception as e:
        logger.error(f"Error adding test data: {e}")
        raise

if __name__ == "__main__":
    # Test database initialization
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Test configuration
    test_config = {
        'endpoint': 'test-endpoint.region.rds.amazonaws.com',  # Replace with actual endpoint
        'password': os.getenv('AURORA_DB_PASSWORD'),
        'db_name': 'TEST_DB',
        'table_name': 'test_table'
    }
    
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Setup database
        setup_source_db(
            endpoint=test_config['endpoint'],
            password=test_config['password'],
            db_name=test_config['db_name'],
            table_name=test_config['table_name']
        )
        
        # Verify setup
        results = verify_database_setup(
            endpoint=test_config['endpoint'],
            password=test_config['password'],
            db_name=test_config['db_name'],
            table_name=test_config['table_name']
        )
        
        print(f"Verification Results:")
        print(f"Row Count: {results['row_count']}")
        print(f"Columns: {len(results['columns'])}")
        
    except Exception as e:
        print(f"Error: {e}")