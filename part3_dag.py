import os
import logging
import requests
import pandas as pd
import numpy as np
import shutil
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


#########################################################
#
#   DAG Settings
#
#########################################################

dag_default_args = {
    'owner': 'BDE-GREG',
    'start_date': datetime.now() - timedelta(days=2+4),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='part3_dag_id',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

#########################################################
#
#   Load Environment Variables
#
#########################################################
AIRFLOW_DATA = "/home/airflow/gcs/data/raw"
AIRBNB = AIRFLOW_DATA + "/Airbnb/"
CENSUS = AIRFLOW_DATA + "/census/"
CODE = AIRFLOW_DATA + "/code/"

#########################################################
#
#   Custom Logics for Operator
#
#########################################################

def import_load_airbnb_func(**kwargs):
    """
    Loads the oldest (chronologically first) Airbnb CSV file from the folder
    into bronze.airbnb_raw, archives it after success, and stops.
    Automatically detects columns — no hardcoding required.
    """

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    cursor = conn_ps.cursor()

    # Ensure folder exists
    if not os.path.exists(AIRBNB):
        logging.error(f" Airbnb folder not found: {AIRBNB}")
        return None

    # List all CSV files in the folder (excluding archive)
    all_files = [
        f for f in os.listdir(AIRBNB)
        if f.lower().endswith('.csv')
    ]

    if not all_files:
        logging.info("📂 No CSV files found in Airbnb folder. Exiting task.")
        return None

    # Sort chronologically (oldest first)
    try:
        all_files_sorted = sorted(
            all_files,
            key=lambda x: datetime.strptime(x.replace('.csv', ''), '%m_%Y')
        )
    except Exception as e:
        logging.warning(f"Could not strictly sort files by date; using alphabetical order. ({e})")
        all_files_sorted = sorted(all_files)

    # Pick the first (oldest) file
    file_name = all_files_sorted[0]
    file_path = os.path.join(AIRBNB, file_name)
    logging.info(f"🕐 Selected file for processing: {file_name}")

    # Read CSV
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        logging.error(f"Failed to read {file_name}: {e}")
        return None

    if df.empty:
        logging.warning(f" {file_name} is empty. Moving to archive without loading.")
        archive_folder = os.path.join(AIRBNB, 'archive')
        os.makedirs(archive_folder, exist_ok=True)
        shutil.move(file_path, os.path.join(archive_folder, file_name))
        return None

    # Get columns dynamically
    col_names = df.columns.tolist()
    logging.info(f"Detected columns: {col_names}")

    # Convert to tuples for insertion
    tuple_values = [tuple(row[col] for col in col_names) for _, row in df.iterrows()]
    cols_str = ', '.join(col_names)
    insert_sql = f"INSERT INTO bronze.airbnb_raw ({cols_str}) VALUES %s"

    # Execute bulk insert
    try:
        execute_values(cursor, insert_sql, tuple_values, page_size=5000)
        conn_ps.commit()
        logging.info(f" Inserted {len(df)} records from {file_name} into bronze.airbnb_raw.")
    except Exception as e:
        conn_ps.rollback()
        logging.error(f" Error inserting data from {file_name}: {e}")
        raise
    finally:
        cursor.close()
        conn_ps.close()

    # Move processed file to archive
    archive_folder = os.path.join(AIRBNB, 'archive')
    os.makedirs(archive_folder, exist_ok=True)
    archived_path = os.path.join(archive_folder, file_name)
    shutil.move(file_path, archived_path)
    logging.info(f"📦 Archived processed file → {archived_path}")

    logging.info("🎉 Task complete: 1 file processed and archived successfully.")
    return None


def import_load_2016Census_G01_NSW_LGA_raw_func(**kwargs):
 
    """
    Dynamically loads Census data CSV into the bronze.raw_subcategory table.
    Automatically detects columns from the CSV — no hardcoded column names required.
    """

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    cursor = conn_ps.cursor()

    # Define file path
    sub_category_file_path = os.path.join(CENSUS, '2016Census_G01_NSW_LGA.csv')
    if not os.path.exists(sub_category_file_path):
        logging.info(f"No file found: {sub_category_file_path}")
        return None

    # Read CSV
    df = pd.read_csv(sub_category_file_path)
    if df.empty:
        logging.info("CSV file is empty. Nothing to load.")
        return None

    # Get columns dynamically
    col_names = df.columns.tolist()
    logging.info(f"Detected columns: {col_names}")

    # Prepare values as tuples
    tuple_values = [tuple(row[col] for col in col_names) for _, row in df.iterrows()]

    # Build dynamic SQL
    cols_str = ', '.join(col_names)
    insert_sql = f"INSERT INTO bronze.lga_g01_raw({cols_str}) VALUES %s"

    # Execute bulk insert
    try:
        execute_values(cursor, insert_sql, tuple_values, page_size=5000)
        conn_ps.commit()
        logging.info(f" Successfully inserted {len(df)} records into bronze.lga_g01_raw .")
    except Exception as e:
        conn_ps.rollback()
        logging.error(f" Error inserting data: {e}")
        raise
    finally:
        cursor.close()
        conn_ps.close()

    # Move processed file to archive
    archive_folder = os.path.join(CENSUS, 'archive')
    os.makedirs(archive_folder, exist_ok=True)
    archive_path = os.path.join(archive_folder, '2016Census_G01_NSW_LGA.csv')
    shutil.move(sub_category_file_path, archive_path)
    logging.info(f" File moved to archive: {archive_path}")

    return None


def import_load_2016Census_G02_NSW_LGA_raw_func(**kwargs):

    """
    Dynamically loads Census data CSV into the bronze.raw_subcategory table.
    Automatically detects columns from the CSV — no hardcoded column names required.
    """

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    cursor = conn_ps.cursor()

    # Define file path
    sub_category_file_path = os.path.join(CENSUS, '2016Census_G02_NSW_LGA.csv')
    if not os.path.exists(sub_category_file_path):
        logging.info(f"No file found: {sub_category_file_path}")
        return None

    # Read CSV
    df = pd.read_csv(sub_category_file_path)
    if df.empty:
        logging.info("CSV file is empty. Nothing to load.")
        return None

    # Get columns dynamically
    col_names = df.columns.tolist()
    logging.info(f"Detected columns: {col_names}")

    # Prepare values as tuples
    tuple_values = [tuple(row[col] for col in col_names) for _, row in df.iterrows()]

    # Build dynamic SQL
    cols_str = ', '.join(col_names)
    insert_sql = f"INSERT INTO bronze.lga_g02_raw({cols_str}) VALUES %s"

    # Execute bulk insert
    try:
        execute_values(cursor, insert_sql, tuple_values, page_size=5000)
        conn_ps.commit()
        logging.info(f" Successfully inserted {len(df)} records into bronze.lga_g02_raw .")
    except Exception as e:
        conn_ps.rollback()
        logging.error(f" Error inserting data: {e}")
        raise
    finally:
        cursor.close()
        conn_ps.close()

    # Move processed file to archive
    archive_folder = os.path.join(CENSUS, 'archive')
    os.makedirs(archive_folder, exist_ok=True)
    archive_path = os.path.join(archive_folder, '2016Census_G02_NSW_LGA.csv')
    shutil.move(sub_category_file_path, archive_path)
    logging.info(f" File moved to archive: {archive_path}")

    return None


def import_load_lga_code_raw_func(**kwargs):

    """
    Loads NSW_LGA_CODE.csv into bronze.lga_code_raw table.
    Reads the CSV dynamically (no manual column names) and archives it after loading.
    """

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    cursor = conn_ps.cursor()

    # Define file paths
    file_path = os.path.join(CODE, "NSW_LGA_CODE.csv")

    if not os.path.exists(file_path):
        logging.info(f"No file found: {file_path}")
        return None

    # Read CSV file
    df = pd.read_csv(file_path)
    if df.empty:
        logging.info("CSV file is empty. Nothing to load.")
        return None

    # Get columns dynamically
    col_names = df.columns.tolist()
    logging.info(f"Detected columns: {col_names}")

    # Prepare values as tuples for insertion
    tuple_values = [tuple(row[col] for col in col_names) for _, row in df.iterrows()]
    cols_str = ', '.join(col_names)
    insert_sql = f"INSERT INTO bronze.lga_code_raw ({cols_str}) VALUES %s"

    # Execute bulk insert
    try:
        execute_values(cursor, insert_sql, tuple_values, page_size=5000)
        conn_ps.commit()
        logging.info(f" Successfully inserted {len(df)} records into bronze.lga_code_raw.")
    except Exception as e:
        conn_ps.rollback()
        logging.error(f" Error inserting data: {e}")
        raise
    finally:
        cursor.close()
        conn_ps.close()

    # Move processed file to archive
    archive_folder = os.path.join(CODE, "archive")
    os.makedirs(archive_folder, exist_ok=True)
    archive_path = os.path.join(archive_folder, "NSW_LGA_CODE.csv")
    shutil.move(file_path, archive_path)
    logging.info(f" Archived NSW_LGA_CODE.csv to {archive_path}")

    return None



def import_load_lga_suburb_raw_func(**kwargs):
    """
    Loads NSW_LGA_SUBURB.csv into bronze.lga_suburb_raw table.
    Cleans headers, drops empty 'Unnamed' columns, and performs bulk insert safely.
    """

    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    cursor = conn_ps.cursor()

    file_path = os.path.join(CODE, "NSW_LGA_SUBURB.csv")

    if not os.path.exists(file_path):
        logging.info(f"No file found: {file_path}")
        return None

    df = pd.read_csv(file_path)
    if df.empty:
        logging.info("CSV file is empty. Nothing to load.")
        return None

    # ==========================================================
    # 🧹 Clean up columns
    # ==========================================================
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df.columns = [
        str(c).strip()
        .replace(':', '_')
        .replace(' ', '_')
        .replace('-', '_')
        .replace('/', '_')
        .lower()  # Force lowercase to match Postgres schema
        for c in df.columns
    ]

    col_names = df.columns.tolist()
    logging.info(f"Cleaned columns: {col_names}")

    # ==========================================================
    #  Prepare data for bulk insert
    # ==========================================================
    tuple_values = [tuple(row) for row in df.to_numpy()]
    cols_str = ', '.join(col_names)

    insert_sql = f"INSERT INTO bronze.lga_suburb_raw ({cols_str}) VALUES %s"

    try:
        execute_values(cursor, insert_sql, tuple_values, page_size=5000)
        conn_ps.commit()
        logging.info(f" Inserted {len(df)} records into bronze.lga_suburb_raw.")
    except Exception as e:
        conn_ps.rollback()
        logging.error(f" Error inserting data: {e}")
        raise
    finally:
        cursor.close()
        conn_ps.close()

    # Archive processed file
    archive_folder = os.path.join(CODE, "archive")
    os.makedirs(archive_folder, exist_ok=True)
    archive_path = os.path.join(archive_folder, "NSW_LGA_SUBURB.csv")
    shutil.move(file_path, archive_path)
    logging.info(f" Archived NSW_LGA_SUBURB.csv to {archive_path}")

    return None

#########################################################
#
#   DAG Operator Setup
#
#########################################################

import_load_airbnb_task = PythonOperator(
    task_id="import_load_airbnb_id",
    python_callable=import_load_airbnb_func,
    provide_context=True,
    dag=dag
)


import_load_2016Census_G01_NSW_LGA_raw_task = PythonOperator(
    task_id="import_load_2016Census_G01_NSW_LGA_raw_id",
    python_callable=import_load_2016Census_G01_NSW_LGA_raw_func,
    provide_context=True,
    dag=dag
)

import_load_2016Census_G02_NSW_LGA_raw_task = PythonOperator(
    task_id="import_load_2016Census_G02_NSW_LGA_raw_id",
    python_callable=import_load_2016Census_G02_NSW_LGA_raw_func,
    provide_context=True,
    dag=dag
)

import_load_lga_code_raw_task = PythonOperator(
    task_id="import_load_lga_code_raw_id",
    python_callable=import_load_lga_code_raw_func,
    provide_context=True,
    dag=dag
)

import_load_lga_suburb_raw_task = PythonOperator(
    task_id="import_load_lga_suburb_raw_id",
    python_callable=import_load_lga_suburb_raw_func,
    provide_context=True,
    dag=dag
)

# Task Dependencies
[import_load_2016Census_G01_NSW_LGA_raw_task, import_load_2016Census_G02_NSW_LGA_raw_task, import_load_lga_code_raw_task, import_load_lga_suburb_raw_task] >> import_load_airbnb_task
