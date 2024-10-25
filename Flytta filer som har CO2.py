from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from datetime import datetime
import os

# Standardinställningar för Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Definiera Airflow DAG
dag = DAG(
    'move_and_rename_files_in_gcs',
    default_args=default_args,
    description='Flyttar och döper om filer baserat på datumvillkor',
    schedule_interval=None,  # Manuell körning
    tags=['gcs', 'move', 'rename', 'StaffanTest']
)

# Funktion för att flytta och döpa om filer i Google Cloud Storage
def move_and_rename_files(bucket_name, source_folder, target_folder):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Lista alla blobbar i källmappen (Produkt)
    blobs = bucket.list_blobs(prefix=source_folder)

    for blob in blobs:
        file_name = blob.name.split('/')[-1]  # Extrahera filnamnet utan mappstrukturen
        if "Produkt.csv" in file_name:
            # Extrahera datumet (ÅÅÅÅMMDD) från filnamnet
            file_date_str = file_name[:8]
            file_date = datetime.strptime(file_date_str, '%Y%m%d').date()

            # Definiera tröskeldatumet (2024-04-29)
            threshold_date = datetime.strptime('20240429', '%Y%m%d').date()

            # Kontrollera om filens datum är större än tröskeldatumet
            if file_date > threshold_date:
                # Nytt filnamn där "Produkt" byts ut till "Produkt2"
                new_file_name = file_name.replace("Produkt.csv", "Produkt2.csv")

                # Kopiera filen till målplatsen (Produkt2) med nytt namn
                new_blob = bucket.blob(f"{target_folder}/{new_file_name}")
                new_blob.rewrite(blob)

                # Ta bort den gamla filen
                blob.delete()

                print(f"Flyttade och döpte om {file_name} till {new_file_name} i {target_folder}")

# Airflow PythonOperator som kör funktionen
move_files_task = PythonOperator(
    task_id='move_and_rename_files_task',
    python_callable=move_and_rename_files,
    op_kwargs={
        'bucket_name': 'if-historical-data-436508',
        'source_folder': 'Produkt',
        'target_folder': 'Produkt2'
    },
    dag=dag
)

# Sätt taskens ordning
move_files_task
