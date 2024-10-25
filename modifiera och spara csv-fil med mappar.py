from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
import pandas as pd
import os
from datetime import datetime

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
    'save_multiple_csv_with_folders_to_gcs_with_mime_type',
    default_args=default_args,
    description='Spara flera CSV-filer från specifika mappar till GCS med MIME-typ text/csv och ta bort kolumn',
    schedule_interval=None,  # Manuell körning
    tags=['csv', 'gcs', 'mime-type', 'multiple-files', 'folders']
)

# Funktion för att spara och ladda upp CSV-filer med MIME-typ text/csv, samt ta bort en specifik kolumn
def save_csv_with_mime_type(bucket_name, blob_name, df):
    # Ta bort kolumnen "DeliveryInstructions" om den finns
    if "DeliveryInstructions" in df.columns:
        df = df.drop(columns=["DeliveryInstructions"])
    
    # Spara filen lokalt
    local_file_path = f"/tmp/{os.path.basename(blob_name)}"
    df.to_csv(local_file_path, index=False, sep='|')

    # Anslut till Google Cloud Storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Ladda upp filen med rätt MIME-typ
    blob.upload_from_filename(local_file_path, content_type='text/csv')

    # Radera temporära filen
    os.remove(local_file_path)

    print(f"Filen {blob_name} har sparats i {bucket_name} som text/csv.")

# Funktion för att processa flera filer från mappar baserat på typ (t.ex. "Order", "Prenumeration")
def process_multiple_files_from_folders(**kwargs):
    bucket_name = 'if-historical-data-436508'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Definiera mapparna och filtyperna
    folders_and_filetypes = {
        'Order': 'Order.csv',
        'Pren': 'Pren.csv'
        # Lägg till fler mappar och filtyper om nödvändigt
    }

    # Iterera över mapparna och filtyperna
    for folder, file_pattern in folders_and_filetypes.items():
        blobs = bucket.list_blobs(prefix=f"{folder}/")  # Lista alla blobbar i respektive mapp

        for blob in blobs:
            # Kolla om filnamnet matchar mönstret, t.ex. "ÅÅÅÅMMDDOrder.csv"
            if file_pattern in blob.name:
                print(f"Bearbetar fil: {blob.name} i mappen {folder}")

                # Läs in CSV-filen från Google Cloud Storage
                df = pd.read_csv(f"gs://{bucket_name}/{blob.name}", sep='|')

                # Spara tillbaka filen efter att ha tagit bort kolumnen "DeliveryInstructions"
                save_csv_with_mime_type(bucket_name, blob.name, df)

# Airflow PythonOperator som kör funktionen
process_files_task = PythonOperator(
    task_id='process_multiple_files_from_folders_task',
    python_callable=process_multiple_files_from_folders,
    provide_context=True,
    dag=dag
)

# Sätt taskens ordning
process_files_task
