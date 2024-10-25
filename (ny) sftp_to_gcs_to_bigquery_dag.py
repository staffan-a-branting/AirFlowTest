# Denna DAG hämtar dagens filer från SMTP-servern 
# och anpassar dem till användbart format
# samt tar bort ej önsvärda kolumner (personinformation).
#
# Därefter läses datat in till respektive tabell i BigQuery.
#
# En liten del av händelserna loggas i en textfil
# kallad "dag_log_ÅÅÅÅMMDD.txt" i mappen "Log" i vår bucket "f-historical-data-436508"
#
# Staffan Branting XLENT BI för Interflora 2024-10-24


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage, bigquery
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
import paramiko
import os
import pandas as pd

# Standardinställningar för Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    'sftp_to_gcs_to_bigquery_dag_new',
    default_args=default_args,
    description='DAG som hämtar CSV-filer från SFTP, laddar till GCS och läser in till BigQuery',
    schedule_interval=None,  # Ingen schemalagd körning, endast manuell
    # schedule_interval='0 6 * * *',  # Schemaläggning: varje morgon kl 6
    tags=['sftp', 'gcs', 'bigquery', 'log', 'DailyLoad']
)

# Google Cloud-projekt, dataset och bucket-information
project_id = 'sales-statistics-436508'
dataset_id = 'sales_statistics_dataset'
bucket_name = 'if-historical-data-436508'

# Global variabel för dagens datum i formatet ÅÅÅÅMMDD
today_str = datetime.today().strftime('%Y%m%d')

# test
# today_str = "20231211"

# Mappning mellan filtyper och BigQuery-tabeller
file_to_table_map = {
    "Betalning":        "Betalning_Load",
    "Butik":            "Butik_Load",
    "Kund":             "Kund_Load",
    "Order":            "Order_Load",
    "Orderrad":         "Orderrad_Load",
    "Orderrad2":        "Orderrad2_Load",
    "Pren":             "Pren_Load",
    "PrenOrderrad":     "PrenOrderrad_Load",
    "PrenOrderrad2":    "PrenOrderrad2_Load",
    "Produkt":          "Produkt_Load",
    "Produkt2":         "Produkt2_Load",
}


# Funktion som söker efter och byter ut en sträng i en fil
def replace_separator(local_file_path, search_str, replace_str):
    with open(local_file_path, 'r') as file:
        file_data = file.read()

    # Byt ut söksträng mot sträng som ersätter den
    file_data = file_data.replace(search_str, replace_str)

    # Spara tillbaka ändrad fil
    with open(local_file_path, 'w') as file:
        file.write(file_data)

# Funktion som filtretar bort (eller väljer ut) specifika kolumner baserat på filtyp
def filter_columns(local_file_path):

    file_name = os.path.basename(local_file_path)

    df = pd.read_csv(local_file_path, delimiter='|')  # Use '|' as the delimiter

    # Kund.csv (delar av filen behövs):
    #   ID
    #   CustomerType
    #   FlowerClubMember
    #   Newsletter
    if "Kund" in file_name:
    # Behåll [ID, CustomerType, FlowerClubMember, Newsletter ZipCode, CountryCode]
        df_filtered = df[["Id", "CustomerType", "FlowerClubMember", "Newsletter", "ZipCode", "CountryCode"]]
        df = df_filtered

    # Pren.csv - ska inte ha med
    #   CustomerEmail
    #   GreetingCardText
    #   DeliveryInstructions
    if "Pren" in file_name and not "PrenOrderrad" in file_name:
        # Remove 'CustomerEmail', 'GreetingCardText' and 'DeliveryInstruction' for 'Pren' file
        if 'CustomerEmail' in df.columns:
            df.drop(columns=['CustomerEmail'], inplace=True)
        if 'GreetingCardText' in df.columns:
            df.drop(columns=['GreetingCardText'], inplace=True)
        if 'DeliveryInstructions' in df.columns:
            df.drop(columns=['DeliveryInstructions'], inplace=True)

    # Order.csv - ska inte ha med
    #   CustomerEmail
    #   GreetingCardText
    #   DeliveryInstructions
    if "Order" in file_name and not "Orderrad" in file_name and not "PrenOrderrad" in file_name:
        # Remove 'CustomerEmail' and 'GreetingCardText'for 'Order' file
        if 'CustomerEmail' in df.columns:
            df.drop(columns=['CustomerEmail'], inplace=True)
        if 'GreetingCardText' in df.columns:
            df.drop(columns=['GreetingCardText'], inplace=True)
        if 'DeliveryInstructions' in df.columns:
            df.drop(columns=['DeliveryInstructions'], inplace=True)

    # Spara tillbaka filen med '|' som eparator
    df.to_csv(local_file_path, index=False, sep='|')
    

# Funktion som lägger upp en kolumn 'skapad_datum' i filen
# Kolumnen innehåller det datum filen skapades i formatet ÅÅÅÅ-MM-DD
def add_creation_date_to_csv(local_file_path, file_type, file_name):

    # Läs in sakad-datum via filnamn (exempelvis "20241002" från "20241002Produkt.csv")
    creation_date = file_name[:8]  # Vi förutsätter att datumet finns i filnamnets första åtta tecken

    # Formatera det lästa datumet till önskad form (exempelvis '2024-10-01')
    creation_date_formatted = f"{creation_date[:4]}-{creation_date[4:6]}-{creation_date[6:]}"

    # Ladda CSV-filen till en DataFrame
    df = pd.read_csv(local_file_path, delimiter='|')  # Använd '|' som separator

    # Lägg till en kolumn 'skapad_datum' med det inlästa datumet
    df['skapad_datum'] = creation_date_formatted

    # Spara den ändrade CSV-filen
    df.to_csv(local_file_path, index=False, sep='|')  # Spara tillbaka med '|' som separator


# Funktion för att kontrollera om filen redan finns i GCS innan uppladdning
def upload_file_to_gcs_if_not_exists(bucket_name, destination_blob_name, local_file_path):
    # Initiera Google Cloud Storage-klienten
    storage_client = storage.Client()

    # Hämta referens till bucketen
    bucket = storage_client.bucket(bucket_name)

    # Kontrollera om filen redan finns i bucketen
    blob = bucket.blob(destination_blob_name)
    if blob.exists():
        print(f"File {destination_blob_name} already exists in bucket {bucket_name}, skipping upload.")
    else:
        # Om filen inte finns, ladda upp den
        blob.upload_from_filename(local_file_path)
        print(f"Uploaded {destination_blob_name} to bucket {bucket_name}")


# Funktion för att logga händelser i en Google Cloud Storage-bucket med daglig loggfil
def log_event(log_message, bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Dynamiskt filnamn baserat på dagens datum, exempel: logg_20241023.txt
    log_file_name = f'Log/dag_log_{datetime.today().strftime("%Y%m%d")}.txt'
    blob = bucket.blob(log_file_name)

    # Hämta befintlig loggfil (om den finns) och lägg till nya loggmeddelanden
    if blob.exists():
        log_content = blob.download_as_text(encoding='utf-8')
    else:
        log_content = ""

    # Lägg till nytt loggmeddelande
    log_content += f"{datetime.now()}: {log_message}\n"

    # Ladda upp loggfilen tillbaka till bucketen med korrekt UTF-8-kodning
    blob.upload_from_string(log_content, content_type='text/plain; charset=utf-8')



# Funktion för att hämta CSV-filer från SFTP och ladda dem till GCS
def fetch_csv_from_sftp(**kwargs):
    log_event("Startar hämtning av filer från SFTP", bucket_name)

    # Hämta anslutningsuppgifter från Airflow Connection
    sftp_conn = BaseHook.get_connection('sftp-connection-csvfiles')

    sftp_host = sftp_conn.host
    sftp_port = sftp_conn.port
    sftp_user = sftp_conn.login
    sftp_password = sftp_conn.password
    remote_dir = '/history'

    
    # SFTP-koppling
    transport = paramiko.Transport((sftp_host, sftp_port))
    transport.connect(username=sftp_user, password=sftp_password)
    sftp = paramiko.SFTPClient.from_transport(transport)


    # Lista filer på SFTP
    files = sftp.listdir(remote_dir)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Filtrera för att endast ta filer som börjar med det angivna datumet
    filtered_files = [file for file in files if file.startswith(today_str)]

    # for filename in files:
    for filename in filtered_files:
        # Kontrollera om filen innehåller dagens datum
        if filename.startswith(today_str) and filename.endswith('.csv'):
        
        # test
        # if filename.startswith("20231029Butik") and filename.endswith('.csv'):

            remote_file_path = f"{remote_dir}/{filename}"
            local_file_path = f"/tmp/{filename}"

            sftp.get(remote_file_path, local_file_path)

            # Replace || with # and filter columns
            # Ersätt || med | i tre steg
            replace_separator(local_file_path, "||", "###")
            replace_separator(local_file_path, "|", "")
            replace_separator(local_file_path, "###", "|")

            # Filtrera bort oönskade kolumner
            filter_columns(local_file_path)

            # Läs in namn på bucket-mapp baserat på filens typ (exempelvis "Order" utifrån filnamn 'Order/202401012Order.csv')
            file_type = filename[8:-4]  # Extahera filtyp från filnamn
            file_creation_date = filename[:8]
            destination_blob_name = f"{file_type}/{filename}"  # exempelvis 'Order/202401012Order.csv'

            # Lägg till kolumn med skapad-atum i filen
            add_creation_date_to_csv(local_file_path, file_type, filename)

            # Justering av filer av typen "Orderrad", "PrenOrderrad" samt "Produkt"
            # fram till och med 2024-04-29 såg filen ut så här
            # OrderId|LineItemId|VariantCode|Quantity|PlacedPrice|SelectedPrice|LineItemParent|OrigLineItemId|skapad_datum
            #
            # från och med 2024-04-30 såg filen ut så här
            # OrderId|LineItemId|VariantCode|Quantity|PlacedPrice|SelectedPrice|LineItemParent|OrigLineItemId|MemberCompensation|CO2|skapad_datum
            if (file_type == "Orderrad" or file_type == "PrenOrderrad" or file_type == "Produkt") and file_creation_date > "2024-04-29":
                file_type = file_type + "2"
                filename = file_creation_date + file_type + ".csv"
                
            destination_blob_name = f"{file_type}/{filename}"  # exempelvis 'Order/202401012Order.csv'

            # Kontrollera om filen redan finns i GCS innan uppladdning
            # och ladda upp den om den inte redan finns
            upload_file_to_gcs_if_not_exists(bucket_name, destination_blob_name, local_file_path)

            # ta bort den lokala filen
            os.remove(local_file_path)

            log_event(f"Laddade upp {filename} till bucket {bucket_name}", bucket_name)
    
    sftp.close()
    transport.close()

    log_event("Klar med hämtning av filer från SFTP", bucket_name)

# Funktion för att kontrollera om ett datum redan finns i BigQuery-tabellen
# genom att via SQL räkna antal rader i den tabell som har skapad_datum lika med sökt datum
def check_if_date_exists(table_id, project_id, dataset_id, creation_date):
    client = bigquery.Client(project=project_id)
    
    query = f"""
    SELECT COUNT(1) AS count
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE skapad_datum = '{creation_date}'
    """
    
    query_job = client.query(query)
    results = query_job.result()
    
    for row in results:
        if row.count > 0:
            return True  # Datumet finns redan
    return False  # Datumet finns inte, filen kan laddas

# Funktion för att ladda upp filen till BigQuery om datumet inte redan finns
def upload_to_bigquery_if_not_exists(bucket_name, file_name, file_date, table_id, project_id, dataset_id):
    # Extrahera datum från filnamnet, exempelvis "20240101" från "20240101Order.csv"
    creation_date = file_date
    formatted_date = f"{creation_date[:4]}-{creation_date[4:6]}-{creation_date[6:]}"  # Formatera till YYYY-MM-DD

    # Kontrollera om datumet redan finns i tabellen
    if check_if_date_exists(table_id, project_id, dataset_id, formatted_date):
        print(f"Data för datum {formatted_date} finns redan i tabell {table_id}, hoppar över fil {file_name}.")
        return

    # Om datumet inte finns, fortsätt att ladda filen till BigQuery
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    uri = f"gs://{bucket_name}/{file_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,  # Stäng av autodetect
        field_delimiter='|',  # Specifik separator: pipe-tecken
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        max_bad_records=10  # Tillåt upp till 10 felaktiga poster
    )

    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()

    log_event(f"Laddade upp {file_name} till tabell {table_id}", bucket_name)

# Funktion för att bearbeta alla filer från GCS
def load_csv_to_bigquery(**kwargs):
    log_event("Startar inläsning av CSV till BigQuery", bucket_name)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Lista alla blobbar med angivet prefix (t.ex. "20241021")
    # <<TODO>> lista bara de som har today_str som prefix
    # log_event(f"Listar blobs och testar på datum today_str = {today_str}", bucket_name)
    # blobs = bucket.list_blobs(prefix=today_str)
    # Extrahera mappnamnet (hela sökvägen utan filnamnet)
    # folder_name = os.path.dirname(file_name_with_path)
    blobs = bucket.list_blobs()

    for blob in blobs:

        file_name_with_path = blob.name
        file_name = os.path.basename(file_name_with_path)  # Extrahera bara filnamnet utan mappstruktur
        file_date = file_name[:8]  # Extrahera datumet från filnamnet (ÅÅÅÅMMDD)

        # ta bara med dagens csv-filer
        if (file_date == today_str):

            print(f"Behandlar fil {file_name} med datum {file_date}")

            # Hämta filtypen från filnamnet
            file_type = file_name[8:-4]

            if file_type in file_to_table_map:
                table_id = file_to_table_map[file_type]

                # Ladda upp filen om datumet inte redan finns
                upload_to_bigquery_if_not_exists(bucket_name, file_name_with_path, file_date, table_id, project_id, dataset_id)
            else:
                print(f"Ingen tabell associerad till filtyp {file_type}, filen behendlades inte...")

    log_event("Klar med inläsning av CSV till BigQuery", bucket_name)


# Task som hämtar filer från SFTP och laddar upp till GCS
fetch_sftp_files = PythonOperator(
    task_id='fetch_csv_from_sftp',
    python_callable=fetch_csv_from_sftp,
    provide_context=True,
    dag=dag
)

# Task som läser in filer från GCS till BigQuery
load_files_to_bq = PythonOperator(
    task_id='load_csv_to_bigquery',
    python_callable=load_csv_to_bigquery,
    provide_context=True,
    dag=dag
)

# Definiera task-flödet
fetch_sftp_files >> load_files_to_bq
