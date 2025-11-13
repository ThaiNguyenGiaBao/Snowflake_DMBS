from celery import Celery
import pydicom
import snowflake.connector

from .command.index import command_service
from .command.index import (
    MERGE_PATIENT,
    MERGE_STUDY,
    MERGE_SERIES,
    MERGE_INSTANCE,
)

# configure broker and result backend
app = Celery(
    "dicom_loader",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1"
)

# Optional: configuration settings
app.conf.update(
    task_routes={
        "tasks.load_one_file_to_snowflake": {"queue": "dicom_load_queue"},
    },
    task_acks_late=True,
    task_retry_kwargs={"max_retries": 3, "countdown": 10},  # etc
)




conn = snowflake.connector.connect(
    user="tngiabao",
    password="Giabao210265210265",
    account="nq21061.ap-southeast-1",
    warehouse="COMPUTE_WH",
    database="DBMS",
    schema="PUBLIC",
)
cur = conn.cursor()

  
@app.task
def load_one_file_to_snowflake(dicom_path: str):
    print("Processing file:", dicom_path)
    ds = pydicom.dcmread(dicom_path, force=True)

    patient_id = dicom_path.split("/")[6] 
    
    patient_row = command_service.extract_patient(ds, patient_id)
    patient_row = list(str(attr) for attr in patient_row)
    study_row   = command_service.extract_study(ds, patient_id)
    study_row   = list(str(attr) for attr in study_row)
    series_row  = command_service.extract_series(ds)
    series_row  = list(str(attr) for attr in series_row)
    instance_row= command_service.extract_instance(ds, dicom_path)
    instance_row= list(str(attr) for attr in instance_row)
        
    cur.execute(MERGE_PATIENT, patient_row)
    cur.execute(MERGE_STUDY, study_row)
    cur.execute(MERGE_SERIES, series_row)
    cur.execute(MERGE_INSTANCE, instance_row)
    
    conn.commit()
    print("Upsert OK for:", dicom_path)
