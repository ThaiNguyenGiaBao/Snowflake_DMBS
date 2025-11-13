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
    backend="redis://localhost:6379/1",
)

# Optional: configuration settings
app.conf.update(
    task_routes={
        "tasks.load_one_file_to_snowflake": {"queue": "dicom_load_queue"},
    },
    task_acks_late=True,
    task_retry_kwargs={"max_retries": 3, "countdown": 10},  # etc
)




SEEN_PATIENTS = set()  # patient_id
SEEN_STUDIES = set()  # study_instance_uid
SEEN_SERIES = set()  # series_instance_uid
SEEN_INSTANCES = set()  # sop_instance_uid


# @app.task
def load_one_file_to_snowflake(dicom_path: str):
    conn = snowflake.connector.connect(
    user="tngiabao",
    password="Giabao210265210265",
    account="nq21061.ap-southeast-1",
    warehouse="COMPUTE_WH",
    database="DBMS",
    schema="PUBLIC",
)
    cur = conn.cursor()
    
    print("Processing file:", dicom_path)
    ds = pydicom.dcmread(dicom_path, force=True)

    patient_id = dicom_path.split("/")[6]

    # ----------- extract rows (as tuples) -----------
    patient_tuple = command_service.extract_patient(ds, patient_id)
    study_tuple = command_service.extract_study(ds, patient_id)
    series_tuple = command_service.extract_series(ds)
    instance_tuple = command_service.extract_instance(ds, dicom_path)

    # IDs for dedupe (from tuple positions)
    patient_id_key = str(patient_tuple[0])  # patient_id
    study_instance_uid_key = str(study_tuple[0])  # StudyInstanceUID
    series_instance_uid_key = str(series_tuple[0])  # SeriesInstanceUID
    sop_instance_uid_key = str(instance_tuple[0])  # SOPInstanceUID

    # convert to list of strings for Snowflake MERGE params
    patient_row = [str(attr) for attr in patient_tuple]
    study_row = [str(attr) for attr in study_tuple]
    series_row = [str(attr) for attr in series_tuple]
    instance_row = [str(attr) for attr in instance_tuple]

    if patient_id_key not in SEEN_PATIENTS:
        cur.execute(MERGE_PATIENT, patient_row)
        SEEN_PATIENTS.add(patient_id_key)

    # STUDY
    if study_instance_uid_key not in SEEN_STUDIES:
        cur.execute(MERGE_STUDY, study_row)
        SEEN_STUDIES.add(study_instance_uid_key)

    # SERIES
    if series_instance_uid_key not in SEEN_SERIES:
        cur.execute(MERGE_SERIES, series_row)
        SEEN_SERIES.add(series_instance_uid_key)

    # INSTANCE
    if sop_instance_uid_key not in SEEN_INSTANCES:
        cur.execute(MERGE_INSTANCE, instance_row)
        SEEN_INSTANCES.add(sop_instance_uid_key)

    conn.commit()
    print("Upsert OK for:", dicom_path)
