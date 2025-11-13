from typing import Any, Optional, Tuple
import json
import json
import pydicom
from pydicom.datadict import tag_for_keyword
import snowflake.connector
from io import BytesIO

from pydicom.multival import MultiValue
from pydicom.valuerep import DSfloat, IS

from pydicom import dcmread

# import numpy as np
# from PIL import Image
# from ..minio.index import minio_service


class Command:
    def __init__(self):
        print("Init command service")

    def extract_patient(self, ds, patient_id) -> Tuple:
        return (
            patient_id,
            self.get(ds, "PatientName"),
            self.get(ds, "PatientBirthDate"),
            self.get(ds, "PatientSex"),
            self.get(ds, "PatientAge"),
            self.get(ds, "PatientSize"),
            self.get(ds, "PatientWeight"),
            self.get(ds, "PatientIdentityRemoved"),
            self.get(ds, "DeidentificationMethod"),
        )

    def extract_study(self, ds, patient_id) -> Tuple:
        return (
            self.get(ds, "StudyInstanceUID"),
            patient_id,
            self.get(ds, "AccessionNumber"),
            self.get(ds, "StudyDate"),
            self.get(ds, "StudyTime"),
            self.get(ds, "StudyDescription"),
            self.get(ds, "RequestedProcedureDescription"),
            self.get(ds, "PerformingPhysicianName"),
            self.get(ds, "PerformedProcedureStepID"),
            self.get(ds, "PerformedProcedureStepDescription"),
            self.get(ds, "PerformedProcedureStepStartDate"),
            self.get(ds, "PerformedProcedureStepStartTime"),
        )

    def extract_series(self, ds) -> Tuple:
        return (
            self.get(ds, "SeriesInstanceUID"),
            self.get(ds, "StudyInstanceUID"),
            self.get(ds, "Modality"),
            self.as_int(self.get(ds, "SeriesNumber")),
            self.get(ds, "SeriesDate"),
            self.get(ds, "SeriesTime"),
            self.get(ds, "SeriesDescription"),
            self.get(ds, "BodyPartExamined"),
            self.get(ds, "PatientPosition"),
            self.get(ds, "Manufacturer"),
            self.get(ds, "ManufacturerModelName"),
            self.get(ds, "SoftwareVersions"),
        )

    def extract_instance(self, ds, dicom_path) -> Tuple:
        rows = self.as_int(self.get(ds, "Rows"))
        cols = self.as_int(self.get(ds, "Columns"))

        # bytes_data = self.dicom_to_png_bytes(dicom_path)
        # file_name = dicom_path.split("/")[-1].replace(".ima", ".png")
        # minio_service.upload_image(file_name, bytes_data)
        image_url = "image_url_placeholder"

        return (
            self.get(ds, "SOPInstanceUID"),
            self.get(ds, "SeriesInstanceUID"),
            self.get(ds, "SOPClassUID"),
            self.as_int(self.get(ds, "InstanceNumber")),
            self.to_variant(self.get(ds, "ImageType")),
            self.get(ds, "InstanceCreationDate"),
            self.get(ds, "InstanceCreationTime"),
            self.get(ds, "AcquisitionDate"),
            self.get(ds, "AcquisitionTime"),
            self.get(ds, "ContentDate"),
            self.get(ds, "ContentTime"),
            self.to_variant(self.get(ds, "ImagePositionPatient")),
            self.to_variant(self.get(ds, "ImageOrientationPatient")),
            self.as_float(self.get(ds, "SliceLocation")),
            self.get(ds, "FrameOfReferenceUID"),
            rows,
            cols,
            self.to_variant(self.get(ds, "PixelSpacing")),
            self.as_float(self.get(ds, "SliceThickness")),
            self.as_float(self.get(ds, "SpacingBetweenSlices") or 0.0),
            self.as_int(self.get(ds, "BitsAllocated")),
            self.as_int(self.get(ds, "BitsStored")),
            self.as_int(self.get(ds, "HighBit")),
            self.as_int(self.get(ds, "PixelRepresentation")),
            self.get(ds, "PhotometricInterpretation"),
            self.as_int(self.get(ds, "SmallestImagePixelValue")),
            self.as_int(self.get(ds, "LargestImagePixelValue")),
            self.to_variant(self.get(ds, "WindowCenter")),
            self.to_variant(self.get(ds, "WindowWidth")),
            self.as_float(self.get(ds, "RepetitionTime")),
            self.as_float(self.get(ds, "EchoTime")),
            self.as_float(self.get(ds, "NumberOfAverages")),
            self.as_float(self.get(ds, "ImagingFrequency")),
            self.get(ds, "ImagedNucleus"),
            self.as_int(self.get(ds, "EchoNumbers")),
            self.as_float(self.get(ds, "MagneticFieldStrength")),
            self.as_int(self.get(ds, "NumberOfPhaseEncodingSteps")),
            self.as_int(self.get(ds, "EchoTrainLength")),
            self.as_float(self.get(ds, "PercentSampling")),
            self.as_float(self.get(ds, "PercentPhaseFieldOfView")),
            self.as_float(self.get(ds, "PixelBandwidth")),
            self.to_variant(self.get(ds, "AcquisitionMatrix")),
            self.get(ds, "InPlanePhaseEncodingDirection"),
            self.as_float(self.get(ds, "FlipAngle")),
            self.get(ds, "VariableFlipAngleFlag"),
            self.as_float(self.get(ds, "SAR")),
            self.as_float(self.get(ds, "dBdt")),
            self.get(ds, "ScanningSequence"),
            self.to_variant(self.get(ds, "SequenceVariant")),
            self.get(ds, "ScanOptions"),
            self.get(ds, "MRAcquisitionType"),
            self.get(ds, "SequenceName"),
            self.get(ds, "AngioFlag"),
        )

    # ----------------------------
    # Helpers inside the class
    # ----------------------------
    def get(self, ds, keyword: str, default=None):
        try:
            if hasattr(ds, keyword):
                v = getattr(ds, keyword)
                if hasattr(v, "to_json"):
                    return v.to_json()
                if isinstance(v, (list, tuple)):
                    return list(v)
                return v
            return default
        except Exception:
            return default

    def to_variant(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, MultiValue):
            return json.dumps(
                [self.to_variant(v) if isinstance(v, MultiValue) else v for v in value]
            )
        if isinstance(value, (DSfloat, IS)):
            return json.dumps(float(value))
        if isinstance(value, (str, int, float, bool)):
            return json.dumps(value)
        return json.dumps(str(value))

    def as_int(self, value) -> Optional[int]:
        try:
            return int(value)
        except Exception:
            return None

    def as_float(self, value) -> Optional[float]:
        try:
            return float(value)
        except Exception:
            return None

    def as_str(self, value) -> Optional[str]:
        try:
            return str(value)
        except Exception:
            return None

    def get_public_url(self, file_name: str) -> str:
        return f"http://222.254.204.38:9001/api/v1/buckets/dbms/objects/download?preview=true&prefix={file_name}&version_id=null"


MERGE_PATIENT = """
MERGE INTO dicom_patient t
USING (SELECT %s AS patient_id,
              %s AS patient_name,
              %s AS patient_birth_date,
              %s AS patient_sex,
              %s AS patient_age,
              %s AS patient_size,
              %s AS patient_weight,
              %s AS patient_identity_removed,
              %s AS deidentification_method) s
ON t.patient_id = s.patient_id
WHEN MATCHED THEN UPDATE SET
  patient_name=s.patient_name,
  patient_birth_date=s.patient_birth_date,
  patient_sex=s.patient_sex,
  patient_age=s.patient_age,
  patient_size=s.patient_size,
  patient_weight=s.patient_weight,
  patient_identity_removed=s.patient_identity_removed,
  deidentification_method=s.deidentification_method
WHEN NOT MATCHED THEN INSERT (
  patient_id, patient_name, patient_birth_date, patient_sex, patient_age, patient_size,
  patient_weight, patient_identity_removed, deidentification_method
) VALUES (
  s.patient_id, s.patient_name, s.patient_birth_date, s.patient_sex, s.patient_age, s.patient_size,
  s.patient_weight, s.patient_identity_removed, s.deidentification_method
);
"""

MERGE_STUDY = """
MERGE INTO dicom_study t
USING (SELECT %s AS study_instance_uid,
              %s AS patient_id,
              %s AS accession_number,
              %s AS study_date,
              %s AS study_time,
              %s AS study_description,
              %s AS requested_procedure_description,
              %s AS performing_physician_name,
              %s AS performed_procedure_step_id,
              %s AS performed_procedure_step_description,
              %s AS performed_procedure_step_start_date,
              %s AS performed_procedure_step_start_time) s
ON t.study_instance_uid = s.study_instance_uid
WHEN MATCHED THEN UPDATE SET
  patient_id=s.patient_id,
  accession_number=s.accession_number,
  study_date=s.study_date,
  study_time=s.study_time,
  study_description=s.study_description,
  requested_procedure_description=s.requested_procedure_description,
  performing_physician_name=s.performing_physician_name,
  performed_procedure_step_id=s.performed_procedure_step_id,
  performed_procedure_step_description=s.performed_procedure_step_description,
  performed_procedure_step_start_date=s.performed_procedure_step_start_date,
  performed_procedure_step_start_time=s.performed_procedure_step_start_time
WHEN NOT MATCHED THEN INSERT (
  study_instance_uid, patient_id, accession_number, study_date, study_time, study_description,
  requested_procedure_description, performing_physician_name, performed_procedure_step_id,
  performed_procedure_step_description, performed_procedure_step_start_date, performed_procedure_step_start_time
) VALUES (
  s.study_instance_uid, s.patient_id, s.accession_number, s.study_date, s.study_time, s.study_description,
  s.requested_procedure_description, s.performing_physician_name, s.performed_procedure_step_id,
  s.performed_procedure_step_description, s.performed_procedure_step_start_date, s.performed_procedure_step_start_time
);
"""

MERGE_SERIES = """
MERGE INTO dicom_series t
USING (SELECT %s AS series_instance_uid,
              %s AS study_instance_uid,
              %s AS modality,
              %s AS series_number,
              %s AS series_date,
              %s AS series_time,
              %s AS series_description,
              %s AS body_part_examined,
              %s AS patient_position,
              %s AS manufacturer,
              %s AS manufacturer_model_name,
              %s AS software_versions) s
ON t.series_instance_uid = s.series_instance_uid
WHEN MATCHED THEN UPDATE SET
  study_instance_uid=s.study_instance_uid,
  modality=s.modality,
  series_number=s.series_number,
  series_date=s.series_date,
  series_time=s.series_time,
  series_description=s.series_description,
  body_part_examined=s.body_part_examined,
  patient_position=s.patient_position,
  manufacturer=s.manufacturer,
  manufacturer_model_name=s.manufacturer_model_name,
  software_versions=s.software_versions
WHEN NOT MATCHED THEN INSERT (
  series_instance_uid, study_instance_uid, modality, series_number, series_date, series_time,
  series_description, body_part_examined, patient_position, manufacturer, manufacturer_model_name, software_versions
) VALUES (
  s.series_instance_uid, s.study_instance_uid, s.modality, s.series_number, s.series_date, s.series_time,
  s.series_description, s.body_part_examined, s.patient_position, s.manufacturer, s.manufacturer_model_name, s.software_versions
);
"""

MERGE_INSTANCE = """
MERGE INTO dicom_instance t
USING (
  SELECT
    %s AS sop_instance_uid,
    %s AS series_instance_uid,

    %s AS sop_class_uid,
    %s AS instance_number,
    %s AS image_type,

    %s AS instance_creation_date,
    %s AS instance_creation_time,
    %s AS acquisition_date,
    %s AS acquisition_time,
    %s AS content_date,
    %s AS content_time,

    %s AS image_position_patient,
    %s AS image_orientation_patient,
    %s AS slice_location,
    %s AS frame_of_reference_uid,

    %s AS row_s,
    %s AS column_s,
    %s AS pixel_spacing,
    %s AS slice_thickness,
    %s AS spacing_between_slices,

    %s AS bits_allocated,
    %s AS bits_stored,
    %s AS high_bit,
    %s AS pixel_representation,
    %s AS photometric_interpretation,
    %s AS smallest_image_pixel_value,
    %s AS largest_image_pixel_value,

    %s AS window_center,
    %s AS window_width,

    %s AS repetition_time,
    %s AS echo_time,
    %s AS number_of_averages,
    %s AS imaging_frequency,
    %s AS imaged_nucleus,
    %s AS echo_numbers,
    %s AS magnetic_field_strength,
    %s AS number_of_phase_encoding_steps,
    %s AS echo_train_length,
    %s AS percent_sampling,
    %s AS percent_phase_field_of_view,
    %s AS pixel_bandwidth,
    %s AS acquisition_matrix,
    %s AS in_plane_phase_encoding_direction,
    %s AS flip_angle,
    %s AS variable_flip_angle_flag,
    %s AS sar,
    %s AS dbdt,
    %s AS scanning_sequence,
    %s AS sequence_variant,
    %s AS scan_options,
    %s AS mr_acquisition_type,
    %s AS sequence_name,
    %s AS angio_flag,
) s
ON t.sop_instance_uid = s.sop_instance_uid
WHEN MATCHED THEN UPDATE SET
  series_instance_uid = s.series_instance_uid,
  sop_class_uid = s.sop_class_uid,
  instance_number = s.instance_number,
  image_type = PARSE_JSON(s.image_type),
  instance_creation_date = s.instance_creation_date,
  instance_creation_time = s.instance_creation_time,
  acquisition_date = s.acquisition_date,
  acquisition_time = s.acquisition_time,
  content_date = s.content_date,
  content_time = s.content_time,
  image_position_patient = PARSE_JSON(s.image_position_patient),
  image_orientation_patient = PARSE_JSON(s.image_orientation_patient),
  slice_location = s.slice_location,
  frame_of_reference_uid = s.frame_of_reference_uid,
  row_s = s.row_s,
  column_s = s.column_s,
  pixel_spacing = PARSE_JSON(s.pixel_spacing),
  slice_thickness = s.slice_thickness,
  spacing_between_slices = s.spacing_between_slices,
  bits_allocated = s.bits_allocated,
  bits_stored = s.bits_stored,
  high_bit = s.high_bit,
  pixel_representation = s.pixel_representation,
  photometric_interpretation = s.photometric_interpretation,
  smallest_image_pixel_value = s.smallest_image_pixel_value,
  largest_image_pixel_value = s.largest_image_pixel_value,
  window_center = PARSE_JSON(s.window_center),
  window_width = PARSE_JSON(s.window_width),
  repetition_time = s.repetition_time,
  echo_time = s.echo_time,
  number_of_averages = s.number_of_averages,
  imaging_frequency = s.imaging_frequency,
  imaged_nucleus = s.imaged_nucleus,
  echo_numbers = s.echo_numbers,
  magnetic_field_strength = s.magnetic_field_strength,
  number_of_phase_encoding_steps = s.number_of_phase_encoding_steps,
  echo_train_length = s.echo_train_length,
  percent_sampling = s.percent_sampling,
  percent_phase_field_of_view = s.percent_phase_field_of_view,
  pixel_bandwidth = s.pixel_bandwidth,
  acquisition_matrix = PARSE_JSON(s.acquisition_matrix),
  in_plane_phase_encoding_direction = s.in_plane_phase_encoding_direction,
  flip_angle = s.flip_angle,
  variable_flip_angle_flag = s.variable_flip_angle_flag,
  sar = s.sar,
  dbdt = s.dbdt,
  scanning_sequence = s.scanning_sequence,
  sequence_variant = PARSE_JSON(s.sequence_variant),
  scan_options = s.scan_options,
  mr_acquisition_type = s.mr_acquisition_type,
  sequence_name = s.sequence_name,
  angio_flag = s.angio_flag
WHEN NOT MATCHED THEN INSERT (
  sop_instance_uid, series_instance_uid, sop_class_uid, instance_number, image_type,
  instance_creation_date, instance_creation_time, acquisition_date, acquisition_time, content_date, content_time,
  image_position_patient, image_orientation_patient, slice_location, frame_of_reference_uid,
  row_s, column_s, pixel_spacing, slice_thickness, spacing_between_slices,
  bits_allocated, bits_stored, high_bit, pixel_representation, photometric_interpretation,
  smallest_image_pixel_value, largest_image_pixel_value,
  window_center, window_width,
  repetition_time, echo_time, number_of_averages, imaging_frequency, imaged_nucleus, echo_numbers,
  magnetic_field_strength, number_of_phase_encoding_steps, echo_train_length, percent_sampling,
  percent_phase_field_of_view, pixel_bandwidth, acquisition_matrix, in_plane_phase_encoding_direction,
  flip_angle, variable_flip_angle_flag, sar, dbdt, scanning_sequence, sequence_variant, scan_options,
  mr_acquisition_type, sequence_name, angio_flag
) VALUES (
  s.sop_instance_uid, s.series_instance_uid, s.sop_class_uid, s.instance_number, PARSE_JSON(s.image_type),
  s.instance_creation_date, s.instance_creation_time, s.acquisition_date, s.acquisition_time, s.content_date, s.content_time,
  PARSE_JSON(s.image_position_patient), PARSE_JSON(s.image_orientation_patient), s.slice_location, s.frame_of_reference_uid,
  s.row_s, s.column_s, PARSE_JSON(s.pixel_spacing), s.slice_thickness, s.spacing_between_slices,
  s.bits_allocated, s.bits_stored, s.high_bit, s.pixel_representation, s.photometric_interpretation,
  s.smallest_image_pixel_value, s.largest_image_pixel_value,
  PARSE_JSON(s.window_center), PARSE_JSON(s.window_width),
  s.repetition_time, s.echo_time, s.number_of_averages, s.imaging_frequency, s.imaged_nucleus, s.echo_numbers,
  s.magnetic_field_strength, s.number_of_phase_encoding_steps, s.echo_train_length, s.percent_sampling,
  s.percent_phase_field_of_view, s.pixel_bandwidth, PARSE_JSON(s.acquisition_matrix), s.in_plane_phase_encoding_direction,
  s.flip_angle, s.variable_flip_angle_flag, s.sar, s.dbdt, s.scanning_sequence, PARSE_JSON(s.sequence_variant), s.scan_options,
  s.mr_acquisition_type, s.sequence_name, s.angio_flag
);
"""

command_service = Command()
