-- 1) PATIENT
CREATE OR REPLACE TABLE dicom_patient (
  patient_id              STRING          NOT NULL,  -- (0010,0020)
  patient_name            STRING,                    -- (0010,0010)
  patient_birth_date      STRING,                    -- (0010,0030)
  patient_sex             STRING,                    -- (0010,0040)
  patient_age             STRING,                    -- (0010,1010)
  patient_size            STRING,                    -- (0010,1020)
  patient_weight          STRING,                    -- (0010,1030)
  patient_identity_removed STRING,                   -- (0012,0062)
  deidentification_method STRING,                    -- (0012,0063)
  CONSTRAINT pk_dicom_patient PRIMARY KEY (patient_id)
);

-- 2) STUDY
CREATE OR REPLACE TABLE dicom_study (
  study_instance_uid                    STRING NOT NULL,  -- (0020,000D)
  patient_id                            STRING NOT NULL REFERENCES dicom_patient(patient_id),
  accession_number                      STRING,           -- (0008,0050)
  study_date                            STRING,           -- (0008,0020)
  study_time                            STRING,           -- (0008,0030)
  study_description                     STRING,           -- (0008,1030)
  requested_procedure_description       STRING,           -- (0032,1060)
  performing_physician_name             STRING,           -- (0008,1050)
  performed_procedure_step_id           STRING,           -- (0040,0253)
  performed_procedure_step_description  STRING,           -- (0040,0254)
  performed_procedure_step_start_date   STRING,           -- (0040,0244)
  performed_procedure_step_start_time   STRING,           -- (0040,0245)
  CONSTRAINT pk_dicom_study PRIMARY KEY (study_instance_uid)
);

-- 3) SERIES
CREATE OR REPLACE TABLE dicom_series (
  series_instance_uid       STRING NOT NULL,           -- (0020,000E)
  study_instance_uid        STRING NOT NULL REFERENCES dicom_study(study_instance_uid),
  modality                  STRING,                    -- (0008,0060)
  series_number             INTEGER,                   -- (0020,0011) use STRING if you prefer
  series_date               STRING,                    -- (0008,0021)
  series_time               STRING,                    -- (0008,0031)
  series_description        STRING,                    -- (0008,103E)
  body_part_examined        STRING,                    -- (0018,0015)
  patient_position          STRING,                    -- (0018,5100)
  manufacturer              STRING,                    -- (0008,0070)
  manufacturer_model_name   STRING,                    -- (0008,1090)
  software_versions         STRING,                    -- (0018,1020)
  CONSTRAINT pk_dicom_series PRIMARY KEY (series_instance_uid)
);

-- 4) INSTANCE (IMAGE)
CREATE OR REPLACE TABLE dicom_instance (
  sop_instance_uid                 STRING NOT NULL,     -- (0008,0018)
  series_instance_uid              STRING NOT NULL REFERENCES dicom_series(series_instance_uid),

  sop_class_uid                    STRING,              -- (0008,0016)
  instance_number                  INTEGER,             -- (0020,0013)
  image_type                       VARIANT,             -- (0008,0008) array

  instance_creation_date           STRING,              -- (0008,0012)
  instance_creation_time           STRING,              -- (0008,0013)
  acquisition_date                 STRING,              -- (0008,0022)
  acquisition_time                 STRING,              -- (0008,0032)
  content_date                     STRING,              -- (0008,0023)
  content_time                     STRING,              -- (0008,0033)

  image_position_patient           VARIANT,             -- (0020,0032) [x,y,z]
  image_orientation_patient        VARIANT,             -- (0020,0037) [row_x,...,col_z]
  slice_location                   FLOAT,               -- (0020,1041)
  frame_of_reference_uid           STRING,              -- (0020,0052)

  row_s                             INTEGER,             -- (0028,0010)
  column_s                          INTEGER,             -- (0028,0011)
  pixel_spacing                    VARIANT,             -- (0028,0030)
  slice_thickness                  FLOAT,               -- (0018,0050)
  spacing_between_slices           FLOAT,               -- (0018,0088)

  bits_allocated                   INTEGER,             -- (0028,0100)
  bits_stored                      INTEGER,             -- (0028,0101)
  high_bit                         INTEGER,             -- (0028,0102)
  pixel_representation             INTEGER,             -- (0028,0103)
  photometric_interpretation       STRING,              -- (0028,0004)
  smallest_image_pixel_value       INTEGER,             -- (0028,0106)
  largest_image_pixel_value        INTEGER,             -- (0028,0107)

  window_center                    VARIANT,             -- (0028,1050) single or multi
  window_width                     VARIANT,             -- (0028,1051) single or multi

  -- Acquisition parameters (MR/general)
  repetition_time                  FLOAT,               -- TR (0018,0080)
  echo_time                        FLOAT,               -- TE (0018,0081)
  number_of_averages               FLOAT,               -- (0018,0083)
  imaging_frequency                FLOAT,               -- (0018,0084)
  imaged_nucleus                   STRING,              -- (0018,0085)
  echo_numbers                     INTEGER,             -- (0018,0086)
  magnetic_field_strength          FLOAT,               -- (0018,0087)
  number_of_phase_encoding_steps   INTEGER,             -- (0018,0089)
  echo_train_length                INTEGER,             -- (0018,0091)
  percent_sampling                 FLOAT,               -- (0018,0093)
  percent_phase_field_of_view      FLOAT,               -- (0018,0094)
  pixel_bandwidth                  FLOAT,               -- (0018,0095)
  acquisition_matrix               VARIANT,             -- (0018,1310)
  in_plane_phase_encoding_direction STRING,             -- (0018,1312)
  flip_angle                       FLOAT,               -- (0018,1314)
  variable_flip_angle_flag         STRING,              -- (0018,1315)
  sar                              FLOAT,               -- (0018,1316)
  dbdt                             FLOAT,               -- (0018,1318)
  scanning_sequence                STRING,              -- (0018,0020)
  sequence_variant                 VARIANT,             -- (0018,0021)
  scan_options                     STRING,              -- (0018,0022)
  mr_acquisition_type              STRING,              -- (0018,0023)
  sequence_name                    STRING,              -- (0018,0024)
  angio_flag                       STRING,              -- (0018,0025)
  transmit_coil_name               STRING,              -- (0018,1251)

  CONSTRAINT pk_dicom_instance PRIMARY KEY (sop_instance_uid)
);
