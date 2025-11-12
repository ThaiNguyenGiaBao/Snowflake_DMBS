import pandas as pd
from ..snowflake_service.index import snowflake_service

# Read Excel
df = pd.read_excel('/Users/baothainguyengia/Desktop/[btl]DBMS/src/utils/report.xlsx')
df = df.rename(columns={
    "Patient ID": "patient_id",
    "Clinician's Notes": "radiologist_report"
})

def quote_literal(value):
    """Safely escape a Python value into a Snowflake SQL string literal."""
    if value is None or pd.isna(value):
        return "NULL"
    # convert to str and escape single quotes
    s = str(value).replace("'", "''")
    return f"'{s}'"

# Build inline VALUES list
values = ",".join(
    f"('{str(row.patient_id)}', {quote_literal(str(row.radiologist_report))})"
    for _, row in df.iterrows()
)

sql = f"""
MERGE INTO dicom_study AS tgt
USING (
    SELECT column1 AS patient_id, column2 AS radiologist_report
    FROM VALUES {values}
) AS src
ON tgt.patient_id = src.patient_id
WHEN MATCHED THEN UPDATE SET tgt.radiologist_report = src.radiologist_report
"""

snowflake_service.execute(sql)
print("✅ Bulk update completed (no temp table)")
