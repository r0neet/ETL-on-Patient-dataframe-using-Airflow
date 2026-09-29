import os
import pandas as pd

from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator



# FILE PATHS


DATA_FILE = os.path.expanduser(
    "~/airflow/data/Group9.csv"
)

OUTPUT_DIR = os.path.expanduser(
    "~/airflow/output"
)

EXTRACTED_FILE = "/tmp/group9_extracted.pkl"
VALIDATED_FILE = "/tmp/group9_validated.pkl"
TRANSFORMED_FILE = "/tmp/group9_transformed.pkl"

FINAL_OUTPUT_FILE = os.path.join(
    OUTPUT_DIR,
    "group9_etl_output.csv"
)

SUMMARY_FILE = os.path.join(
    OUTPUT_DIR,
    "group9_etl_summary.txt"
)



# EXTRACT


def extract():

    print("\n========== EXTRACT ==========")

    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(
            f"Dataset not found: {DATA_FILE}"
        )

    df = pd.read_csv(DATA_FILE)

    print("Dataset read successfully.")
    print(f"Rows    : {len(df)}")
    print(f"Columns : {len(df.columns)}")

    print("\nColumns:")
    print(df.columns.tolist())

    # Save extracted data for the next task
    df.to_pickle(EXTRACTED_FILE)

    print("\nExtract completed successfully.")



# VALIDATE
# Four columns:
# 1. Blood_Type
# 2. Status
# 3. Age
# 4. Last_Visit_Date


def validate():

    print("\n========== VALIDATE ==========")

    if not os.path.exists(EXTRACTED_FILE):
        raise FileNotFoundError(
            "Extracted data was not found."
        )

    df = pd.read_pickle(EXTRACTED_FILE)

   
    # VALIDATION 1: BLOOD_TYPE
   

    print("\n--- Validation 1: Blood_Type ---")

    print("Original data type:")
    print(df["Blood_Type"].dtype)

    missing_blood_type = df["Blood_Type"].isna().sum()

    valid_blood_types = [
        "A+", "A-",
        "B+", "B-",
        "AB+", "AB-",
        "O+", "O-"
    ]

    invalid_blood_type = (
        ~df["Blood_Type"].isin(valid_blood_types)
    ).sum()

    print(
        f"Missing Blood_Type values: "
        f"{missing_blood_type}"
    )

    print(
        f"Invalid Blood_Type values: "
        f"{invalid_blood_type}"
    )

    if missing_blood_type > 0:
        raise ValueError(
            "Blood_Type validation failed: missing values found."
        )

    if invalid_blood_type > 0:
        raise ValueError(
            "Blood_Type validation failed: invalid blood group found."
        )

    print("Blood_Type validation passed.")

   
    # VALIDATION 2: STATUS
   

    print("\n--- Validation 2: Status ---")

    print("Original data type:")
    print(df["Status"].dtype)

    missing_status = df["Status"].isna().sum()

    # These are the statuses required by the transformation.
    # Other valid statuses may exist in the dataset.
    required_statuses = [
        "Under Treatment",
        "Recovered"
    ]

    invalid_status = (
        ~df["Status"].isin(
            required_statuses
        )
    ).sum()

    print(
        f"Missing Status values: "
        f"{missing_status}"
    )

    print(
        "Number of records whose Status is "
        f"not one of {required_statuses}: "
        f"{invalid_status}"
    )

    if missing_status > 0:
        raise ValueError(
            "Status validation failed: missing values found."
        )

    print(
        "Status column validation passed "
        "(required transformation statuses checked)."
    )

    
    # VALIDATION 3: AGE
   

    print("\n--- Validation 3: Age ---")

    print("Original data type:")
    print(df["Age"].dtype)

    df["Age"] = pd.to_numeric(
        df["Age"],
        errors="coerce"
    )

    missing_or_invalid_age = df["Age"].isna().sum()

    invalid_age_range = (
        (df["Age"] < 0) |
        (df["Age"] > 120)
    ).sum()

    print(
        f"Missing/invalid Age values: "
        f"{missing_or_invalid_age}"
    )

    print(
        f"Invalid Age range values: "
        f"{invalid_age_range}"
    )

    if missing_or_invalid_age > 0:
        raise ValueError(
            "Age validation failed."
        )

    if invalid_age_range > 0:
        raise ValueError(
            "Age validation failed: age must be between 0 and 120."
        )

    print("Age validation passed.")

    
    # VALIDATION 4: LAST_VISIT_DATE
    

    print("\n--- Validation 4: Last_Visit_Date ---")

    print("Original data type:")
    print(df["Last_Visit_Date"].dtype)

    df["Last_Visit_Date"] = pd.to_datetime(
        df["Last_Visit_Date"],
        errors="coerce"
    )

    invalid_dates = (
        df["Last_Visit_Date"].isna().sum()
    )

    print(
        f"Missing/invalid date values: "
        f"{invalid_dates}"
    )

    if invalid_dates > 0:
        raise ValueError(
            "Last_Visit_Date validation failed."
        )

    print(
        "Last_Visit_Date validation passed."
    )

   
    # DUPLICATE CHECK
  

    print("\n--- General Data Validation ---")

    duplicate_count = df.duplicated().sum()

    print(
        f"Duplicate rows: {duplicate_count}"
    )

    if duplicate_count > 0:
        df = df.drop_duplicates()
        print("Duplicate rows removed.")

    print("\nValidated data types:")

    print(
        df[
            [
                "Blood_Type",
                "Status",
                "Age",
                "Last_Visit_Date"
            ]
        ].dtypes
    )

    df.to_pickle(VALIDATED_FILE)

    print("\nValidation completed successfully.")



# TRANSFORM
# Find total patients:
# Blood_Type = A+ve
# AND
# Status = Under Treatment OR Recovered


def transform():

    print("\n========== TRANSFORM ==========")

    if not os.path.exists(VALIDATED_FILE):
        raise FileNotFoundError(
            "Validated data was not found."
        )

    df = pd.read_pickle(VALIDATED_FILE)

    # Required transformation
    selected_patients = df[
        (df["Blood_Type"] == "A+") &
        (
            df["Status"].isin(
                [
                    "Under Treatment",
                    "Recovered"
                ]
            )
        )
    ].copy()

    total_patients = len(selected_patients)

    print("\n--- Required Transformation ---")

    print(
        'Blood_Type = "A+ve" '
        'AND Status = "Under Treatment" '
        'OR "Recovered"'
    )

    print(
        f"\nTotal matching patients: "
        f"{total_patients}"
    )

    print("\nMatching patient records:")

    print(
        selected_patients[
            [
                "Patient_ID",
                "Patient_Name",
                "Age",
                "Gender",
                "Blood_Type",
                "Medical_Condition",
                "Last_Visit_Date",
                "Status"
            ]
        ]
    )

    # GroupBy / aggregation
    status_summary = (
        selected_patients
        .groupby("Status")
        .size()
        .reset_index(
            name="Patient_Count"
        )
    )

    print("\n--- GroupBy / Aggregation ---")
    print(status_summary)

    selected_patients[
        "Total_Matching_Patients"
    ] = total_patients

    selected_patients.to_pickle(
        TRANSFORMED_FILE
    )

    with open(
        "/tmp/group9_patient_count.txt",
        "w"
    ) as file:
        file.write(
            str(total_patients)
        )

    print("\nTransform completed successfully.")



# LOAD


def load():

    print("\n========== LOAD ==========")

    if not os.path.exists(TRANSFORMED_FILE):
        raise FileNotFoundError(
            "Transformed data was not found."
        )

    if not os.path.exists(
        "/tmp/group9_patient_count.txt"
    ):
        raise FileNotFoundError(
            "Patient count was not found."
        )

    df = pd.read_pickle(
        TRANSFORMED_FILE
    )

    with open(
        "/tmp/group9_patient_count.txt",
        "r"
    ) as file:
        total_patients = int(
            file.read().strip()
        )

    os.makedirs(
        OUTPUT_DIR,
        exist_ok=True
    )

    # Write final transformed data
    df.to_csv(
        FINAL_OUTPUT_FILE,
        index=False
    )

    # Write summary
    with open(
        SUMMARY_FILE,
        "w"
    ) as file:

        file.write(
            "GROUP 9 ETL RESULT\n"
        )

        file.write(
            "==================\n\n"
        )

        file.write(
            "Validation columns:\n"
        )

        file.write(
            "1. Blood_Type\n"
        )

        file.write(
            "2. Status\n"
        )

        file.write(
            "3. Age\n"
        )

        file.write(
            "4. Last_Visit_Date\n\n"
        )

        file.write(
            "Transformation condition:\n"
        )

        file.write(
            'Blood_Type = "A+ve"\n'
        )

        file.write(
            'AND Status = "Under Treatment" '
            'OR "Recovered"\n\n'
        )

        file.write(
            f"Total matching patients: "
            f"{total_patients}\n"
        )

    print(
        "\nFinal transformed data saved to:"
    )

    print(FINAL_OUTPUT_FILE)

    print(
        "\nETL summary saved to:"
    )

    print(SUMMARY_FILE)

    print(
        f"\nTOTAL MATCHING PATIENTS = "
        f"{total_patients}"
    )

    print("\nLoad completed successfully.")



# NOTIFICATION


def notification():

    print("\n========== NOTIFICATION ==========")

    print(
        "Group 9 ETL pipeline completed successfully."
    )

    print(
        "Notification stage completed "
        "(SMTP/email not configured)."
    )



# AIRFLOW DAG


with DAG(
    dag_id="group9_etl",

    start_date=datetime(
        2026,
        9,
        23
    ),

    schedule="0 9 * * *",

    catchup=False,

) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    validate_task = PythonOperator(
        task_id="validate",
        python_callable=validate,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    notification_task = PythonOperator(
        task_id="notification",
        python_callable=notification,
    )

    # ETL pipeline order
    (
        extract_task >> validate_task >> transform_task >> load_task >> notification_task
    )
