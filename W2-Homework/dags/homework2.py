from airflow.sdk import DAG, Param, task, get_current_context,dag
from datetime import datetime,timedelta
from airflow.providers.standard.operators.bash import BashOperator
import pendulum
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator,BigQueryInsertJobOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

taxis=['green','yellow']
years = [2019, 2020,2021]
months = [f"{m:02d}" for m in range(1, 13)] 

GCS_BUCKET_NAME = "zoomcamp-486023-homework2"  
GCP_CONN_ID = "gcp_zoomcamp"
GCP_PROJECT_ID='zoomcamp-486023' 
GCP_DATASET_ID='zoomcamp'



def taxi_branch(**context):
    taxi = context["params"]["taxi"]
    return "create_ext_table_green_taxi" if taxi == "green" else "create_ext_table_yellow_taxi"

with DAG(
    dag_id="homework2_nont_taxi_v15",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1,tz="America/Los_Angeles"),
    catchup=False,
    params={
        "year": Param(
            default=2019,
            type="integer",
            enum=years,
            title="Year",
        ),
        "month": Param(
            default="01",
            type="string",
            enum=months,
            title="Month",
        ),
        "taxi": Param(
            default="green",
            type="string",
            enum=taxis,
            title="Taxi Color",
        )        
    },
) as dag:
        download_file = BashOperator(
            task_id = 'download_taxi_file',
            do_xcom_push=True,
            bash_command ="""
            set -euo pipefail

            OUTDIR="$(mktemp -d -t airflow_taxi_XXXXXX)"
            echo "OUTDIR=${OUTDIR}" >&2

            BASE_URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{{ params.taxi }}/"
            FILE="{{ params.taxi }}_tripdata_{{ params.year }}-{{ params.month }}.csv.gz"
            URL="${BASE_URL}${FILE}"

            echo "************" >&2
            echo "Downloading: ${URL}" >&2
            wget -O "${OUTDIR}/${FILE}" "${URL}"
            echo "Unzipping: ${OUTDIR}/${FILE}" >&2
            gunzip -k "${OUTDIR}/${FILE}"

            CSV_PATH="${OUTDIR}/${FILE%.gz}"
            echo "${CSV_PATH}"
            """
        )

        # create_bucket = GCSCreateBucketOperator(
        #     task_id="create_gcs_bucket",
        #     bucket_name=GCS_BUCKET_NAME,
        #     location="US",
        #     storage_class="STANDARD",
        #     gcp_conn_id=GCP_CONN_ID,
        # )

        upload_file = LocalFilesystemToGCSOperator(
            task_id="upload_taxi_file_to_gcs",
            src="{{ ti.xcom_pull(task_ids='download_taxi_file') }}",
            dst="{{ params.taxi }}_tripdata_{{ params.year }}-{{ params.month }}.csv",
            bucket=GCS_BUCKET_NAME,
            mime_type="text/csv",
            gcp_conn_id=GCP_CONN_ID,
        )

        # create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        #     task_id="create_bq_dataset",
        #     dataset_id="zoomcamp",
        #     location="US",                      
        #     gcp_conn_id="gcp_zoomcamp",
        #     exists_ok=True,
        # )


        branch = BranchPythonOperator(
            task_id="branch_by_taxi",
            python_callable=taxi_branch,
        )

        done = EmptyOperator(
            task_id="done",
            trigger_rule="none_failed_min_one_success",
        )

        create_ext_table_green_taxi = BigQueryInsertJobOperator(
            task_id="create_ext_table_green_taxi",
            gcp_conn_id=GCP_CONN_ID,
            location="US",
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{{{{ params.taxi }}}}_tripdata_{{{{ params.year }}}}_{{{{ params.month }}}}_ext`
                        (
                        VendorID STRING OPTIONS (description = 'A code indicating the LPEP provider that provided the record. 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc.'),
                        lpep_pickup_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was engaged'),
                        lpep_dropoff_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was disengaged'),
                        store_and_fwd_flag STRING OPTIONS (description = 'Y= store and forward trip N= not a store and forward trip'),
                        RatecodeID STRING OPTIONS (description = '1= Standard rate 2=JFK 3=Newark 4=Nassau or Westchester 5=Negotiated fare 6=Group ride'),
                        PULocationID STRING OPTIONS (description = 'TLC Taxi Zone in which the taximeter was engaged'),
                        DOLocationID STRING OPTIONS (description = 'TLC Taxi Zone in which the taximeter was disengaged'),
                        passenger_count INT64 OPTIONS (description = 'The number of passengers in the vehicle.'),
                        trip_distance NUMERIC OPTIONS (description = 'The elapsed trip distance in miles reported by the taximeter.'),
                        fare_amount NUMERIC OPTIONS (description = 'The time-and-distance fare calculated by the meter'),
                        extra NUMERIC OPTIONS (description = 'Miscellaneous extras and surcharges.'),
                        mta_tax NUMERIC OPTIONS (description = '$0.50 MTA tax'),
                        tip_amount NUMERIC OPTIONS (description = 'Tip amount (credit card tips).'),
                        tolls_amount NUMERIC OPTIONS (description = 'Total amount of all tolls paid in trip.'),
                        ehail_fee NUMERIC,
                        improvement_surcharge NUMERIC OPTIONS (description = '$0.30 improvement surcharge'),
                        total_amount NUMERIC OPTIONS (description = 'The total amount charged to passengers.'),
                        payment_type INTEGER OPTIONS (description = '1= Credit card 2= Cash 3= No charge 4= Dispute 5= Unknown 6= Voided trip'),
                        trip_type STRING OPTIONS (description = '1= Street-hail 2= Dispatch'),
                        congestion_surcharge NUMERIC OPTIONS (description = 'Congestion surcharge applied to trips in congested zones')
                        )
                        OPTIONS (
                        format = 'CSV',
                        uris = ['gs://{GCS_BUCKET_NAME}/{{{{ params.taxi }}}}_tripdata_{{{{ params.year }}}}-{{{{ params.month }}}}.csv'],
                        skip_leading_rows = 1,
                        ignore_unknown_values = TRUE
                        );
                        """,
                    "useLegacySql": False,
                }
            },     

        )

        create_main_table_green_taxi = BigQueryInsertJobOperator(
            task_id="create_main_table_green_taxi",
            gcp_conn_id=GCP_CONN_ID,
            location="US",
            configuration={
                "query": {
                    "query": f"""
                        CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.green_tripdata`
                        (
                            unique_row_id BYTES OPTIONS (description = 'A unique identifier for the trip, generated by hashing key trip attributes.'),
                            filename STRING OPTIONS (description = 'The source filename from which the trip data was loaded.'),      
                            VendorID STRING OPTIONS (description = 'A code indicating the LPEP provider that provided the record. 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc.'),
                            lpep_pickup_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was engaged'),
                            lpep_dropoff_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was disengaged'),
                            store_and_fwd_flag STRING OPTIONS (description = 'This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka "store and forward," because the vehicle did not have a connection to the server. Y= store and forward trip N= not a store and forward trip'),
                            RatecodeID STRING OPTIONS (description = 'The final rate code in effect at the end of the trip. 1= Standard rate 2=JFK 3=Newark 4=Nassau or Westchester 5=Negotiated fare 6=Group ride'),
                            PULocationID STRING OPTIONS (description = 'TLC Taxi Zone in which the taximeter was engaged'),
                            DOLocationID STRING OPTIONS (description = 'TLC Taxi Zone in which the taximeter was disengaged'),
                            passenger_count INT64 OPTIONS (description = 'The number of passengers in the vehicle. This is a driver-entered value.'),
                            trip_distance NUMERIC OPTIONS (description = 'The elapsed trip distance in miles reported by the taximeter.'),
                            fare_amount NUMERIC OPTIONS (description = 'The time-and-distance fare calculated by the meter'),
                            extra NUMERIC OPTIONS (description = 'Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges'),
                            mta_tax NUMERIC OPTIONS (description = '$0.50 MTA tax that is automatically triggered based on the metered rate in use'),
                            tip_amount NUMERIC OPTIONS (description = 'Tip amount. This field is automatically populated for credit card tips. Cash tips are not included.'),
                            tolls_amount NUMERIC OPTIONS (description = 'Total amount of all tolls paid in trip.'),
                            ehail_fee NUMERIC,
                            improvement_surcharge NUMERIC OPTIONS (description = '$0.30 improvement surcharge assessed on hailed trips at the flag drop. The improvement surcharge began being levied in 2015.'),
                            total_amount NUMERIC OPTIONS (description = 'The total amount charged to passengers. Does not include cash tips.'),
                            payment_type INTEGER OPTIONS (description = 'A numeric code signifying how the passenger paid for the trip. 1= Credit card 2= Cash 3= No charge 4= Dispute 5= Unknown 6= Voided trip'),
                            trip_type STRING OPTIONS (description = 'A code indicating whether the trip was a street-hail or a dispatch that is automatically assigned based on the metered rate in use but can be altered by the driver. 1= Street-hail 2= Dispatch'),
                            congestion_surcharge NUMERIC OPTIONS (description = 'Congestion surcharge applied to trips in congested zones')
                        )
                        PARTITION BY DATE(lpep_pickup_datetime);
                        """,
                    "useLegacySql": False,
                }
            },     

        )

        create_int_table_green_taxi = BigQueryInsertJobOperator(
            task_id="create_int_table_green_taxi",
            gcp_conn_id=GCP_CONN_ID,
            location="US",
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{{{{ params.taxi }}}}_tripdata_{{{{ params.year }}}}_{{{{ params.month }}}}`
                        AS
                        SELECT
                            MD5(CONCAT(
                            COALESCE(CAST(VendorID AS STRING), ""),
                            COALESCE(CAST(lpep_pickup_datetime AS STRING), ""),
                            COALESCE(CAST(lpep_dropoff_datetime AS STRING), ""),
                            COALESCE(CAST(PULocationID AS STRING), ""),
                            COALESCE(CAST(DOLocationID AS STRING), "")
                            )) AS unique_row_id,
                            "{{{{ params.taxi }}}}_tripdata_{{{{ params.year }}}}-{{{{ params.month }}}}.csv" AS filename,
                            *
                        FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{{{{ params.taxi }}}}_tripdata_{{{{ params.year }}}}_{{{{ params.month }}}}_ext`;
                        """,
                    "useLegacySql": False,
                }
            },     

        )

        merge_table_green_taxi = BigQueryInsertJobOperator(
            task_id="merge_table_green_taxi",
            gcp_conn_id=GCP_CONN_ID,
            location="US",
            configuration={
                "query": {
                    "query": f"""
                        MERGE INTO `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.green_tripdata` T
                        USING `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{{{{ params.taxi }}}}_tripdata_{{{{ params.year }}}}_{{{{ params.month }}}}` S
                        ON T.unique_row_id = S.unique_row_id
                        WHEN NOT MATCHED THEN
                            INSERT (unique_row_id, filename, VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag, RatecodeID, PULocationID, DOLocationID, passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge)
                            VALUES (S.unique_row_id, S.filename, S.VendorID, S.lpep_pickup_datetime, S.lpep_dropoff_datetime, S.store_and_fwd_flag, S.RatecodeID, S.PULocationID, S.DOLocationID, S.passenger_count, S.trip_distance, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount, S.ehail_fee, S.improvement_surcharge, S.total_amount, S.payment_type, S.trip_type, S.congestion_surcharge);
                        """,
                    "useLegacySql": False,
                }
            },     

        )

        create_ext_table_yellow_taxi = BigQueryInsertJobOperator(
            task_id="create_ext_table_yellow_taxi",
            gcp_conn_id=GCP_CONN_ID,
            location="US",
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{{{{ params.taxi }}}}_tripdata_{{{{ params.year }}}}_{{{{ params.month }}}}_ext`
                        (
                            VendorID STRING OPTIONS (description = 'A code indicating the LPEP provider that provided the record. 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc.'),
                            tpep_pickup_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was engaged'),
                            tpep_dropoff_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was disengaged'),
                            passenger_count INTEGER OPTIONS (description = 'The number of passengers in the vehicle. This is a driver-entered value.'),
                            trip_distance NUMERIC OPTIONS (description = 'The elapsed trip distance in miles reported by the taximeter.'),
                            RatecodeID STRING OPTIONS (description = 'The final rate code in effect at the end of the trip. 1= Standard rate 2=JFK 3=Newark 4=Nassau or Westchester 5=Negotiated fare 6=Group ride'),
                            store_and_fwd_flag STRING OPTIONS (description = 'This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka "store and forward," because the vehicle did not have a connection to the server. TRUE = store and forward trip, FALSE = not a store and forward trip'),
                            PULocationID STRING OPTIONS (description = 'TLC Taxi Zone in which the taximeter was engaged'),
                            DOLocationID STRING OPTIONS (description = 'TLC Taxi Zone in which the taximeter was disengaged'),
                            payment_type INTEGER OPTIONS (description = 'A numeric code signifying how the passenger paid for the trip. 1= Credit card 2= Cash 3= No charge 4= Dispute 5= Unknown 6= Voided trip'),
                            fare_amount NUMERIC OPTIONS (description = 'The time-and-distance fare calculated by the meter'),
                            extra NUMERIC OPTIONS (description = 'Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges'),
                            mta_tax NUMERIC OPTIONS (description = '$0.50 MTA tax that is automatically triggered based on the metered rate in use'),
                            tip_amount NUMERIC OPTIONS (description = 'Tip amount. This field is automatically populated for credit card tips. Cash tips are not included.'),
                            tolls_amount NUMERIC OPTIONS (description = 'Total amount of all tolls paid in trip.'),
                            improvement_surcharge NUMERIC OPTIONS (description = '$0.30 improvement surcharge assessed on hailed trips at the flag drop. The improvement surcharge began being levied in 2015.'),
                            total_amount NUMERIC OPTIONS (description = 'The total amount charged to passengers. Does not include cash tips.'),
                            congestion_surcharge NUMERIC OPTIONS (description = 'Congestion surcharge applied to trips in congested zones')
                        )
                        OPTIONS (
                            format = 'CSV',
                            uris = ['gs://{GCS_BUCKET_NAME}/{{{{ params.taxi }}}}_tripdata_{{{{ params.year }}}}-{{{{ params.month }}}}.csv'],
                            skip_leading_rows = 1,
                            ignore_unknown_values = TRUE
                        );
                        """,
                    "useLegacySql": False,
                }
            },     

        )

        create_main_table_yellow_taxi = BigQueryInsertJobOperator(
            task_id="create_main_table_yellow_taxi",
            gcp_conn_id=GCP_CONN_ID,
            location="US",
            configuration={
                "query": {
                    "query": f"""
                        CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.yellow_tripdata`
                        (
                            unique_row_id BYTES OPTIONS (description = 'A unique identifier for the trip, generated by hashing key trip attributes.'),
                            filename STRING OPTIONS (description = 'The source filename from which the trip data was loaded.'),      
                            VendorID STRING OPTIONS (description = 'A code indicating the LPEP provider that provided the record. 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc.'),
                            tpep_pickup_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was engaged'),
                            tpep_dropoff_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was disengaged'),
                            passenger_count INTEGER OPTIONS (description = 'The number of passengers in the vehicle. This is a driver-entered value.'),
                            trip_distance NUMERIC OPTIONS (description = 'The elapsed trip distance in miles reported by the taximeter.'),
                            RatecodeID STRING OPTIONS (description = 'The final rate code in effect at the end of the trip. 1= Standard rate 2=JFK 3=Newark 4=Nassau or Westchester 5=Negotiated fare 6=Group ride'),
                            store_and_fwd_flag STRING OPTIONS (description = 'This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka "store and forward," because the vehicle did not have a connection to the server. TRUE = store and forward trip, FALSE = not a store and forward trip'),
                            PULocationID STRING OPTIONS (description = 'TLC Taxi Zone in which the taximeter was engaged'),
                            DOLocationID STRING OPTIONS (description = 'TLC Taxi Zone in which the taximeter was disengaged'),
                            payment_type INTEGER OPTIONS (description = 'A numeric code signifying how the passenger paid for the trip. 1= Credit card 2= Cash 3= No charge 4= Dispute 5= Unknown 6= Voided trip'),
                            fare_amount NUMERIC OPTIONS (description = 'The time-and-distance fare calculated by the meter'),
                            extra NUMERIC OPTIONS (description = 'Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges'),
                            mta_tax NUMERIC OPTIONS (description = '$0.50 MTA tax that is automatically triggered based on the metered rate in use'),
                            tip_amount NUMERIC OPTIONS (description = 'Tip amount. This field is automatically populated for credit card tips. Cash tips are not included.'),
                            tolls_amount NUMERIC OPTIONS (description = 'Total amount of all tolls paid in trip.'),
                            improvement_surcharge NUMERIC OPTIONS (description = '$0.30 improvement surcharge assessed on hailed trips at the flag drop. The improvement surcharge began being levied in 2015.'),
                            total_amount NUMERIC OPTIONS (description = 'The total amount charged to passengers. Does not include cash tips.'),
                            congestion_surcharge NUMERIC OPTIONS (description = 'Congestion surcharge applied to trips in congested zones')
                        )
                        PARTITION BY DATE(tpep_pickup_datetime);
                        """,
                    "useLegacySql": False,
                }
            },     

        )

        create_int_table_yellow_taxi = BigQueryInsertJobOperator(
            task_id="create_int_table_yellow_taxi",
            gcp_conn_id=GCP_CONN_ID,
            location="US",
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{{{{ params.taxi }}}}_tripdata_{{{{ params.year }}}}_{{{{ params.month }}}}`
                        AS
                        SELECT
                            MD5(CONCAT(
                            COALESCE(CAST(VendorID AS STRING), ""),
                            COALESCE(CAST(tpep_pickup_datetime AS STRING), ""),
                            COALESCE(CAST(tpep_dropoff_datetime AS STRING), ""),
                            COALESCE(CAST(PULocationID AS STRING), ""),
                            COALESCE(CAST(DOLocationID AS STRING), "")
                            )) AS unique_row_id,
                            "{{{{ params.taxi }}}}_tripdata_{{{{ params.year }}}}-{{{{ params.month }}}}.csv" AS filename,
                            *
                        FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{{{{ params.taxi }}}}_tripdata_{{{{ params.year }}}}_{{{{ params.month }}}}_ext`;
                        """,
                    "useLegacySql": False,
                }
            },     

        )

        merge_table_yellow_taxi = BigQueryInsertJobOperator(
            task_id="merge_table_yellow_taxi",
            gcp_conn_id=GCP_CONN_ID,
            location="US",
            configuration={
                "query": {
                    "query": f"""
                        MERGE INTO `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.yellow_tripdata` T
                        USING `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{{{{ params.taxi }}}}_tripdata_{{{{ params.year }}}}_{{{{ params.month }}}}` S
                        ON T.unique_row_id = S.unique_row_id
                        WHEN NOT MATCHED THEN
                            INSERT (unique_row_id, filename, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge)
                            VALUES (S.unique_row_id, S.filename, S.VendorID, S.tpep_pickup_datetime, S.tpep_dropoff_datetime, S.passenger_count, S.trip_distance, S.RatecodeID, S.store_and_fwd_flag, S.PULocationID, S.DOLocationID, S.payment_type, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount, S.improvement_surcharge, S.total_amount, S.congestion_surcharge);
                        """,    
                    "useLegacySql": False,
                }
            },     

        )

download_file >> upload_file >> branch
branch >> create_ext_table_green_taxi >> create_main_table_green_taxi >> create_int_table_green_taxi >> merge_table_green_taxi >> done
branch >> create_ext_table_yellow_taxi >> create_main_table_yellow_taxi >> create_int_table_yellow_taxi >> merge_table_yellow_taxi >> done
