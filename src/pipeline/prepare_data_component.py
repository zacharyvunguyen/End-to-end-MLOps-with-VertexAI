from kfp.v2.dsl import component, Output, Dataset
import os
import sys

# Assuming this script might be part of a larger project, adjust paths as necessary
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, "..")))


@component(
    base_image='python:3.9',
    packages_to_install=['google-cloud-bigquery']
)
def bq_table_prep_op(
        project: str,
        region: str,
        source_bq_table_id: str,
        out_bq_dataset_id: str,
        limit: int,
        prepped_data: Output[Dataset]
):
    """Prepares a dataset in BigQuery by applying transformations and returns the new table URI."""
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound

    # Construct the fully qualified table ID
    new_bq_table_id = f"{project}.{out_bq_dataset_id}.source_prepped_limit_{limit}"

    # Initialize BigQuery client
    bqclient = bigquery.Client(project=project)

    # Define the SQL query
    query = f"""
    CREATE OR REPLACE TABLE `{new_bq_table_id}` AS (
        WITH all_hash_limit AS (
            SELECT    
                weight_pounds,
                CAST(is_male AS STRING) AS is_male,
                mother_age,
                CASE
                    WHEN plurality = 1 THEN 'Single(1)'
                    WHEN plurality = 2 THEN 'Twins(2)'
                    WHEN plurality = 3 THEN 'Triplets(3)'
                    WHEN plurality = 4 THEN 'Quadruplets(4)'
                    WHEN plurality = 5 THEN 'Quintuplets(5)'
                END AS plurality,
                gestation_weeks,
                IFNULL(CAST(cigarette_use AS STRING), 'Unknown') AS cigarette_use,
                IFNULL(CAST(alcohol_use AS STRING), 'Unknown') AS alcohol_use,
                ABS(FARM_FINGERPRINT(
                    CONCAT(
                        CAST(year AS STRING),
                        CAST(month AS STRING),
                        CAST(COALESCE(wday, day, 0) AS STRING),
                        CAST(IFNULL(state, 'Unknown') AS STRING),
                        CAST(IFNULL(mother_birth_state, 'Unknown') AS STRING)
                    )
                )) AS hash_values
            FROM `{source_bq_table_id}`
            WHERE
                year > 2002 AND
                weight_pounds > 0 AND
                mother_age > 0 AND
                plurality > 0 AND
                gestation_weeks > 19
            LIMIT {limit}
        )
        SELECT 
            weight_pounds,
            is_male,
            mother_age,
            plurality,
            gestation_weeks,
            cigarette_use,
            alcohol_use,
            CASE 
                WHEN MOD(hash_values, 10) < 8 THEN 'TRAIN' 
                WHEN MOD(hash_values, 10) < 9 THEN 'VALIDATE'
                ELSE 'TEST'
            END AS splits
        FROM all_hash_limit
    )
    """

    # Execute the query
    query_job = bqclient.query(query)
    query_job.result()  # Wait for the query to finish

    # Set the URI for the prepped data artifact to the new BigQuery table
    prepped_data.uri = f"bq://{new_bq_table_id}"
