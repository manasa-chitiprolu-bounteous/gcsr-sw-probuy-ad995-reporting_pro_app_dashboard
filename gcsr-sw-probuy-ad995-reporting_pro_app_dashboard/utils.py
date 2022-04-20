from google.cloud import bigquery
import json

def view_to_table(project, dataset_id, table):
    client = bigquery.Client()
    #first delete last 5 days of data before re-appending last 5 days of new data
    delete_query = f'DELETE FROM {project}.{dataset_id}.{table} WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)'
    query_job_delete = client.query(delete_query)
    query_job_delete.result()

    #read view to get last 5 days of data and append to BQ table
    job_config = bigquery.QueryJobConfig(destination=f'{project}.{dataset_id}.{table}',
                                         write_disposition='WRITE_APPEND',
                                         create_disposition='CREATE_IF_NEEDED')
    query = "SELECT * FROM `{}.{}.{}`".format(project, dataset_id, f'v_{table}')
    query_job = client.query(query, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.
    print(f"Query results loaded to the table {project}.{dataset_id}.{table}")