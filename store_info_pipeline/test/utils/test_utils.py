from google.cloud import bigquery


def get_count(query, count_column):
    client = bigquery.Client()
    query_job = client.query(query)  # API request
    rows = query_job.result()  # Waits for query to finish
    for row in rows:
        return row[count_column]