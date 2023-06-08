from google.cloud import bigquery
import argparse
import hashlib


# Construct a BigQuery client object.
def return_content(project, query):
    bigquery_client = bigquery.Client(project)
    query_job = bigquery_client.query(query)
    rows = query_job.result(timeout=60)
    content = [row.values() for row in rows]
    return content


def compute_hash(content):
    """Compute a hash value of a list of objects by hashing their string
  representations."""
    content = [
        str(x).encode('utf-8') if not isinstance(x, bytes) else x
        for x in content
    ]
    content.sort()
    m = hashlib.new("sha1")
    for elem in content:
        m.update(elem)
    return m.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--query', dest='query', help="Query to run")
    parser.add_argument('-p', '--project', dest='project', help="Project Id")
    args = parser.parse_args()
    print(
        f"Expected SHA1 is {compute_hash(return_content(args.project,args.query))}"
    )


if __name__ == "__main__":
    main()
