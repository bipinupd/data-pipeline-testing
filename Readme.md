The repo contains the data-pipeline-testing.







export TEMP_LOCATION=<<GCS_TEMP_LOCATION>>
export STAGING_LOCATION=<<GCS_STAGING_LOCATION>
export PROJECT_ID=<<PROJECT_ID>
pytest --log-cli-level=INFO test/taxirides_ittest.py --test-pipeline-options="--runner=TestDataflowRunner --wait_until_finish_duration=100000 -temp_location=${TEMP_LOCATION}--staging_location=${STAGING_LOCATION} --project=${PROJECT_ID} --setup=./setup.py"