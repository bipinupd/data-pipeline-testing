def pytest_addoption(parser):
    parser.addoption('--test-pipeline-options',
                     help='Options to use in test pipelines. NOTE: Tests may '
                     'ignore some or all of these options.')
    # parser.addoption('--PROJECT_ID', help='Project id')
    # parser.addoption('--TEMP_LOCATION', help='temp location')
    # parser.addoption('--STAGING_LOCATION', help='staging location')
    # parser.addoption('--TEST_GCS_BUCKET', help='gcs bucket with test files')
    # TEST_GCS_BUCKET="test_bucket_4_dataflow_pipelines"
    # TEMP_DIR = "gs://my-dataflow-temp-stage/temp-it"
    # STAGING_LOCATION = "gs://my-dataflow-temp-stage/staging-it"
