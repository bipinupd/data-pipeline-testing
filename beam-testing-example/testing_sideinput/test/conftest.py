def pytest_addoption(parser):
    parser.addoption('--test-pipeline-options',
                     help='Options to use in test pipelines. NOTE: Tests may '
                     'ignore some or all of these options.')
