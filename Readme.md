The repo contains the data-pipeline-testing.

- `beam-testing-example` contains multiple examples including pardo, combinefxn, side input and windows.

  - In `testing_pardo/excercise` folder contains a generic pardo function that takes a number and produces the result based on wheather the elements are divisible by that number or not.
      - Write a test case to with input array [3,7,8,9,12,14,16,"1a"] and test it for numbers 5,6,7 
      - Solution.py is the solution
  - In `testing_windows/excercise` folder contains a sliding windows and write unitest it. `solution_unittest.py` contians the solution.

- `store-info-pipeline` contains a use case to read the csv files from GCS bucket and summarize the data based on the store_id and then number of products sold by the store.

 ![Store Info Batch Pipeeline](docs/images/store-info.jpg "Store Info Batch Pipeline")

  
GCS objects in CSV has data in the following format

`
txn_date, txn_id, store_id, prodcut_name, qty, amount`

Data is written in the BigQiuery, Error recordsgoes to error table.

 ![Store_Info_Batch_Pipeeline](docs/images/tables.jpg "Store Info Batch Pipeline")

  - Run the unittest
  
    `python -m pytest test/store_info_batch_unittest.py`

  - Run the integration test
  
    ```
        export TEMP_LOCATION= <<set the temp gcs location>>
        export STAGING_LOCATION= <<set the staging location>>
        export PROJECT_ID=<project_id_to run the test and create bq dataset and tables>
        python -m pytest --log-cli-level=INFO test/store_info_batch_ittest.py --test-pipeline-options="--project=${PROJECT_ID} --runner=DataflowRunner --input=${INPUT_BUCKET} --temp_location=${TEMP_LOCATION} --staging_location=${STAGING_LOCATION} --setup=./setup.py"
    ```

- `taxi-rides-streaming-pipeline` contains a use case to reads from a PubSub and summarizes the ride status within a redefined sliding window.

 ![PubSub to PubSub Streaming](docs/images/taxirides.jpg "PubSub to PubSub Streaming Pipeline")

   - Run the unittest
  
        ```
        python -m pytest test/store_info_batch_unittest.py
        ```
    - Run the integration test
        ```
        export TEMP_LOCATION=<<GCS_TEMP_LOCATION>>
        export STAGING_LOCATION=<<GCS_STAGING_LOCATION>
        export PROJECT_ID=<<PROJECT_ID>
        pytest --log-cli-level=INFO test/taxirides_ittest.py --test-pipeline-options="--runner=TestDataflowRunner --wait_until_finish_duration=100000 -temp_location=${TEMP_LOCATION}--staging_location=${STAGING_LOCATION} --project=${PROJECT_ID} --setup=./setup.py"
        ```