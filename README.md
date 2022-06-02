# BigQueryCheck_py


## Usage
* It is to automate to create JSON files that include BigQuery schema and table information

### Requirements

    pip install -r requirments.txt


## Environments

    CREDENTIALS_JSON = "{YOUR_CREDENTIAL_JSON_PATH}"


## Code

    cd bq2json_py
    python main.py -d=dataset_name 

* -d / --dataset (required): project name in BigQuery
* -t / --table (optional) : csv file name 
* -at / --alltb (default=False) : Must be used with -d / --dataset to create a json file of all tables information of the given dataset

