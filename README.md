# BigQueryCheck_py


## Usage
* It is to automate to create JSON files that include BigQuery schema and table information

### Requirements

    pip install -r requirments.txt


## Environments

    cd bq2josn_py
    vi .env
    CREDENTIALS_JSON = "{YOUR_CREDENTIAL_JSON_PATH}"


## Code

    cd bq2json_py
    python main.py -p project_name -d dataset_name 

## Parameters
* -p / --project (required): project name of BigQuery
* -d / --dataset (required): dataset name in a project of BigQuery
* -t / --table (optional) : table name in the dataset
* -at / --alltb (default=False) : Must be used with -d / --dataset to create a json file of all tables information of the given dataset

