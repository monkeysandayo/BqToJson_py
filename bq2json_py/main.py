from google.cloud import bigquery
from SchemaToJson import SchemaToJson
from TableToJson import TableToJson
from dotenv import load_dotenv
import os
import argparse


def create(args):
    load_dotenv()
    credentials_json = os.environ.get("CREDENTIALS_JSON")
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_json

    client = bigquery.Client(project=args.project)


    # project = client.project
    # datasets = set(client.list_datasets())
    dataset_id = args.dataset
    table_id = args.table or ""

   

    if not args.alltb and (args.dataset is not None and args.table is not None):
        table_json = TableToJson(client, dataset_id, table_id)
        table_json.write_to_json()

    if args.alltb and (args.dataset is not None and args.table is None):
        table_json = TableToJson(client, dataset_id, table_id)
        table_json.all_tables_to_json()
    
    
    if not args.alltb and (args.dataset is not None and args.table is None):
        schema = SchemaToJson(client,dataset_id)
        schema.all_tables_to_json()
 

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get Schema or RDS metadata Info in JSON format')
    parser.add_argument('-at', '--alltb', help="all tables in current dataset (Must use with dataset option)", dest="alltb", action='store_true')
    parser.add_argument('-p', '--project', help="project_id", dest="project", type=str, required=True)
    parser.add_argument('-d', '--dataset', help="dataset id", dest="dataset", type=str, required=True)
    parser.add_argument('-t', '--table', help="table_id", dest="table", type=str)

    args = parser.parse_args()

    if  args.alltb and (args.dataset is None or args.table is not None):
        parser.error("argument error")
    
    create(args)