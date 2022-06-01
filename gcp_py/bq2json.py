from google.cloud import bigquery
import io
import os
import argparse
import json
import secrets


class DuplicateKeyError(Exception):pass

class ProjectDatasets:
    
    def __init__(self, client):
        self.client = client
        self.project = self.client.project
        self.datasets = set()
        self.tables = set()

    
    def get_datasets(self):
        for dataset in set(self.client.list_datasets()):
            self.datasets.add(dataset.dataset_id)
        
        return self.datasets
 
    def get_all_tables(self, dataset_id):
        for table in set(self.client.list_tables(dataset_id)):
            self.tables.add(table.table_id)
        return self.tables

class SchemaToJson:

    def __init__(self, client, dataset_id, table):    
        self.client = client
        self.dataset_id = dataset_id
        self.table = table
        self.schema_dict = dict()
        self.table_dict = dict()
    
    def createSchema(self):
        if self.dataset_id not in self.schema_dict:
            self.schema_dict[self.dataset_id] = self.createTable(self.table.table_id)
        else:
            raise DuplicateKeyError('{self.dataset_id} already present')
        
        return self.schema_dict
        
    def createTable(self, table_id):
        if table_id not in self.table_dict:
            self.table_dict[table_id] = self.createInfo()
        else:
            raise DuplicateKeyError("{table_id} already present")
        return self.table_dict
    
    def createInfo(self):
        return {
            "schema_path": "",
            "deletion_protection" : True,
            "time_partitioning": self.time_partitioning(),
            "clustering": self.table.clustering_fields,
            "labels": self.table.labels

        }
    
    def time_partitioning(self):
        return {
            "field": self.table.time_partitioning.field,
            "type": self.table.time_partitioning.type_,
            "expiration_ms": 0 if self.table.partition_expiration == None else self.table.partition_expiration,
            "require_partition_filter": False if self.table.require_partition_filter == None else self.table.require_partition_filter
        }
    
    def write_to_json(self):
        file_path = f"../data/{self.dataset_id}_{secrets.token_urlsafe(6)}.json"
        # print(self.createSchema())
        with open(file_path,'w') as file:
            json.dump(self.createSchema(), file, indent =4 )

class TableSchemaToJson:
    def __init__(self, client, dataset_id, table_id):
        self.client = client
        self.project= client.project
        self.dataset_id = dataset_id
        self.table_id = table_id
   
    
    def get_dataset_ref(self):
        return self.client.dataset(self.dataset_id, project=self.project)
    
    def get_table_ref(self):
        return self.get_dataset_ref().table(self.table_id)

    def get_table(self):
        return self.client.get_table(self.get_table_ref())

    def set_table(self, new_table_id):
        self.table_id = new_table_id

    def table_to_json(self):
        file_path = f"../data/{self.table_id}_{secrets.token_urlsafe(6)}.json"
        f = io.StringIO("")
        self.client.schema_to_json(self.get_table().schema, f)
        with open(file_path,'w') as file:
            file.write(f.getvalue())
            file.close()

    def all_tables_to_json(self):
        for table in set(self.client.list_tables(self.dataset_id)):
            self.set_table(table.table_id)
            self.table_to_json()

    def sche_to_json(self):
        result = dict()
        
        

    




def main(args):
    credentials_json = '../.tmp/dev.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_json

    client = bigquery.Client()

    project = client.project
    datasets = set(client.list_datasets())
    dataset_id = args.dataset
    table_id = args.table

    # proj_datasets = ProjectDatasets(client)
 

    table_json = TableSchemaToJson(client, dataset_id, table_id)
    
    # asyncio.run(table_json.table_to_json())
    # asyncio.run(table_json.all_tables_to_json())
    # table_json.all_tables_to_json()

    


    dataset_ref = client.dataset(dataset_id, project=project)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)

    sche_json = SchemaToJson(client, dataset_id, table)
    sche_json.write_to_json()

    # print (table.clustering_fields)
    # print (table.time_partitioning, table.partition_expiration, table.require_partition_filter)
    # print (table.schema)
    # print(table.labels)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get Schema or RDS metadata Info in JSON format')
    parser.add_argument('-at', '--alltb', help="all tables in current dataset (Must use with dataset option)", dest="alltb", action='store_true')
    parser.add_argument('-d', '--dataset', help="dataset id", dest="dataset", type=str)
    parser.add_argument('-t', '--table', help="table_id", dest="table", type=str)

    args = parser.parse_args()

    if  args.alltb and (args.dataset is None or args.table is not None):
        parser.error("argument error")
    
    
    main(args)