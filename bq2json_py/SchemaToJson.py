import json
import secrets
import os
import io
import DuplicateKeyError

class SchemaToJson:

    def __init__(self, client, dataset_id, deletion_protection=True):
        self.client = client
        self.dataset_id = dataset_id
        self.datasets = set(self.client.list_datasets())
        self.tables = set()
        self.schema_dict = dict()
        self.table_dict = dict()
        self.deletion_protection = deletion_protection
    
    def get_dataset_ref(self):
        return self.client.dataset(self.dataset_id, project=self.client.project)

    def get_datasets(self):
        return self.datasets

    def get_single_table(self, table_id):
        table_ref =self.get_dataset_ref().table(table_id)
        return self.client.get_table(table_ref)

    def get_all_tables(self):
        for table in set(self.client.list_tables(self.dataset_id)):
            self.tables.add(self.get_single_table(table.table_id))
        return self.tables
    
    def set_dataset(self, new_dataset_id):
        self.dataset_id = new_dataset_id
    
    def createSchema(self):
        if self.dataset_id not in self.schema_dict:
            self.schema_dict[self.dataset_id] = self.table_dict
        else:
            raise DuplicateKeyError(f'{self.dataset_id} already present')
        
        return self.schema_dict
        
    def createTable(self, table):
        if table.table_id not in self.table_dict:
            self.table_dict[table.table_id] = self.createInfo(table)
    
    def createInfo(self, table):
        return {
            "schema_path": f"./tables/{self.dataset_id}_schema/{table.table_id}.json",
            "deletion_protection" : self.deletion_protection,
            "time_partitioning": self.time_partitioning(table),
            "clustering": [] if table.clustering_fields is None else table.clustering_fields,
            "labels": table.labels
   }
    
    def time_partitioning(self, table):
        field = None
        type_ = None
        try:
           field = table.time_partitioning.field
        except AttributeError:
            pass
        try:
           type_ = table.time_partitioning.type_
        except AttributeError:
            pass
        if ( not (field or type_) and not (table.partition_expiration or table.require_partition_filter)):
            return None
        return {
            "field": field,
            "type": type_,
            "expiration_ms": 0 if table.partition_expiration == None else table.partition_expiration,
            "require_partition_filter": False if table.require_partition_filter == None else table.require_partition_filter
        }

    def write_to_json(self):
        try:
            os.makedirs(f"../data/tables")
        except FileExistsError:
            pass
        file_path = f"../data/tables/{self.dataset_id}_tables.json"
        with open(file_path,'w') as file:
            json.dump(self.createSchema(), file, indent=4 )

    def all_tables_to_json(self):
        self.table_dict = dict()
        for table in self.get_all_tables():
            self.createTable(table)
        self.write_to_json()
    
    # def all_datasets_to_json(self):
    #     for dataset in self.get_datasets():
    #         self.set_dataset(dataset.dataset_id)
    #         self.all_tables_to_json()