from SchemaToJson import SchemaToJson
import os
import json
import secrets
import io

class TableToJson(SchemaToJson):

    def __init__(self, client, dataset_id, table_id):
        super().__init__(client, dataset_id)    
        self.table_id = table_id
        if self.table_id != "":
            self.table = super().get_single_table(self.table_id)
        else:
            self.table = ""
    

    def set_table(self, new_table_id):
        self.table_id = new_table_id
        self.table = super().get_single_table(self.table_id)

    def write_to_json(self):
        try:
            os.makedirs("../data/tables")
        except FileExistsError:
            pass
        file_path = f"../data/tables/{self.table_id}_{secrets.token_urlsafe(6)}.json"
        f = io.StringIO("")
        self.client.schema_to_json(self.table.schema, f)
        with open(file_path,'w') as file:
            file.write(f.getvalue())
            file.close()

    def all_tables_to_json(self):
        for table in super().get_all_tables():
            self.set_table(table.table_id)
            self.write_to_json()