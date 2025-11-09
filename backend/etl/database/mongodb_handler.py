from pymongo import MongoClient
from etl.config import MONGODB_CONFIG
import os

class MongoDBHandler:
    def __init__(self):
        self.connection_string = os.getenv('MONGODB_URI')
        self.database_name = MONGODB_CONFIG['database_name']
        self.client = None
        self.db = None
        
    def connect(self):
        try:
            self.client = MongoClient(self.connection_string)
            self.db = self.client[self.database_name]
            print("Conectado a MongoDB Atlas exitosamente")
            return True
        except Exception as e:
            print(f"Error conectando a MongoDB: {e}")
            return False
    
    def disconnect(self):
        if self.client:
            self.client.close()
            print("Conexión a MongoDB cerrada")
    
    def insert_data(self, collection_name, data):
        collection = self.db[collection_name]
        if isinstance(data, list):
            result = collection.insert_many(data)
            print(f"Insertados {len(result.inserted_ids)} documentos en {collection_name}")
        else:
            result = collection.insert_one(data)
            print(f"Insertado 1 documento en {collection_name}")
        return result
    
    def create_indexes(self):
        self.db.orders.create_index("order_id", unique=True)
        self.db.orders.create_index("customer_id")
        self.db.processed_results.create_index("fecha_procesamiento")
        self.db.economic_data.create_index("state")
        print("Índices creados exitosamente")
