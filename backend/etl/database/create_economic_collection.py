from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGODB_URI")
DB_NAME = "ecommerce_brazil"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

economic_data = db.create_collection("economic_data")
economic_data.create_index("state", unique=True)

print("Colección economic_data creada con índice en 'state'")
client.close()
