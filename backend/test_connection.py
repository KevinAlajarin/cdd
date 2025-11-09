from etl.database.mongodb_handler import MongoDBHandler
from dotenv import load_dotenv

load_dotenv()

def test_connection():
    print("🧪 Probando conexión a MongoDB Atlas...")
    
    db_handler = MongoDBHandler()
    if db_handler.connect():
        print("✅ Conexión exitosa!")
        db_handler.disconnect()
    else:
        print("❌ Error de conexión")

if __name__ == "__main__":
    test_connection()