# main.py
import os
from dotenv import load_dotenv
from etl.processing.data_cleaner import DataCleaner
from etl.processing.data_processor import DataProcessor
from etl.database.mongo_handler import MongoDBHandler

def main():
    print("INICIANDO SISTEMA ETL - ECOMMERCE BRAZIL")
    print("==================================================\n")

    # === FASE 1: PROCESAMIENTO ETL ===
    print("FASE 1: PROCESAMIENTO ETL")

    # Carga de variables de entorno
    load_dotenv()

    # Inicialización de limpiador y procesador
    cleaner = DataCleaner()
    processor = DataProcessor()  # No requiere cleaner como argumento

    if not processor.execute_etl():
        print("❌ Error durante la fase ETL.")
        return
    print("✅ ETL completado exitosamente\n")

    # === FASE 2: CARGA EN MONGODB ===
    print("FASE 2: CARGA EN MONGODB")

    mongo_uri = os.getenv(
        "MONGO_URI",
        "mongodb+srv://ecommerce_user:I5aoIQQEgE6wko6M@ecommerce-cluster.azje0im.mongodb.net/?appName=ecommerce-cluster"
    )
    mongo_db_name = os.getenv("MONGO_DB", "ecommerce_brazil")

    mongo_handler = MongoDBHandler(mongo_uri, mongo_db_name)

    if not mongo_handler.connect():
        print("❌ Error al conectar con MongoDB.")
        return

    print("Subiendo colecciones a MongoDB...")

    try:
        # Obtener los documentos procesados listos para MongoDB
        mongo_docs = processor.prepare_mongodb_documents()

        # Subir todas las colecciones procesadas
        for name, records in mongo_docs.items():
            mongo_handler.insert_many(name, records)
            print(f"✅ Colección '{name}' cargada ({len(records)} documentos)")

        print("✅ Datos cargados exitosamente en MongoDB\n")
    except Exception as e:
        print(f"❌ Error al cargar datos en MongoDB: {e}")
        return

    # === FASE 3: ANÁLISIS Y RESULTADOS ===
    print("FASE 3: ANÁLISIS Y RESULTADOS")

    results = processor.processed_results or {}

    print("📦 Resultados procesados:")
    warehouses = results.get("warehouses")
    econ = results.get("economic_time_series", {}).get("econ_correlations_with_orders")
    metrics = results.get("metrics")

    if warehouses:
        print(f" - Ubicaciones óptimas de warehouse: {len(warehouses)} regiones detectadas")
    if econ:
        print(f" - Correlaciones económicas: {econ}")
    if metrics:
        print(f" - Métricas generales: {metrics}")

    print(f" - Fecha de procesamiento: {results.get('timestamp', None)}")
    print("\n✅ Sistema ETL finalizado correctamente")


if __name__ == "__main__":
    main()
