import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from etl.processing.data_processor import DataProcessor
from etl.database.mongodb_handler import MongoDBHandler
from dotenv import load_dotenv

def main():
    print("INICIANDO SISTEMA ETL - ECOMMERCE BRAZIL")
    print("=" * 50)
    
    load_dotenv()
    
    print("\nFASE 1: PROCESAMIENTO ETL")
    processor = DataProcessor()
    if not processor.execute_etl():
        print("Error en el procesamiento ETL. Saliendo...")
        return
    
    print("\nFASE 2: CONEXIÓN MONGODB")
    db_handler = MongoDBHandler()
    if not db_handler.connect():
        print("Error conectando a MongoDB. Saliendo...")
        return
    
    try:
        print("\nFASE 3: PREPARACIÓN DE DATOS")
        documents = processor.prepare_mongodb_documents()
        
        print("\nFASE 4: INSERCIÓN EN MONGODB")
        collections_to_insert = [
            'customers', 'geolocation', 'order_items', 
            'orders', 'products', 'sellers', 'economic_data'
        ]
        
        for collection in collections_to_insert:
            print(f"Insertando {collection}...")
            db_handler.insert_data(collection, documents[collection])
        
        print("Insertando processed_results...")
        db_handler.insert_data('processed_results', documents['processed_results'])
        
        print("\nFASE 5: CREACIÓN DE ÍNDICES")
        db_handler.create_indexes()
        
        print("\nPROCESO COMPLETADO EXITOSAMENTE")
        print("=" * 50)
        
        results = processor.get_processed_data()['processed_results']
        print(f"Ubicación galpón: {results['galpon_ubicacion']}")
        print(f"Factor económico promedio: {results['factor_economico_promedio']:.2f}")
        print(f"Top 3 categorías:")
        for i, cat in enumerate(results['distribucion_inventario'][:3]):
            print(f"   {i+1}. {cat['categoria']}: {cat['porcentaje']:.2f}%")
        print(f"Tiempo promedio entrega: {results['metricas_generales']['tiempo_promedio_entrega']:.2f} días")
        print(f"Fecha procesamiento: {results['fecha_procesamiento']}")
        
    except Exception as e:
        print(f"Error durante la inserción: {e}")
    finally:
        db_handler.disconnect()

if __name__ == "__main__":
    main()
