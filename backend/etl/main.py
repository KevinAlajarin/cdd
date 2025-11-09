import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from etl.processing.data_processor import DataProcessor
from etl.database.mongodb_handler import MongoDBHandler
from dotenv import load_dotenv

def main():
    print("🚀 INICIANDO SISTEMA ETL - ECOMMERCE BRAZIL")
    print("=" * 50)
    
    # Cargar variables de entorno
    load_dotenv()
    
    # 1. Procesar datos
    print("\n📊 FASE 1: PROCESAMIENTO ETL")
    processor = DataProcessor()
    
    if not processor.execute_etl():
        print("❌ Error en el procesamiento ETL. Saliendo...")
        return
    
    # 2. Conectar a MongoDB
    print("\n🗄️ FASE 2: CONEXIÓN MONGODB")
    db_handler = MongoDBHandler()
    
    if not db_handler.connect():
        print("❌ Error conectando a MongoDB. Saliendo...")
        return
    
    try:
        # 3. Preparar datos para inserción
        print("\n📥 FASE 3: PREPARACIÓN DE DATOS")
        documents = processor.prepare_mongodb_documents()
        
        # 4. Insertar datos en MongoDB
        print("\n💾 FASE 4: INSERCIÓN EN MONGODB")
        
        # Insertar datasets originales (solo delivered)
        collections_to_insert = [
            'customers', 'geolocation', 'order_items', 
            'orders', 'products', 'sellers'
        ]
        
        for collection in collections_to_insert:
            print(f"📤 Insertando {collection}...")
            db_handler.insert_data(collection, documents[collection])
        
        # Insertar processed_results
        print("📤 Insertando processed_results...")
        db_handler.insert_data('processed_results', documents['processed_results'])
        
        # 5. Crear índices
        print("\n📊 FASE 5: CREACIÓN DE ÍNDICES")
        db_handler.create_indexes()
        
        # 6. Mostrar resumen
        print("\n✅ PROCESO COMPLETADO EXITOSAMENTE")
        print("=" * 50)
        
        results = processor.get_processed_data()['processed_results']
        print(f"📍 Ubicación galpón: {results['galpon_ubicacion']}")
        print(f"📦 Top 3 categorías:")
        for i, cat in enumerate(results['distribucion_inventario'][:3]):
            print(f"   {i+1}. {cat['categoria']}: {cat['porcentaje']:.2f}%")
        print(f"⏱️ Tiempo promedio entrega: {results['metricas_generales']['tiempo_promedio_entrega']:.2f} días")
        print(f"📅 Fecha procesamiento: {results['fecha_procesamiento']}")
        
    except Exception as e:
        print(f"❌ Error durante la inserción: {e}")
    finally:
        # Cerrar conexión
        db_handler.disconnect()

if __name__ == "__main__":
    main()