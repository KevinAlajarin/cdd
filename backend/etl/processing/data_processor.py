import pandas as pd
from .data_cleaner import DataCleaner
from .calculations import MetricCalculator

class DataProcessor:
    def __init__(self):
        self.cleaner = DataCleaner()
        self.calculator = None
        self.processed_results = {}
    
    def execute_etl(self):
        """Ejecutar todo el proceso ETL"""
        print("🔄 Iniciando proceso ETL completo...")
        
        # 1. Cargar datos
        if not self.cleaner.load_all_datasets():
            return False
        
        # 2. Filtrar órdenes entregadas
        self.cleaner.filter_delivered_orders()
        
        # 3. Limpiar datos
        self.cleaner.clean_datasets()
        
        # 4. Inicializar calculadora
        self.calculator = MetricCalculator(self.cleaner.get_all_datasets())
        
        # 5. Calcular métricas , aca vamos agregando las que necesitemos
        self.processed_results['galpon_ubicacion'] = self.calculator.calculate_warehouse_location() # para esto vamos a necesitar definidos los estados
        self.processed_results['distribucion_inventario'] = self.calculator.calculate_inventory_distribution() # esto divide en porcentajes que categoria de productos se vendio mas pero creo que no los clasifica por ubicacion
        self.processed_results['metricas_generales'] = self.calculator.calculate_delivery_metrics()  # posiblemente innecesario
        self.processed_results['fecha_procesamiento'] = pd.Timestamp.now().isoformat() # ni puta idea que hace
        # traducir latitud y longitud a estado?
        # que todos los datasets tengan el mismo formato de tiempo

        print("✅ Proceso ETL completado exitosamente")
        return True
    
    def get_processed_data(self):
        """Obtener datos para MongoDB"""
        return {
            'datasets_originales': self.cleaner.get_all_datasets(),
            'processed_results': self.processed_results
        }
    
    def prepare_mongodb_documents(self):
        """Preparar documentos para inserción en MongoDB"""
        datasets = self.cleaner.get_all_datasets()
        
        documents = {}
        
        # Preparar documentos para cada colección
        for name, df in datasets.items():
            # Convertir DataFrame a lista de diccionarios
            documents[name] = df.to_dict('records')
        
        # Agregar processed_results
        documents['processed_results'] = [self.processed_results]
        
        return documents