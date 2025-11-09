import pandas as pd
from .data_cleaner import DataCleaner
from .calculations import MetricCalculator

class DataProcessor:
    def __init__(self):
        self.cleaner = DataCleaner()
        self.calculator = None
        self.processed_results = {}
    
    def execute_etl(self):
        print("Iniciando proceso ETL completo...")
        if not self.cleaner.load_all_datasets():
            return False
        self.cleaner.filter_delivered_orders()
        self.cleaner.clean_datasets()
        self.calculator = MetricCalculator(self.cleaner.get_all_datasets())
        self.processed_results['galpon_ubicacion'], self.processed_results['factor_economico_promedio'] = \
            self.calculator.calculate_warehouse_location_economic_merge()
        self.processed_results['distribucion_inventario'] = self.calculator.calculate_inventory_distribution()
        self.processed_results['metricas_generales'] = self.calculator.calculate_delivery_metrics()
        self.processed_results['fecha_procesamiento'] = pd.Timestamp.now().isoformat()
        print("Proceso ETL completado exitosamente")
        return True
    
    def get_processed_data(self):
        return {
            'datasets_originales': self.cleaner.get_all_datasets(),
            'processed_results': self.processed_results
        }
    
    def prepare_mongodb_documents(self):
        datasets = self.cleaner.get_all_datasets()
        return {name: df.to_dict('records') for name, df in datasets.items()} | {'processed_results': [self.processed_results]}
