# etl/processing/data_processor.py
import pandas as pd
from datetime import datetime
from .data_cleaner import DataCleaner
from .metric_calculator import MetricCalculator
from .warehouse_allocator import WarehouseAllocator

class DataProcessor:
    """
    Orquesta el proceso ETL completo:
    - Carga, limpieza y filtrado de datos
    - Cálculo de ubicaciones de warehouses y métricas económicas
    - Preparación para MongoDB
    """

    def __init__(self, cleaner=None):
        self.cleaner = cleaner if cleaner else DataCleaner()
        self.calculator = None
        self.processed_results = {}

    def execute_etl(self):
        print("Iniciando proceso ETL completo...")

        if not self.cleaner.load_all_datasets():
            print("Error cargando datasets.")
            return False

        self.cleaner.filter_delivered_orders()
        self.cleaner.clean_datasets()

        try:
            self.calculator = MetricCalculator(
                df_items=self.cleaner.datasets["order_items"],
                df_customers=self.cleaner.datasets["customers"],
                df_geolocation=self.cleaner.datasets["geolocation"],
                df_economic=self.cleaner.datasets["economic_indicators"],
            )
        except Exception as e:
            print(f"Error al instanciar MetricCalculator: {e}")
            return False

        print("Calculando ubicación de galpones y relación económica...")

        try:
            allocator = WarehouseAllocator(
                df_orders=self.cleaner.datasets["orders"],
                df_customers=self.cleaner.datasets["customers"],
                df_geolocation=self.cleaner.datasets["geolocation"],
                df_items=self.cleaner.datasets["order_items"],
                df_products=self.cleaner.datasets["products"],
                n_clusters=27
            )

            warehouses = allocator.estimate()
            results = self.calculator.calculate_all()

            # Integrar warehouses con correlaciones económicas
            results["warehouses"] = warehouses
            results["timestamp"] = datetime.utcnow().isoformat()

            self.processed_results = results
            print("Proceso ETL completado correctamente.")
            return True

        except Exception as e:
            print(f"Error en el cálculo de métricas o ubicación: {e}")
            return False

    def get_processed_data(self):
        return {
            "datasets_originales": self.cleaner.get_all_datasets(),
            "processed_results": self.processed_results,
        }

    def prepare_mongodb_documents(self):
        datasets = self.cleaner.get_all_datasets()
        mongo_docs = {name: df.to_dict("records") for name, df in datasets.items()}
        mongo_docs["processed_results"] = [self.processed_results]
        return mongo_docs
