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

    def execute_etl(self, n_clusters=None):
        print("Iniciando proceso ETL completo...")

        if not self.cleaner.load_all_datasets():
            print("Error cargando datasets.")
            return False

        # Filtrar y limpiar
        self.cleaner.filter_delivered_orders()
        self.cleaner.clean_datasets()

        # Instanciar metric calculator with required datasets
        try:
            self.calculator = MetricCalculator(
                df_orders=self.cleaner.datasets.get("orders"),
                df_items=self.cleaner.datasets.get("order_items"),
                df_customers=self.cleaner.datasets.get("customers"),
                df_geolocation=self.cleaner.datasets.get("geolocation"),
                df_economic=self.cleaner.datasets.get("economic_indicators"),
                df_products=self.cleaner.datasets.get("products")
            )
        except Exception as e:
            print(f"Error al instanciar MetricCalculator: {e}")
            return False

        print("Calculando métricas y ubicaciones de warehouses...")

        try:
            # 1) calculate metrics & delivery stats & economic analysis
            results_base = self.calculator.calculate_all()

            # 2) estimate warehouses with clustering and improvements
            allocator = WarehouseAllocator(
                df_orders=self.cleaner.datasets.get("orders"),
                df_customers=self.cleaner.datasets.get("customers"),
                df_geolocation=self.cleaner.datasets.get("geolocation"),
                df_items=self.cleaner.datasets.get("order_items"),
                df_products=self.cleaner.datasets.get("products"),
                n_clusters=n_clusters
            )
            warehouses = allocator.estimate()

            # 3) Build processed_results enriched structure
            processed = {
                "timestamp": datetime.utcnow().isoformat(),
                "metrics": results_base.get("metrics", {}),
                "economic_analysis": results_base.get("economic_analysis", {}),
                "delivery_stats": results_base.get("delivery_stats", {}),
                "warehouses": warehouses,
                "notes": {
                    "clustering_method": "KMeans",
                    "n_clusters": allocator.n_clusters
                }
            }

            # add summary metrics derived
            total_wh = len(warehouses)
            processed["metrics"]["total_warehouses"] = total_wh
            if total_wh > 0:
                processed["metrics"]["avg_customers_per_warehouse"] = int(sum([w.get("customer_count",0) for w in warehouses]) / total_wh)

            self.processed_results = processed
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
        # push processed_results as a single document
        mongo_docs["processed_results"] = [self.processed_results]
        # also create a separate warehouses collection for convenience
        mongo_docs["warehouses"] = self.processed_results.get("warehouses", [])
        return mongo_docs
