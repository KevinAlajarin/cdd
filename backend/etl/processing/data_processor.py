import pandas as pd
from datetime import datetime
from .data_cleaner import DataCleaner
from .metric_calculator import MetricCalculator
from .warehouse_allocator import WarehouseAllocator

class DataProcessor:
    """
    Orquesta el proceso ETL completo con mejoras de clustering, métricas y proyección de crecimiento de clientes.
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

        # Instanciar metric calculator
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
            # 1) Calcular métricas generales y económicas
            results_base = self.calculator.calculate_all()

            # 2) Estimar warehouses
            allocator = WarehouseAllocator(
                df_orders=self.cleaner.datasets.get("orders"),
                df_customers=self.cleaner.datasets.get("customers"),
                df_geolocation=self.cleaner.datasets.get("geolocation"),
                df_items=self.cleaner.datasets.get("order_items"),
                df_products=self.cleaner.datasets.get("products"),
                n_clusters=n_clusters
            )
            warehouses = allocator.estimate()

            # 3) Proyección de crecimiento de clientes por warehouse (1 y 2 años)
            econ = results_base.get("economic_analysis", {}).get("national_correlations", {})
            econ_act = econ.get("econ_act", 0.0)
            peo_debt = econ.get("peo_debt", 0.0)
            inflation = econ.get("inflation", 0.0)
            interest_rate = econ.get("interest_rate", 0.0)

            # Normalización simple 0-1
            norm_econ_act = min(max(econ_act,0),1)
            norm_peo_debt = min(max(peo_debt,0),1)
            norm_inflation = min(max(inflation,0),1)
            norm_interest_rate = min(max(interest_rate,0),1)

            for w in warehouses:
                growth_factor = 0.5*norm_econ_act - 0.2*norm_peo_debt - 0.1*norm_inflation - 0.1*norm_interest_rate
                w["estimated_customer_growth_1y"] = int(w["customer_count"] * (1 + growth_factor))
                w["estimated_customer_growth_2y"] = int(w["customer_count"] * (1 + growth_factor)**2)

            # 4) Construir processed_results
            processed = {
                "timestamp": datetime.utcnow().isoformat(),
                "metrics": results_base.get("metrics", {}),
                "economic_analysis": results_base.get("economic_analysis", {}),
                "delivery_stats": results_base.get("delivery_stats", {}),
                "warehouses": warehouses,
                "cluster_logs": allocator.logs,
                "notes": {
                    "clustering_method": "KMeans",
                    "n_clusters": allocator.n_clusters
                }
            }

            total_wh = len(warehouses)
            processed["metrics"]["total_warehouses"] = total_wh
            if total_wh > 0:
                processed["metrics"]["avg_customers_per_warehouse"] = int(
                    processed["metrics"]["total_customers"] / total_wh
                )

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
        mongo_docs["processed_results"] = [self.processed_results]
        return mongo_docs
