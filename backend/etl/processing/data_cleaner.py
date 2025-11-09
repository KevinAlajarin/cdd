import pandas as pd
import os

class DataCleaner:
    def __init__(self):
        self.datasets = {}

    def load_all_datasets(self):
        print("Cargando datasets...")

        paths = {
    "orders": "data/olist_orders_dataset.csv",
    "customers": "data/olist_customers_dataset.csv",
    "order_items": "data/olist_order_items_dataset.csv",
    "products": "data/olist_products_dataset.csv",
    "sellers": "data/olist_sellers_dataset.csv",
    "geolocation": "data/olist_geolocation_dataset.csv",
    "economic_indicators": "data/brazil_economy_indicators.csv"
        }

        for name, path in paths.items():
            if os.path.exists(path):
                try:
                    self.datasets[name] = pd.read_csv(path)
                except Exception as e:
                    print(f"❌ Error al cargar {name}: {e}")
                    self.datasets[name] = None
            else:
                print(f"⚠️  Archivo no encontrado: {path}")
                self.datasets[name] = None

        print("✅ Todos los datasets cargados exitosamente")
        return True

    def filter_delivered_orders(self):
        if "orders" in self.datasets and isinstance(self.datasets["orders"], pd.DataFrame):
            df = self.datasets["orders"]
            delivered = df[df["order_status"] == "delivered"]
            print(f"Órdenes filtradas: {len(delivered)}/{len(df)}")
            self.datasets["orders"] = delivered

    def clean_datasets(self):
        print("Aplicando limpieza de datos...")

        for name, df in self.datasets.items():
            if isinstance(df, pd.DataFrame):
                df.drop_duplicates(inplace=True)
                df.dropna(inplace=True)
                self.datasets[name] = df

        print("✅ Limpieza completada")

    def get_all_datasets(self):
        return self.datasets
