import pandas as pd
from etl.config import DATASET_FILES

class DataCleaner:
    def __init__(self):
        self.datasets = {}
    
    def load_all_datasets(self):
        print("Cargando datasets...")
        try:
            for name, path in DATASET_FILES.items():
                self.datasets[name] = pd.read_csv(path)
            print("Todos los datasets cargados exitosamente")
            return True
        except Exception as e:
            print(f"Error cargando datasets: {e}")
            return False
    
    def filter_delivered_orders(self):
        print("Filtrando órdenes entregadas...")
        orders_original = len(self.datasets['orders'])
        self.datasets['orders'] = self.datasets['orders'][
            self.datasets['orders']['order_status'] == 'delivered'
        ]
        delivered_order_ids = self.datasets['orders']['order_id'].unique()
        self.datasets['order_items'] = self.datasets['order_items'][
            self.datasets['order_items']['order_id'].isin(delivered_order_ids)
        ]
        print(f"Órdenes filtradas: {len(self.datasets['orders'])}/{orders_original}")
    
    def clean_datasets(self):
        print("Aplicando limpieza de datos...")
        date_columns = [
            'order_purchase_timestamp', 'order_approved_at',
            'order_delivered_carrier_date', 'order_delivered_customer_date',
            'order_estimated_delivery_date'
        ]
        for col in date_columns:
            if col in self.datasets['orders'].columns:
                self.datasets['orders'][col] = pd.to_datetime(
                    self.datasets['orders'][col], errors='coerce'
                )
        
        self.datasets['orders'].dropna(subset=['order_purchase_timestamp', 'order_delivered_customer_date'], inplace=True)
        self.datasets['order_items'] = self.datasets['order_items'][self.datasets['order_items']['price'] > 0]
        
        for col in ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']:
            if col in self.datasets['products']:
                self.datasets['products'][col] = self.datasets['products'][col].fillna(0).clip(lower=0)
        
        print("Limpieza completada")
    
    def get_all_datasets(self):
        return self.datasets
