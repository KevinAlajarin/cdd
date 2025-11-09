import pandas as pd
from etl.config import DATASET_FILES

class DataCleaner:
    def __init__(self):
        self.datasets = {}
    
    def load_all_datasets(self):
        """Cargar todos los datasets desde CSV"""
        print("📂 Cargando datasets...")
        
        try:
            self.datasets['customers'] = pd.read_csv(DATASET_FILES['customers'])
            self.datasets['geolocation'] = pd.read_csv(DATASET_FILES['geolocation'])
            self.datasets['order_items'] = pd.read_csv(DATASET_FILES['order_items'])
            self.datasets['orders'] = pd.read_csv(DATASET_FILES['orders'])
            self.datasets['products'] = pd.read_csv(DATASET_FILES['products'])
            self.datasets['sellers'] = pd.read_csv(DATASET_FILES['sellers'])
            # agregar dataset de indicador economico
            
            print("✅ Todos los datasets cargados exitosamente")
            return True
            
        except Exception as e:
            print(f"❌ Error cargando datasets: {e}")
            return False
    
    def filter_delivered_orders(self):
        """Filtrar solo órdenes con status 'delivered'"""
        print("🎯 Filtrando órdenes entregadas...")
        
        orders_original = len(self.datasets['orders'])
        self.datasets['orders'] = self.datasets['orders'][
            self.datasets['orders']['order_status'] == 'delivered'
        ]
        orders_filtered = len(self.datasets['orders'])
        
        print(f"📊 Órdenes filtradas: {orders_filtered}/{orders_original} (entregadas/total)")
        
        # Filtrar order_items para mantener solo los de órdenes entregadas
        delivered_order_ids = self.datasets['orders']['order_id'].unique()
        self.datasets['order_items'] = self.datasets['order_items'][
            self.datasets['order_items']['order_id'].isin(delivered_order_ids)
        ]
        
        print("✅ Filtrado completado")
    
    def clean_datasets(self):
        """Aplicar limpieza básica a todos los datasets"""
        print("🧹 Aplicando limpieza de datos...")
        
        # Orders: Convertir fechas y limpiar nulos
        date_columns = ['order_purchase_timestamp', 'order_approved_at', 
                       'order_delivered_carrier_date', 'order_delivered_customer_date',
                       'order_estimated_delivery_date']
        
        for col in date_columns:
            if col in self.datasets['orders'].columns:
                self.datasets['orders'][col] = pd.to_datetime(
                    self.datasets['orders'][col], errors='coerce'
                )
        
        # Eliminar órdenes con fechas críticas nulas
        critical_dates = ['order_purchase_timestamp', 'order_delivered_customer_date']
        self.datasets['orders'] = self.datasets['orders'].dropna(subset=critical_dates)
        
        # Order Items: Validar precios positivos
        self.datasets['order_items'] = self.datasets['order_items'][
            self.datasets['order_items']['price'] > 0
        ]
        
        # Products: Limpiar dimensiones negativas o nulas
        dimension_cols = ['product_weight_g', 'product_length_cm', 
                         'product_height_cm', 'product_width_cm']
        for col in dimension_cols:
            if col in self.datasets['products'].columns:
                self.datasets['products'][col] = self.datasets['products'][col].fillna(0)
                self.datasets['products'][col] = self.datasets['products'][col].clip(lower=0)
        
        print("✅ Limpieza completada")
    
    def get_dataset(self, name):
        """Obtener dataset específico"""
        return self.datasets.get(name)
    
    def get_all_datasets(self):
        """Obtener todos los datasets"""
        return self.datasets


