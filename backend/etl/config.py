import os
from pathlib import Path

# Configuración de paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATASETS_DIR = BASE_DIR / "datasets"

# Configuración de archivos CSV
DATASET_FILES = {
    'customers': DATASETS_DIR / "olist_customers_dataset.csv",
    'geolocation': DATASETS_DIR / "olist_geolocation_dataset.csv", 
    'order_items': DATASETS_DIR / "olist_order_items_dataset.csv",
    'orders': DATASETS_DIR / "olist_orders_dataset.csv",
    'products': DATASETS_DIR / "olist_products_dataset.csv",
    'sellers': DATASETS_DIR / "olist_sellers_dataset.csv"
}

# Configuración MongoDB (se completará después)
MONGODB_CONFIG = {
    'connection_string': '',  # Lo pondremos en .env
    'database_name': 'ecommerce_brazil',
    'collections': {
        'orders': 'orders',
        'order_items': 'order_items', 
        'customers': 'customers',
        'sellers': 'sellers',
        'products': 'products',
        'geolocation': 'geolocation',
        'processed_results': 'processed_results'
    }
}