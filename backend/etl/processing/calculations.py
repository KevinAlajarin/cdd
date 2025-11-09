import pandas as pd
import numpy as np

class MetricCalculator:
    def __init__(self, datasets):
        self.datasets = datasets
    
    def calculate_warehouse_location_economic_merge(self):
        print("Calculando ubicación del galpón y relación con datos económicos...")
        customers_with_geo = self.datasets['customers'].merge(
            self.datasets['geolocation'],
            left_on='customer_zip_code_prefix',
            right_on='geolocation_zip_code_prefix',
            how='inner'
        )
        economic_data = self.datasets['economic_data']
        customers_with_geo = customers_with_geo.merge(
            economic_data, left_on='customer_state', right_on='state', how='left'
        )
        avg_lat = customers_with_geo['geolocation_lat'].mean()
        avg_lng = customers_with_geo['geolocation_lng'].mean()
        avg_econ = customers_with_geo['economic_index'].mean() if 'economic_index' in customers_with_geo else np.nan
        return {'lat': float(avg_lat), 'lng': float(avg_lng)}, float(avg_econ)
    
    def calculate_inventory_distribution(self):
        items_with_categories = self.datasets['order_items'].merge(
            self.datasets['products'][['product_id', 'product_category_name']],
            on='product_id', how='inner'
        )
        category_counts = items_with_categories['product_category_name'].value_counts()
        total_orders = category_counts.sum()
        return [
            {'categoria': c, 'porcentaje': float((n / total_orders) * 100)}
            for c, n in category_counts.items()
        ]
    
    def calculate_delivery_metrics(self):
        orders = self.datasets['orders'].copy()
        orders['tiempo_entrega_real'] = (
            orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']
        ).dt.total_seconds() / (24 * 3600)
        metrics = {
            'tiempo_promedio_entrega': float(orders['tiempo_entrega_real'].mean()),
            'total_pedidos_analizados': int(len(orders))
        }
        return metrics
