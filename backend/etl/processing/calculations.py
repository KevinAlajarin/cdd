import pandas as pd
import numpy as np
from datetime import datetime

class MetricCalculator:
    def __init__(self, datasets):
        self.datasets = datasets
    
    # la ubicacion del warehouse se define en base a la densidad de pedidos en la zona
    # si la densidad es muy alta , subdividir en distintos warehouses dentro de la misma zona geografica (latitud y longitud)
    def calculate_warehouse_location(self):
        """Calcular ubicación óptima del galpón (centro geográfico)"""
        print("🗺️ Calculando ubicación del galpón...")
        
        # Unir customers con geolocation para obtener coordenadas
        customers_with_geo = self.datasets['customers'].merge(
            self.datasets['geolocation'],
            left_on='customer_zip_code_prefix',
            right_on='geolocation_zip_code_prefix',
            how='inner'
        )
        
        # Calcular centro geográfico simple
        avg_lat = customers_with_geo['geolocation_lat'].mean()
        avg_lng = customers_with_geo['geolocation_lng'].mean()
        
        print(f"📍 Ubicación calculada: Lat {avg_lat:.6f}, Lng {avg_lng:.6f}")
        
        
        return {
            'lat': float(avg_lat),
            'lng': float(avg_lng)
        }
    
    def calculate_inventory_distribution(self):
        """Calcular distribución de inventario por categoría"""
        print("📊 Calculando distribución de inventario...")
        
        # Unir order_items con products para obtener categorías
        items_with_categories = self.datasets['order_items'].merge(
            self.datasets['products'][['product_id', 'product_category_name']],
            on='product_id',
            how='inner'
        )
        
        # Contar frecuencia de pedidos por categoría
        category_counts = items_with_categories['product_category_name'].value_counts()
        
        # Calcular porcentajes
        total_orders = category_counts.sum()
        distribution = []
        
        for category, count in category_counts.items():
            percentage = (count / total_orders) * 100
            distribution.append({
                'categoria': category,
                'porcentaje': float(percentage)
            })
        
        # Ordenar por porcentaje descendente
        distribution.sort(key=lambda x: x['porcentaje'], reverse=True)
        
        print(f"📦 Distribución calculada para {len(distribution)} categorías")
        
        return distribution
    

    # borrar¡
    def calculate_delivery_metrics(self):
        """Calcular métricas de tiempo de entrega"""
        print("⏱️ Calculando métricas de entrega...")
        
        orders = self.datasets['orders'].copy()
        
        # Calcular tiempo real de entrega en días
        orders['tiempo_entrega_real'] = (
            orders['order_delivered_customer_date'] - 
            orders['order_purchase_timestamp']
        ).dt.total_seconds() / (24 * 3600)  # Convertir a días
        
        # Calcular diferencia entre estimado y real
        orders['diferencia_estimado_vs_real'] = (
            orders['order_estimated_delivery_date'] - 
            orders['order_delivered_customer_date']
        ).dt.total_seconds() / (24 * 3600)
        
        metrics = {
            'tiempo_promedio_entrega': float(orders['tiempo_entrega_real'].mean()),
            'diferencia_promedio_estimado': float(orders['diferencia_estimado_vs_real'].mean()),
            'total_pedidos_analizados': int(len(orders))
        }
        
        print("✅ Métricas de entrega calculadas")
        return metrics