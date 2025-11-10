import pandas as pd
import numpy as np

class WarehouseAllocator:
    def __init__(self, df_orders, df_customers, df_geolocation, df_items, df_products):
        self.df_orders = df_orders
        self.df_customers = df_customers
        self.df_geolocation = df_geolocation
        self.df_items = df_items
        self.df_products = df_products

    def estimate(self):
        merged = self.df_orders.merge(self.df_customers, on='customer_id', how='left')
        merged = merged.merge(self.df_geolocation, on='customer_zip_code_prefix', how='left')
        merged = merged[merged['order_status'] == 'delivered']

        summary = merged.groupby('customer_state').agg({
            'order_id': 'count',
            'geolocation_lat': 'mean',
            'geolocation_lng': 'mean'
        }).reset_index()

        summary.rename(columns={
            'customer_state': 'state',
            'order_id': 'orders_count',
            'geolocation_lat': 'lat',
            'geolocation_lng': 'lng'
        }, inplace=True)

        summary['size'] = pd.qcut(summary['orders_count'], q=3, labels=['small', 'medium', 'large'])
        summary['estimated_capacity_m3'] = summary['orders_count'] * 0.05

        top_items = self.df_items.groupby('product_id').size().reset_index(name='count')
        top_items = top_items.sort_values('count', ascending=False).head(5)
        top_products = self.df_products[self.df_products['product_id'].isin(top_items['product_id'])]

        summary['product_distribution'] = [top_products.to_dict(orient='records')] * len(summary)

        return summary.to_dict(orient='records')
