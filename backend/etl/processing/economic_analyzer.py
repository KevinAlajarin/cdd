import pandas as pd
import numpy as np

class EconomicAnalyzer:
    def __init__(self, df_orders, df_economic):
        self.df_orders = df_orders
        self.df_economic = df_economic

    def analyze(self):
        df = self.df_orders.copy()
        df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
        df['year_month'] = df['order_purchase_timestamp'].dt.to_period('M').astype(str)

        monthly_orders = df.groupby('year_month').size().reset_index(name='orders_count')
        joined = monthly_orders.merge(self.df_economic, on='year_month', how='left')

        corr = joined.corr(numeric_only=True)['orders_count'].drop('orders_count', errors='ignore').to_dict()

        joined['month_index'] = np.arange(len(joined))
        slope, intercept = np.polyfit(joined['month_index'], joined['orders_count'], 1)
        trend = "increasing" if slope > 0 else "decreasing"

        return {
            "monthly_volumes": monthly_orders.to_dict(orient='records'),
            "joined_monthly": joined.to_dict(orient='records'),
            "trend_estimates": {"slope": slope, "trend": trend},
            "econ_correlations_with_orders": corr
        }
