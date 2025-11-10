# etl/processing/metric_calculator.py
import pandas as pd
import numpy as np
from datetime import datetime

class MetricCalculator:
    """
    Clase encargada de los cálculos analíticos del proyecto:
    - Estimar ubicación óptima de warehouses
    - Calcular correlaciones económicas
    - Generar métricas generales y timestamp
    """

    def __init__(self, df_items, df_customers, df_geolocation, df_economic):
        self.df_items = df_items
        self.df_customers = df_customers
        self.df_geolocation = df_geolocation
        self.df_economic = df_economic

    # ================================================================
    # MÉTODO PRINCIPAL
    # ================================================================
    def calculate_all(self):
        results = {}

        # --- 1️⃣ Estimar ubicaciones de warehouses ---
        results["warehouses"] = self._estimate_warehouses()

        # --- 2️⃣ Calcular correlaciones económicas ---
        results["economic_correlations"] = self._calculate_economic_relations()

        # --- 3️⃣ Métricas generales y timestamp ---
        results["metrics"] = self._generate_metrics()
        results["timestamp"] = datetime.utcnow().isoformat()

        return results

    # ================================================================
    # MÉTODOS SECUNDARIOS
    # ================================================================

    def _estimate_warehouses(self):
        """
        Calcula ubicaciones óptimas de warehouses en base a densidad de pedidos.
        Si hay mucha concentración en una zona, se asume ciudad grande → warehouse grande.
        """
        df = self.df_customers.merge(
            self.df_geolocation,
            left_on="customer_zip_code_prefix",
            right_on="geolocation_zip_code_prefix",
            how="inner"
        )

        # Agrupar por estado (densidad aproximada)
        density = df.groupby("geolocation_state")[["geolocation_lat", "geolocation_lng"]].mean().reset_index()

        warehouses = []
        for _, row in density.iterrows():
            warehouses.append({
                "state": row["geolocation_state"],
                "lat": row["geolocation_lat"],
                "lng": row["geolocation_lng"]
            })


        return warehouses

    def _calculate_economic_relations(self):
        """
        Calcula correlaciones entre actividad económica y variables clave.
        """
        df = self.df_economic.copy()

        cols = ["econ_act", "peo_debt", "inflation", "interest_rate"]
        correlations = {}

        for col in cols:
            if df[col].dtype in [np.float64, np.int64]:
                corr = df["econ_act"].corr(df[col])
                correlations[col] = round(corr, 3)
            else:
                correlations[col] = None

        print("Calculando relación económica-temporal...")
        print(f"Correlaciones calculadas: {correlations}")

        return correlations

    def _generate_metrics(self):
        """
        Calcula métricas descriptivas básicas sobre pedidos.
        """
        total_customers = len(self.df_customers)
        total_items = len(self.df_items)

        metrics = {
            "total_customers": total_customers,
            "total_items": total_items,
            "items_per_customer_avg": round(total_items / total_customers, 2) if total_customers else None,
        }

        return metrics
