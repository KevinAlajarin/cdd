# etl/processing/warehouse_allocator.py
import pandas as pd
from sklearn.cluster import KMeans

class WarehouseAllocator:
    """
    Asigna ubicaciones óptimas de warehouse usando clustering geográfico (K-Means)
    basado en la densidad de pedidos y coordenadas de clientes.
    """

    def __init__(self, df_orders, df_customers, df_geolocation, df_items, df_products, n_clusters=27):
        self.df_orders = df_orders
        self.df_customers = df_customers
        self.df_geolocation = df_geolocation
        self.df_items = df_items
        self.df_products = df_products
        self.n_clusters = n_clusters

    def estimate(self):
        print("Estimando ubicaciones óptimas de warehouse mediante clustering geográfico...")

        # === 1️ Validar y preparar columnas ===
        df_cust = self.df_customers.copy()
        df_geo = self.df_geolocation.copy()

        # Verificar si están las columnas esperadas
        if "customer_zip_code_prefix" not in df_cust.columns:
            # Buscar alternativas
            zip_col = [c for c in df_cust.columns if "zip" in c][0]
            df_cust = df_cust.rename(columns={zip_col: "customer_zip_code_prefix"})

        if "geolocation_zip_code_prefix" not in df_geo.columns:
            zip_col = [c for c in df_geo.columns if "zip" in c][0]
            df_geo = df_geo.rename(columns={zip_col: "geolocation_zip_code_prefix"})

        # === 2️ Vincular clientes con coordenadas ===
        df_merge = pd.merge(
            df_cust,
            df_geo,
            left_on="customer_zip_code_prefix",
            right_on="geolocation_zip_code_prefix",
            how="left"
        )

        # Filtrar coordenadas válidas
        df_merge = df_merge.dropna(subset=["geolocation_lat", "geolocation_lng"])

        if df_merge.empty:
            raise ValueError("No hay coordenadas válidas para clientes tras el merge geográfico.")

        # === 3️ Aplicar clustering K-Means sobre coordenadas ===
        coords = df_merge[["geolocation_lat", "geolocation_lng"]].values
        kmeans = KMeans(n_clusters=self.n_clusters, random_state=42, n_init=10)
        df_merge["cluster"] = kmeans.fit_predict(coords)

        # === 4️ Calcular centroides y estadísticas ===
        warehouses = []
        for i in range(self.n_clusters):
            cluster_points = df_merge[df_merge["cluster"] == i]
            lat_mean = cluster_points["geolocation_lat"].mean()
            lon_mean = cluster_points["geolocation_lng"].mean()
            density = len(cluster_points)

            warehouses.append({
                "warehouse_id": i + 1,
                "latitude": float(lat_mean),
                "longitude": float(lon_mean),
                "customer_count": int(density),
            })

        print(f"{len(warehouses)} ubicaciones de warehouse estimadas.")
        return warehouses
