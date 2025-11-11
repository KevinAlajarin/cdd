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

        # === 1️ Preparar data ===
        df_cust = self.df_customers.copy()
        df_geo = self.df_geolocation.copy()

        # Asegurar nombres de columnas
        if "customer_zip_code_prefix" not in df_cust.columns:
            zip_col = [c for c in df_cust.columns if "zip" in c][0]
            df_cust = df_cust.rename(columns={zip_col: "customer_zip_code_prefix"})

        if "geolocation_zip_code_prefix" not in df_geo.columns:
            zip_col = [c for c in df_geo.columns if "zip" in c][0]
            df_geo = df_geo.rename(columns={zip_col: "geolocation_zip_code_prefix"})

        # === 2️ Merge clientes + coordenadas ===
        df_merge = pd.merge(
            df_cust,
            df_geo,
            left_on="customer_zip_code_prefix",
            right_on="geolocation_zip_code_prefix",
            how="left"
        ).dropna(subset=["geolocation_lat", "geolocation_lng"])

        if df_merge.empty:
            raise ValueError("No hay coordenadas válidas para clientes tras el merge geográfico.")

        # === 3️ Clustering geográfico ===
        coords = df_merge[["geolocation_lat", "geolocation_lng"]].values
        kmeans = KMeans(n_clusters=self.n_clusters, random_state=42, n_init=10)
        df_merge["cluster"] = kmeans.fit_predict(coords)

        # === 4️ Vincular pedidos y productos a cada cluster ===
        df_orders = self.df_orders.copy()
        df_items = self.df_items.copy()
        df_products = self.df_products.copy()

        df_full = (
            df_orders.merge(df_items, on="order_id", how="inner")
            .merge(df_products, on="product_id", how="left")
            .merge(df_merge[["customer_id", "cluster"]], on="customer_id", how="left")
        )

        # === 5️ Calcular resumen por warehouse ===
        warehouses = []
        for cluster_id in sorted(df_full["cluster"].dropna().unique()):
            cluster_points = df_merge[df_merge["cluster"] == cluster_id]
            lat_mean = cluster_points["geolocation_lat"].mean()
            lon_mean = cluster_points["geolocation_lng"].mean()

            # top 5 productos más vendidos por cluster
            cluster_items = df_full[df_full["cluster"] == cluster_id]
            top_items = (
                cluster_items["product_id"]
                .value_counts()
                .head(5)
                .index.tolist()
            )

            warehouses.append({
                "warehouse_id": int(cluster_id + 1),
                "latitude": float(lat_mean),
                "longitude": float(lon_mean),
                "customer_count": int(len(cluster_points)),
                "top_items": top_items,
            })

        print(f"{len(warehouses)} ubicaciones de warehouse estimadas con productos top.")
        return warehouses
